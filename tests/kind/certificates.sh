#!/usr/bin/env bash

install_certificate_controllers() {
  helm upgrade --install cert-manager cert-manager \
    --repo https://charts.jetstack.io --version v1.21.1 \
    --namespace cert-manager --create-namespace \
    --set crds.enabled=true --wait --timeout 180s
  helm upgrade --install reloader reloader \
    --repo https://stakater.github.io/stakater-charts --version 2.2.16 \
    --namespace reloader --create-namespace \
    --set reloader.reloadStrategy=annotations --wait --timeout 180s

  kubectl -n "$NAMESPACE" create secret tls internal-issuer-ca \
    --cert="$CERTIFICATES/ca.crt" --key="$CERTIFICATES/ca.key"
  kubectl -n "$NAMESPACE" create secret generic internal-public-trust \
    --from-file=ca.crt="$CERTIFICATES/ca.crt"
  kubectl -n "$NAMESPACE" apply -f - <<'YAML'
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: relaygate-internal
spec:
  ca:
    secretName: internal-issuer-ca
YAML
  kubectl -n "$NAMESPACE" wait --for=condition=Ready issuer/relaygate-internal --timeout=120s
  kubectl -n "$NAMESPACE" apply -f - <<'YAML'
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: relaygate-edge-tls
spec:
  secretName: relaygate-edge-tls
  issuerRef:
    name: relaygate-internal
    kind: Issuer
  dnsNames:
    - relaygate-gateway.internal
  usages:
    - server auth
  privateKey:
    algorithm: ECDSA
    size: 256
    rotationPolicy: Always
YAML
  kubectl -n "$NAMESPACE" wait --for=condition=Ready certificate/relaygate-edge-tls --timeout=120s
}

certificate_serial() {
  kubectl -n "$NAMESPACE" get secret "$1" -o jsonpath='{.data.tls\.crt}' |
    base64 -d | openssl x509 -noout -serial
}

verify_certificate_reissue_rollout() {
  local role certificate old_serial attempt new_serial pod address served_serial
  local -a pods uids unchanged_pods unchanged_uids addresses
  for role in edge gw rt; do
    if [[ "$role" == edge ]]; then
      certificate=relaygate-edge-tls
    else
      certificate="relaygate-${role}-internal-tls"
    fi
    kubectl -n "$NAMESPACE" wait --for=condition=Ready "certificate/$certificate" --timeout=120s
    old_serial=$(certificate_serial "$certificate")
    if [[ "$role" != rt ]]; then
      pods=(relaygate-gateway-0 relaygate-gateway-1 relaygate-gateway-2)
      unchanged_pods=(relaygate-rt-0 relaygate-rt-1)
    else
      pods=(relaygate-rt-0 relaygate-rt-1)
      unchanged_pods=(relaygate-gateway-0 relaygate-gateway-1 relaygate-gateway-2)
    fi
    uids=()
    for pod in "${pods[@]}"; do
      uids+=("$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}')")
    done
    unchanged_uids=()
    for pod in "${unchanged_pods[@]}"; do
      unchanged_uids+=("$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}')")
    done

    # Key-spec change uses cert-manager's reissuance path; no manual Pod restart.
    kubectl -n "$NAMESPACE" patch certificate "$certificate" --type=merge \
      -p '{"spec":{"privateKey":{"size":384}}}'
    new_serial=$old_serial
    for ((attempt = 0; attempt < 180; attempt++)); do
      new_serial=$(certificate_serial "$certificate")
      if [[ "$new_serial" != "$old_serial" ]]; then
        break
      fi
      sleep 1
    done
    if [[ "$new_serial" == "$old_serial" ]]; then
      echo "certificate did not reissue: $certificate" >&2
      return 1
    fi
    for ((attempt = 0; attempt < ${#pods[@]}; attempt++)); do
      wait_for_replaced_pod "${pods[$attempt]}" "${uids[$attempt]}" 180
    done
    for ((attempt = 0; attempt < ${#unchanged_pods[@]}; attempt++)); do
      pod=${unchanged_pods[$attempt]}
      if [[ "$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}')" != "${unchanged_uids[$attempt]}" ]]; then
        echo "unrelated Pod restarted after $certificate renewal: $pod" >&2
        return 1
      fi
    done
    if [[ "$role" == edge ]]; then
      IFS=, read -r -a addresses <<<"$GATEWAYS"
      addresses+=(127.0.0.1:28423)
      for address in "${addresses[@]}"; do
        served_serial=$(timeout 15 openssl s_client -connect "$address" \
          -servername relaygate-gateway.internal -alpn relaygate/2 \
          -CAfile "$CERTIFICATES/ca.crt" -verify_return_error </dev/null 2>/dev/null |
          openssl x509 -noout -serial)
        if [[ "$served_serial" != "$new_serial" ]]; then
          echo "Gateway still serves a different edge certificate at $address" >&2
          return 1
        fi
        printf '%s %s\n' "$address" "$served_serial" >>"$ARTIFACTS/certificate-edge-served.txt"
      done
    fi
    kubectl -n "$NAMESPACE" get certificate "$certificate" -o json \
      >"$ARTIFACTS/certificate-${role}.json"
    wait_for_destination "$DESTINATION_A"
    wait_for_destination "$DESTINATION_B"
    wait_for_destination "$DESTINATION_C"
    run_probe "certificate-${role}-recovery" matrix
  done
  record_pass KIND-14 'edge/internal reissue replaces only affected Pods; served edge serial and SDK recovery verified'
}
