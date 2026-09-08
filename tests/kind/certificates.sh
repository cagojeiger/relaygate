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
}

certificate_serial() {
  kubectl -n "$NAMESPACE" get secret "$1" -o jsonpath='{.data.tls\.crt}' |
    base64 -d | openssl x509 -noout -serial
}

verify_certificate_reissue_rollout() {
  local role certificate old_serial attempt new_serial pod
  local -a pods uids
  for role in gw rt; do
    certificate="relaygate-${role}-internal-tls"
    kubectl -n "$NAMESPACE" wait --for=condition=Ready "certificate/$certificate" --timeout=120s
    old_serial=$(certificate_serial "$certificate")
    if [[ "$role" == gw ]]; then
      pods=(relaygate-gateway-0 relaygate-gateway-1 relaygate-gateway-2)
    else
      pods=(relaygate-rt-0 relaygate-rt-1)
    fi
    uids=()
    for pod in "${pods[@]}"; do
      uids+=("$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.metadata.uid}')")
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
    kubectl -n "$NAMESPACE" get certificate "$certificate" -o json \
      >"$ARTIFACTS/certificate-${role}.json"
    wait_for_destination "$DESTINATION_A"
    wait_for_destination "$DESTINATION_B"
    wait_for_destination "$DESTINATION_C"
    run_probe "certificate-${role}-recovery" matrix
  done
  record_pass KIND-14 'cert-manager reissue changes leaf serial and Reloader replaces GW/RT; SDK reconverges'
}
