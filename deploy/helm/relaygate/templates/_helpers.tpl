{{/* Base name. It is capped so component suffixes remain unique DNS labels. */}}
{{- define "relaygate.name" -}}
{{- .Chart.Name | trunc 45 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.fullname" -}}
{{- $base := .Release.Name -}}
{{- if gt (len $base) 45 -}}
{{- printf "%s-%s" ($base | trunc 36 | trimSuffix "-") ($base | sha256sum | trunc 8) -}}
{{- else -}}
{{- $base | trimSuffix "-" -}}
{{- end }}
{{- end }}

{{- define "relaygate.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.gatewayName" -}}
{{- printf "%s-gateway" (include "relaygate.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.gatewayPeerServiceName" -}}
{{- printf "%s-gateway-peer" (include "relaygate.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.gatewayServiceName" -}}
{{- printf "%s" (include "relaygate.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.routeTableName" -}}
{{- printf "%s-rt" (include "relaygate.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.routeTablePodName" -}}
{{- printf "%s-%d" (include "relaygate.routeTableName" .root) .index }}
{{- end }}

{{- define "relaygate.shardDirectoryName" -}}
{{- printf "%s-shard-directory" (include "relaygate.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "relaygate.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "relaygate.selectorLabels" -}}
app.kubernetes.io/name: {{ include "relaygate.name" .root | quote }}
app.kubernetes.io/instance: {{ .root.Release.Name | quote }}
app.kubernetes.io/component: {{ .component | quote }}
{{- end }}

{{- define "relaygate.labels" -}}
helm.sh/chart: {{ include "relaygate.chart" .root | quote }}
{{ include "relaygate.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .root.Release.Service | quote }}
app.kubernetes.io/part-of: "relaygate"
{{- end }}

{{- define "relaygate.shardDirectory" -}}
{{- $root := . -}}
{{- $shards := list -}}
{{- range $index := until (int .Values.routeTable.shardCount) -}}
{{- $podName := include "relaygate.routeTablePodName" (dict "root" $root "index" $index) -}}
{{- $serviceName := include "relaygate.routeTableName" $root -}}
{{- $endpoint := printf "%s.%s.%s.svc.%s:%d" $podName $serviceName $root.Release.Namespace $root.Values.clusterDomain (int $root.Values.routeTable.port) -}}
{{- $shards = append $shards (dict "id" (printf "rt-%d" $index) "endpoint" $endpoint) -}}
{{- end -}}
{{- dict "format_version" 1 "authority_hash" "sha256-modulo-v1" "shards" $shards | toJson -}}
{{- end }}

{{- define "relaygate.validateValues" -}}
{{- if not (regexMatch "^[a-z]([-a-z0-9]*[a-z0-9])?$" .Release.Name) }}
{{- fail "the Helm release name must be an RFC 1035 label so generated Service names are valid" }}
{{- end }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "clusterDomain" "value" .Values.clusterDomain) }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "credentials.existingSecret" "value" .Values.credentials.existingSecret) }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "tls.edge.existingSecret" "value" .Values.tls.edge.existingSecret) }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "tls.edge.serverName" "value" .Values.tls.edge.serverName) }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "tls.internal.existingSecret" "value" .Values.tls.internal.existingSecret) }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "tls.internal.gatewayServerName" "value" .Values.tls.internal.gatewayServerName) }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "tls.internal.routeTableServerName" "value" .Values.tls.internal.routeTableServerName) }}
{{- $gatewayLastIndex := sub (int .Values.gateway.replicaCount) 1 -}}
{{- $gatewayPodName := printf "%s-%d" (include "relaygate.gatewayName" .) $gatewayLastIndex -}}
{{- $gatewayHostname := printf "%s.%s.%s.svc.%s" $gatewayPodName (include "relaygate.gatewayPeerServiceName" .) .Release.Namespace .Values.clusterDomain -}}
{{- include "relaygate.validateGeneratedHostname" (dict "name" "Gateway peer hostname" "value" $gatewayHostname) }}
{{- $routeTableLastIndex := sub (int .Values.routeTable.shardCount) 1 -}}
{{- $routeTablePodName := include "relaygate.routeTablePodName" (dict "root" . "index" $routeTableLastIndex) -}}
{{- $routeTableHostname := printf "%s.%s.%s.svc.%s" $routeTablePodName (include "relaygate.routeTableName" .) .Release.Namespace .Values.clusterDomain -}}
{{- include "relaygate.validateGeneratedHostname" (dict "name" "RouteTable shard hostname" "value" $routeTableHostname) }}
{{- range .Values.imagePullSecrets }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "imagePullSecrets[].name" "value" .name) }}
{{- end }}
{{- if .Values.serviceAccount.name }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "serviceAccount.name" "value" .Values.serviceAccount.name) }}
{{- end }}
{{- if eq (int .Values.gateway.sdkPort) (int .Values.gateway.peerPort) }}
{{- fail "gateway.sdkPort and gateway.peerPort must be different" }}
{{- end }}
{{- if and .Values.metrics.enabled (or (eq (int .Values.metrics.gatewayPort) (int .Values.gateway.sdkPort)) (eq (int .Values.metrics.gatewayPort) (int .Values.gateway.peerPort))) }}
{{- fail "metrics.gatewayPort must be different from Gateway SDK and peer ports" }}
{{- end }}
{{- if and .Values.metrics.enabled (eq (int .Values.metrics.routeTablePort) (int .Values.routeTable.port)) }}
{{- fail "metrics.routeTablePort must be different from the RouteTable service port" }}
{{- end }}
{{- if le (mul (int .Values.gateway.terminationGracePeriodSeconds) 1000) (int .Values.gateway.drainTimeoutMs) }}
{{- fail "gateway.terminationGracePeriodSeconds must exceed gateway.drainTimeoutMs" }}
{{- end }}
{{- range $component := list "gateway" "routeTable" }}
{{- $image := index $.Values $component "image" -}}
{{- if contains "@" $image.repository }}
{{- fail (printf "%s.image.repository must not contain a digest" $component) }}
{{- end }}
{{- if regexMatch ":[^/]*$" $image.repository }}
{{- fail (printf "%s.image.repository must not contain a tag" $component) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateGeneratedHostname" -}}
{{- include "relaygate.validateDnsSubdomainLabels" . }}
{{- if gt (len .value) 253 }}
{{- fail (printf "%s must not exceed 253 characters" .name) }}
{{- end }}
{{- end }}

{{- define "relaygate.validateDnsSubdomainLabels" -}}
{{- range $label := splitList "." .value }}
{{- if gt (len $label) 63 }}
{{- fail (printf "%s must not contain a DNS label longer than 63 characters" $.name) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateGatewayExtraEnv" -}}
{{- $managed := list "POD_NAME" "POD_NAMESPACE" "RELAYGATE_BIND_ADDR" "RELAYGATE_PEER_BIND_ADDR" "RELAYGATE_RT_SHARD_DIRECTORY_PATH" "RELAYGATE_GATEWAY_NAME" "RELAYGATE_GATEWAY_LOCATOR" "RELAYGATE_INTERNAL_GATEWAY_KEYS" "RELAYGATE_CLUSTER_TOKEN" "RELAYGATE_NEXT_CLUSTER_TOKEN" "RELAYGATE_SDK_TLS_CA_PATH" "RELAYGATE_SDK_TLS_CERT_PATH" "RELAYGATE_SDK_TLS_KEY_PATH" "RELAYGATE_SDK_TLS_SERVER_NAME" "RELAYGATE_INTERNAL_TLS_CA_PATH" "RELAYGATE_INTERNAL_TLS_CERT_PATH" "RELAYGATE_INTERNAL_TLS_KEY_PATH" "RELAYGATE_PEER_TLS_SERVER_NAME" "RELAYGATE_RT_TLS_SERVER_NAME" "RELAYGATE_LOG" "RELAYGATE_LOG_FORMAT" "RELAYGATE_DRAIN_TIMEOUT_MS" "RELAYGATE_STATS_INTERVAL_MS" "RELAYGATE_METRICS_BIND_ADDR" "RELAYGATE_METRICS_INTERVAL_MS" -}}
{{- range .Values.gateway.extraEnv }}
{{- if has .name $managed }}
{{- fail (printf "gateway.extraEnv cannot override chart-managed variable %s" .name) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateGatewayPodMetadata" -}}
{{- $reservedLabels := list "helm.sh/chart" "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" "app.kubernetes.io/managed-by" "app.kubernetes.io/part-of" "apps.kubernetes.io/pod-index" -}}
{{- range $key, $_ := .Values.gateway.podLabels }}
{{- if has $key $reservedLabels }}
{{- fail (printf "gateway.podLabels cannot override chart-managed label %s" $key) }}
{{- end }}
{{- end }}
{{- $reservedAnnotations := list "checksum/shard-directory" "relaygate.io/credentials-reload" "relaygate.io/tls-reload" "prometheus.io/scrape" "prometheus.io/path" "prometheus.io/port" -}}
{{- range $key, $_ := .Values.gateway.podAnnotations }}
{{- if has $key $reservedAnnotations }}
{{- fail (printf "gateway.podAnnotations cannot override chart-managed annotation %s" $key) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateRouteTableExtraEnv" -}}
{{- $managed := list "POD_INDEX" "RELAYGATE_RT_BIND_ADDR" "RELAYGATE_RT_SHARD_DIRECTORY_PATH" "RELAYGATE_RT_SHARD_ID" "RELAYGATE_RT_LEASE_TTL_MS" "RELAYGATE_INTERNAL_GATEWAY_KEYS" "RELAYGATE_INTERNAL_TLS_CA_PATH" "RELAYGATE_INTERNAL_TLS_CERT_PATH" "RELAYGATE_INTERNAL_TLS_KEY_PATH" "RELAYGATE_LOG" "RELAYGATE_LOG_FORMAT" "RELAYGATE_METRICS_BIND_ADDR" "RELAYGATE_METRICS_INTERVAL_MS" -}}
{{- range .Values.routeTable.extraEnv }}
{{- if has .name $managed }}
{{- fail (printf "routeTable.extraEnv cannot override chart-managed variable %s" .name) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateRouteTablePodMetadata" -}}
{{- $reservedLabels := list "helm.sh/chart" "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" "app.kubernetes.io/managed-by" "app.kubernetes.io/part-of" "apps.kubernetes.io/pod-index" -}}
{{- range $key, $_ := .Values.routeTable.podLabels }}
{{- if has $key $reservedLabels }}
{{- fail (printf "routeTable.podLabels cannot override chart-managed label %s" $key) }}
{{- end }}
{{- end }}
{{- $reservedAnnotations := list "checksum/shard-directory" "relaygate.io/credentials-reload" "relaygate.io/tls-reload" "prometheus.io/scrape" "prometheus.io/path" "prometheus.io/port" -}}
{{- range $key, $_ := .Values.routeTable.podAnnotations }}
{{- if has $key $reservedAnnotations }}
{{- fail (printf "routeTable.podAnnotations cannot override chart-managed annotation %s" $key) }}
{{- end }}
{{- end }}
{{- end }}
