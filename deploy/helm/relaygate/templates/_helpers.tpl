{{/* Base name. It is capped so component suffixes remain unique DNS labels. */}}
{{- define "relaygate.name" -}}
{{- .Chart.Name | trunc 45 | trimSuffix "-" }}
{{- end }}

{{- define "relaygate.fullname" -}}
{{- $name := .Chart.Name }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 45 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 45 | trimSuffix "-" }}
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
{{- printf "%s-rt-%d" (include "relaygate.fullname" .root) .index | trunc 63 | trimSuffix "-" }}
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
app.kubernetes.io/name: {{ include "relaygate.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "relaygate.labels" -}}
helm.sh/chart: {{ include "relaygate.chart" .root }}
{{ include "relaygate.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/part-of: relaygate
{{- end }}

{{- define "relaygate.shardDirectory" -}}
{{- $root := . -}}
{{- $shards := list -}}
{{- range $index := until (int .Values.routeTable.shardCount) -}}
{{- $serviceName := include "relaygate.routeTableName" (dict "root" $root "index" $index) -}}
{{- $endpoint := printf "%s.%s.svc.%s:%d" $serviceName $root.Release.Namespace $root.Values.clusterDomain (int $root.Values.routeTable.port) -}}
{{- $shards = append $shards (dict "id" (printf "rt-%d" $index) "endpoint" $endpoint) -}}
{{- end -}}
{{- dict "format_version" 1 "authority_hash" "sha256-modulo-v1" "shards" $shards | toJson -}}
{{- end }}

{{- define "relaygate.validateValues" -}}
{{- if not .Values.internalTransport.trustedLocalAdapter }}
{{- fail "internalTransport.trustedLocalAdapter must be true to acknowledge the current local/CI plain-TCP key adapter" }}
{{- end }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "clusterDomain" "value" .Values.clusterDomain) }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "credentials.existingSecret" "value" .Values.credentials.existingSecret) }}
{{- range .Values.imagePullSecrets }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "imagePullSecrets[].name" "value" .name) }}
{{- end }}
{{- if .Values.serviceAccount.name }}
{{- include "relaygate.validateDnsSubdomainLabels" (dict "name" "serviceAccount.name" "value" .Values.serviceAccount.name) }}
{{- end }}
{{- if eq (int .Values.gateway.sdkPort) (int .Values.gateway.peerPort) }}
{{- fail "gateway.sdkPort and gateway.peerPort must be different" }}
{{- end }}
{{- range $component := list "gateway" "routeTable" }}
{{- $image := index $.Values $component "image" -}}
{{- if contains "@" $image.repository }}
{{- fail (printf "%s.image.repository must not contain a digest" $component) }}
{{- end }}
{{- if regexMatch ":[^/]+$" $image.repository }}
{{- fail (printf "%s.image.repository must not contain a tag" $component) }}
{{- end }}
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
{{- $managed := list "POD_NAME" "POD_NAMESPACE" "RELAYGATE_BIND_ADDR" "RELAYGATE_PEER_BIND_ADDR" "RELAYGATE_RT_TRUSTED_LOCAL" "RELAYGATE_RT_SHARD_DIRECTORY_PATH" "RELAYGATE_GATEWAY_NAME" "RELAYGATE_GATEWAY_LOCATOR" "RELAYGATE_INTERNAL_GATEWAY_KEYS" "RELAYGATE_CLIENT_KEYS" "RELAYGATE_LOG" "RELAYGATE_LOG_FORMAT" "RELAYGATE_STATS_INTERVAL_MS" -}}
{{- range .Values.gateway.extraEnv }}
{{- if has .name $managed }}
{{- fail (printf "gateway.extraEnv cannot override chart-managed variable %s" .name) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateGatewayPodMetadata" -}}
{{- $reservedLabels := list "helm.sh/chart" "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" "app.kubernetes.io/managed-by" "app.kubernetes.io/part-of" -}}
{{- range $key, $_ := .Values.gateway.podLabels }}
{{- if has $key $reservedLabels }}
{{- fail (printf "gateway.podLabels cannot override chart-managed label %s" $key) }}
{{- end }}
{{- end }}
{{- $reservedAnnotations := list "checksum/shard-directory" "relaygate.io/credentials-reload" -}}
{{- range $key, $_ := .Values.gateway.podAnnotations }}
{{- if has $key $reservedAnnotations }}
{{- fail (printf "gateway.podAnnotations cannot override chart-managed annotation %s" $key) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateRouteTableExtraEnv" -}}
{{- $managed := list "RELAYGATE_RT_TRUSTED_LOCAL" "RELAYGATE_RT_BIND_ADDR" "RELAYGATE_RT_SHARD_DIRECTORY_PATH" "RELAYGATE_RT_SHARD_ID" "RELAYGATE_RT_LEASE_TTL_MS" "RELAYGATE_INTERNAL_GATEWAY_KEYS" "RELAYGATE_LOG" "RELAYGATE_LOG_FORMAT" -}}
{{- range .Values.routeTable.extraEnv }}
{{- if has .name $managed }}
{{- fail (printf "routeTable.extraEnv cannot override chart-managed variable %s" .name) }}
{{- end }}
{{- end }}
{{- end }}

{{- define "relaygate.validateRouteTablePodMetadata" -}}
{{- $reservedLabels := list "helm.sh/chart" "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" "app.kubernetes.io/managed-by" "app.kubernetes.io/part-of" "relaygate.io/shard" -}}
{{- range $key, $_ := .Values.routeTable.podLabels }}
{{- if has $key $reservedLabels }}
{{- fail (printf "routeTable.podLabels cannot override chart-managed label %s" $key) }}
{{- end }}
{{- end }}
{{- $reservedAnnotations := list "checksum/shard-directory" "relaygate.io/credentials-reload" -}}
{{- range $key, $_ := .Values.routeTable.podAnnotations }}
{{- if has $key $reservedAnnotations }}
{{- fail (printf "routeTable.podAnnotations cannot override chart-managed annotation %s" $key) }}
{{- end }}
{{- end }}
{{- end }}
