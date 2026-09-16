{{- define "openfoodfacts.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "openfoodfacts.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "openfoodfacts.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "openfoodfacts.labels" -}}
app.kubernetes.io/name: {{ include "openfoodfacts.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "openfoodfacts.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (printf "%s-spark" (include "openfoodfacts.fullname" .)) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "openfoodfacts.image" -}}
{{- printf "%s:%s" .repository .tag }}
{{- end }}