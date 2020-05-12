{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}

{{- define "secor.fullname" -}}
{{- printf "%s-%s" .Release.Name .Values.app.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "secor.chart"}}
{{- printf "%s-%s"  .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "secor.release"}}
{{- printf "%s" .Release.Name }}
{{- end -}}

{{- define "secor.matchLabels" -}}
app.kubernetes.io/name: secor
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "secor.metaLabels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
kubernetes.io/change-cause: {{ .Values.changecause }}
{{- end -}}

{{- define "secor.labels" -}}
{{ include "secor.matchLabels" . }}
{{ include "secor.metaLabels" . }}
{{- end -}}

# Component names section

#####################
# Deployments block #
#####################

{{- define "secor.component.deployment.secor"}}
{{- printf "%s-%s" .Release.Name "deployment-secor" }}
{{- end -}}

{{- define "secor.component.deployment.secormon"}}
{{- printf "%s-%s" .Release.Name "deployment-secor-monitor" }}
{{- end -}}
