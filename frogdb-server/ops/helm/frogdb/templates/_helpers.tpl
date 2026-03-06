{{/*
Expand the name of the chart.
*/}}
{{- define "frogdb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "frogdb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "frogdb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "frogdb.labels" -}}
helm.sh/chart: {{ include "frogdb.chart" . }}
{{ include "frogdb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "frogdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "frogdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "frogdb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "frogdb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the headless service name for cluster discovery
*/}}
{{- define "frogdb.headlessServiceName" -}}
{{- printf "%s-headless" (include "frogdb.fullname" .) }}
{{- end }}

{{/*
Generate cluster peer addresses for Raft discovery.
This generates the list of peer addresses based on the StatefulSet naming pattern.
Format: frogdb-0.frogdb-headless.namespace.svc.cluster.local:16379
*/}}
{{- define "frogdb.clusterPeers" -}}
{{- $fullname := include "frogdb.fullname" . -}}
{{- $headless := include "frogdb.headlessServiceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $busPort := .Values.cluster.busPort | int -}}
{{- $replicas := .Values.replicaCount | int -}}
{{- $peers := list -}}
{{- range $i := until $replicas -}}
{{- $peers = append $peers (printf "%s-%d.%s.%s.svc.cluster.local:%d" $fullname $i $headless $namespace $busPort) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

{{/*
Return the appropriate storage class for the cloud provider
*/}}
{{- define "frogdb.storageClass" -}}
{{- if .Values.persistence.storageClass -}}
{{- .Values.persistence.storageClass -}}
{{- else -}}
{{- "" -}}
{{- end -}}
{{- end }}
