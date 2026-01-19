{{/*
Expand the name of the chart.
*/}}
{{- define "scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "scheduler.fullname" -}}
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
{{- define "scheduler.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "scheduler.labels" -}}
helm.sh/chart: {{ include "scheduler.chart" . }}
{{ include "scheduler.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "scheduler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "scheduler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "scheduler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "scheduler.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Get database URL from config or global
*/}}
{{- define "scheduler.databaseUrl" -}}
{{- if .Values.config.database_url }}
{{- .Values.config.database_url }}
{{- else if .Values.global.database.url }}
{{- .Values.global.database.url }}
{{- else }}
{{- "postgres://sandbox0:sandbox0@postgresql:5432/sandbox0?sslmode=disable" }}
{{- end }}
{{- end }}

{{/*
Get internal JWT secret name
*/}}
{{- define "scheduler.internalJwtSecretName" -}}
{{- if .Values.secrets.internalJwtSecretName }}
{{- .Values.secrets.internalJwtSecretName }}
{{- else if .Values.global.jwt.internalJwtSecretName }}
{{- .Values.global.jwt.internalJwtSecretName }}
{{- else }}
{{- "sandbox0-internal-jwt" }}
{{- end }}
{{- end }}

{{/*
Get internal JWT private key key
*/}}
{{- define "scheduler.internalJwtPrivateKeyKey" -}}
{{- if .Values.secrets.internalJwtPrivateKeyKey }}
{{- .Values.secrets.internalJwtPrivateKeyKey }}
{{- else if .Values.global.jwt.privateKeyKey }}
{{- .Values.global.jwt.privateKeyKey }}
{{- else }}
{{- "private.key" }}
{{- end }}
{{- end }}

{{/*
Get internal JWT public key key
*/}}
{{- define "scheduler.internalJwtPublicKeyKey" -}}
{{- if .Values.secrets.internalJwtPublicKeyKey }}
{{- .Values.secrets.internalJwtPublicKeyKey }}
{{- else if .Values.global.jwt.publicKeyKey }}
{{- .Values.global.jwt.publicKeyKey }}
{{- else }}
{{- "public.key" }}
{{- end }}
{{- end }}
