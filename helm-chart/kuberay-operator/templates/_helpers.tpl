{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "kuberay-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Allow the component label to be overridden, otherwise provide a default value.
*/}}
{{- define "kuberay-operator.component" -}}
{{- default .Chart.Name .Values.componentOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kuberay-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kuberay-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "kuberay-operator.labels" -}}
app.kubernetes.io/name: {{ include "kuberay-operator.name" . }}
helm.sh/chart: {{ include "kuberay-operator.chart" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- /* Create the name of the deployment to use. */ -}}
{{- define "kuberay-operator.deployment.name" -}}
{{- include "kuberay-operator.fullname" . }}
{{- end -}}

{{/*
FeatureGates
*/}}
{{- define "kuberay.featureGates" -}}
{{- $features := "" }}
{{- range .Values.featureGates }}
  {{- $str := printf "%s=%t," .name .enabled }}
  {{- $features = print $features $str }}
{{- end }}
{{- with .Values.featureGates }}
--feature-gates={{ $features | trimSuffix "," }}
{{- end }}
{{- end }}

{{/*
Whether the KubernetesWAS feature gate is enabled in .Values.featureGates.
*/}}
{{- define "kuberay.kubernetesWASEnabled" -}}
{{- range .Values.featureGates -}}
{{- if and (eq .name "KubernetesWAS") .enabled -}}
true
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Whether the NodeLabelDelivery feature gate is enabled in .Values.featureGates.
*/}}
{{- define "kuberay.nodeLabelDeliveryEnabled" -}}
{{- range .Values.featureGates -}}
{{- if and (eq .name "NodeLabelDelivery") .enabled -}}
true
{{- end -}}
{{- end -}}
{{- end }}

{{- /* Names shared by the admission webhook templates and the Deployment. */ -}}
{{- define "kuberay-operator.webhook.serviceName" -}}
{{- include "kuberay-operator.fullname" . -}}-webhook
{{- end -}}
{{- define "kuberay-operator.webhook.certName" -}}
{{- include "kuberay-operator.fullname" . -}}-serving-cert
{{- end -}}
{{- define "kuberay-operator.webhook.certSecretName" -}}
{{- include "kuberay-operator.fullname" . -}}-webhook-server-cert
{{- end -}}

{{- /* Create the name of the service to use. */ -}}
{{- define "kuberay-operator.service.name" -}}
{{- include "kuberay-operator.fullname" . }}
{{- end -}}

{{- /* Create the name of the service account to use. */ -}}
{{- define "kuberay-operator.serviceAccount.name" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "kuberay-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- /* Create the name of the cluster role to use. */ -}}
{{- define "kuberay-operator.clusterRole.name" -}}
{{- include "kuberay-operator.fullname" . -}}
{{- end -}}

{{- /* Create the name of the cluster role binding to use. */ -}}
{{- define "kuberay-operator.clusterRoleBinding.name" -}}
{{- include "kuberay-operator.fullname" . -}}
{{- end -}}

{{- /* Create the name of the role to use. */ -}}
{{- define "kuberay-operator.role.name" -}}
{{- include "kuberay-operator.fullname" . -}}
{{- end -}}

{{- /* Create the name of the role binding to use. */ -}}
{{- define "kuberay-operator.roleBinding.name" -}}
{{- include "kuberay-operator.fullname" . -}}
{{- end -}}

{{/*
Validate operator configuration values.
This template validates reconcileConcurrency, kubeClient.qps, and kubeClient.burst.
It should be called early in the deployment to ensure invalid values are caught.
*/}}
{{- define "kuberay-operator.validateConfig" -}}
{{- if hasKey .Values "reconcileConcurrency" }}
{{- $rc := toString .Values.reconcileConcurrency }}
{{- if not (regexMatch "^[1-9][0-9]*$" $rc) }}
{{- fail (printf "values.reconcileConcurrency must be a positive integer, got %q" $rc) }}
{{- end }}
{{- end }}
{{- if hasKey .Values "kubeClient" }}
{{- if hasKey .Values.kubeClient "qps" }}
{{- $qps := toString .Values.kubeClient.qps }}
{{- if not (regexMatch "^[0-9]+(\\.[0-9]+)?$" $qps) }}
{{- fail (printf "values.kubeClient.qps must be a valid float number, got %q" $qps) }}
{{- end }}
{{- end }}
{{- if hasKey .Values.kubeClient "burst" }}
{{- $burst := toString .Values.kubeClient.burst }}
{{- if not (regexMatch "^[0-9]+$" $burst) }}
{{- fail (printf "values.kubeClient.burst must be a non-negative integer, got %q" $burst) }}
{{- end }}
{{- end }}
{{- end }}
{{- $nodeEventForwarderEnabled := false -}}
{{- if and (hasKey .Values "nodeEventForwarder") (hasKey .Values.nodeEventForwarder "enabled") -}}
  {{- $nodeEventForwarderEnabled = .Values.nodeEventForwarder.enabled -}}
{{- end -}}
{{- if and .Values.singleNamespaceInstall $nodeEventForwarderEnabled }}
{{- fail "nodeEventForwarder is not supported when singleNamespaceInstall is true because Node events are cluster-scoped and recorded in 'default' or 'kube-system', which requires permissions outside a single namespace." }}
{{- end }}
{{- if and .Values.singleNamespaceInstall .Values.webhooks.enabled }}
{{- fail "webhooks are not supported when singleNamespaceInstall is true because the webhook configurations and Node reads are cluster-scoped." }}
{{- end }}
{{- end }}

{{- /* Create the name of the node event forwarder role to use. */ -}}
{{- define "kuberay-operator.nodeEventForwarderRole.name" -}}
{{- include "kuberay-operator.fullname" . -}}-node-event-forwarder
{{- end -}}

{{- /* Create the name of the node event forwarder role binding to use. */ -}}
{{- define "kuberay-operator.nodeEventForwarderRoleBinding.name" -}}
{{- include "kuberay-operator.fullname" . -}}-node-event-forwarder
{{- end -}}

{{- /* Create the name of the leader election role to use. */ -}}
{{- define "kuberay-operator.leaderElectionRole.name" -}}
{{- include "kuberay-operator.fullname" . -}}-leader-election
{{- end -}}

{{- /* Create the name of the leader election role binding to use. */ -}}
{{- define "kuberay-operator.leaderElectionRoleBinding.name" -}}
{{- include "kuberay-operator.fullname" . -}}-leader-election
{{- end -}}

{{/*
Create a template to ensure consistency for Role and ClusterRole.
*/}}
{{- define "role.consistentRules" -}}
rules:
{{- if .clusterScoped }}
# the pods/binding webhook reads the bound node's labels; nodes are cluster-scoped so only the ClusterRole carries it
- apiGroups:
  - ""
  resources:
  - nodes
  verbs:
  - get
{{- end }}
- apiGroups:
  - ""
  resources:
  - persistentvolumeclaims
  - pods/status
  - services
  verbs:
  - create
  - delete
  - get
  - list
  - patch
  - update
  - watch
- apiGroups:
  - ""
  resources:
  - pods
  verbs:
  - create
  - delete
  - deletecollection
  - get
  - list
  - patch
  - update
  - watch
- apiGroups:
  - ""
  resources:
  - pods/proxy
  - services/status
  verbs:
  - get
  - patch
  - update
- apiGroups:
  - ""
  resources:
  - pods/resize
  verbs:
  - patch
- apiGroups:
  - ""
  resources:
  - secrets
  verbs:
  - create
  - delete
  - get
  - list
  - update
  - watch
- apiGroups:
  - ""
  resources:
  - serviceaccounts
  verbs:
  - create
  - delete
  - get
  - list
  - watch
- apiGroups:
  - ""
  resources:
  - services/proxy
  verbs:
  - create
  - get
  - patch
  - update
- apiGroups:
  - batch
  resources:
  - jobs
  verbs:
  - create
  - delete
  - get
  - list
  - patch
  - update
  - watch
- apiGroups:
  - cert-manager.io
  resources:
  - certificates
  verbs:
  - create
  - get
  - list
  - update
  - watch
- apiGroups:
  - cert-manager.io
  resources:
  - certificates/status
  verbs:
  - get
- apiGroups:
  - cert-manager.io
  resources:
  - issuers
  verbs:
  - create
  - get
  - list
  - watch
- apiGroups:
  - coordination.k8s.io
  resources:
  - leases
  verbs:
  - create
  - get
  - list
  - update
- apiGroups:
  - discovery.k8s.io
  resources:
  - endpointslices
  verbs:
  - get
  - list
  - watch
- apiGroups:
  - events.k8s.io
  resources:
  - events
  verbs:
  - create
  - patch
- apiGroups:
  - extensions
  - networking.k8s.io
  resources:
  - ingresses
  verbs:
  - create
  - delete
  - get
  - list
  - patch
  - update
  - watch
- apiGroups:
  - gateway.networking.k8s.io
  resources:
  - gateways
  - httproutes
  verbs:
  - create
  - delete
  - get
  - list
  - update
  - watch
- apiGroups:
  - networking.k8s.io
  resources:
  - ingressclasses
  verbs:
  - get
  - list
  - watch
- apiGroups:
  - networking.k8s.io
  resources:
  - networkpolicies
  verbs:
  - create
  - delete
  - get
  - list
  - update
  - watch
- apiGroups:
  - ray.io
  resources:
  - rayclusters
  - raycronjobs
  - rayjobs
  - rayservices
  verbs:
  - create
  - delete
  - get
  - list
  - patch
  - update
  - watch
- apiGroups:
  - ray.io
  resources:
  - rayclusters/finalizers
  - raycronjobs/finalizers
  - rayjobs/finalizers
  - rayservices/finalizers
  verbs:
  - update
- apiGroups:
  - ray.io
  resources:
  - rayclusters/status
  - raycronjobs/status
  - rayjobs/status
  - rayservices/status
  verbs:
  - get
  - patch
  - update
- apiGroups:
  - rbac.authorization.k8s.io
  resources:
  - rolebindings
  verbs:
  - create
  - delete
  - get
  - list
  - watch
- apiGroups:
  - rbac.authorization.k8s.io
  resources:
  - roles
  verbs:
  - create
  - delete
  - get
  - list
  - update
  - watch
- apiGroups:
  - route.openshift.io
  resources:
  - routes
  verbs:
  - create
  - delete
  - get
  - list
  - patch
  - update
  - watch
{{- if .kubernetesWASEnabled }}
- apiGroups:
  - scheduling.k8s.io
  resources:
  - podgroups
  - workloads
  verbs:
  - create
  - delete
  - get
  - list
  - patch
  - update
  - watch
{{- end -}}
{{- if or .batchSchedulerEnabled (eq .batchSchedulerName "volcano") }}
- apiGroups:
  - scheduling.volcano.sh
  resources:
  - podgroups
  verbs:
  - create
  - delete
  - get
  - list
  - update
  - watch
{{- end -}}
{{- if or .batchSchedulerEnabled (eq .batchSchedulerName "scheduler-plugins") }}
- apiGroups:
  - scheduling.x-k8s.io
  resources:
  - podgroups
  verbs:
  - create
  - get
  - list
  - watch
{{- end -}}
{{- end -}}
