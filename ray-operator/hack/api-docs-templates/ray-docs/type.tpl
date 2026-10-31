{{- /* Adapted from the crd-ref-docs v0.2.0 built-in markdown templates: h2 headings and no blank-line padding. */ -}}
{{- define "type" -}}
{{- $type := . -}}
{{- if markdownShouldRenderType $type }}

## {{ $type.Name }}
{{- if $type.IsAlias }}

_Underlying type:_ _{{ markdownRenderTypeLink $type.UnderlyingType }}_
{{- end }}
{{- with $type.Doc }}

{{ . }}
{{- end }}
{{- if $type.Validation }}

_Validation:_
{{- range $type.Validation }}
- {{ . }}
{{- end }}
{{- end }}
{{- if $type.References }}

_Appears in:_
{{- range $type.SortedReferences }}
- {{ markdownRenderTypeLink . }}
{{- end }}
{{- end }}
{{- if $type.Members }}

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
{{- if $type.GVK }}
| `apiVersion` _string_ | `{{ $type.GVK.Group }}/{{ $type.GVK.Version }}` | | |
| `kind` _string_ | `{{ $type.GVK.Kind }}` | | |
{{- end }}
{{- range $type.Members }}
| `{{ .Name }}` _{{ markdownRenderType .Type }}_ | {{ template "type_members" . }} | {{ markdownRenderDefault .Default }} | {{ range .Validation -}} {{ markdownRenderFieldDoc . }} <br />{{ end }} |
{{- end }}
{{- end }}
{{- if $type.EnumValues }}

| Field | Description |
| --- | --- |
{{- range $type.EnumValues }}
| `{{ .Name }}` | {{ markdownRenderFieldDoc .Doc }} |
{{- end }}
{{- end }}
{{- end }}
{{- end -}}
