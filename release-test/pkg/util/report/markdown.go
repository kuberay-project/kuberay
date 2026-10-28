package report

import (
	"embed"
	"strings"
	"text/template"
)

// templateFS holds the markdown the runner writes: a unit's report.md, headed by front.tmpl, and SUMMARY.md.
//
//go:embed templates/*.tmpl
var templateFS embed.FS

var tmpl = template.Must(template.New("").Funcs(template.FuncMap{
	"cell": cell,
	"esc":  esc,
	"join": strings.Join,
}).ParseFS(templateFS, "templates/*.tmpl"))

// Render executes templates/<name>. The templates are fixed and the tests run each, so an error is a bug in one.
func Render(name string, data any) string {
	var builder strings.Builder
	if err := tmpl.ExecuteTemplate(&builder, name, data); err != nil {
		panic(err)
	}
	return builder.String()
}

// cell fits a command or step name into one markdown table cell.
func cell(text string) string {
	text, _, _ = strings.Cut(text, "\n")
	return esc(text)
}

// esc escapes the pipes in a markdown table cell.
func esc(text string) string {
	return strings.ReplaceAll(text, "|", `\|`)
}
