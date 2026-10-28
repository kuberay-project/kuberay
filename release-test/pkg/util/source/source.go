package source

import (
	"context"
	"crypto/sha1" //nolint:gosec // git object ids are SHA-1; this is not a security boundary
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"

	"github.com/ray-project/kuberay/release-test/pkg/util"
)

var (
	docsVersion = regexp.MustCompile(`^https://docs\.ray\.io/en/[^/]+/`)
	kuberayRaw  = "https://raw.githubusercontent.com/ray-project/kuberay/"
	httpClient  = &http.Client{Timeout: time.Minute}

	codeDirective = regexp.MustCompile(`^\{(code|code-block|code-cell|literalinclude|sourcecode)\}`)
	continuedLine = regexp.MustCompile(`\\\n[ \t]*`)
	commentLine   = regexp.MustCompile(`(?m)^[ \t]*#[^\n]*\n?`)
	// kuberayVersion: what the manifest writes as $KUBERAY_VERSION, plus the kubectl plugin tarball
	// and ArgoCD's targetRevision.
	kuberayVersion = regexp.MustCompile(`(` + util.KuberayVersionRefs + `|kubectl-ray_|targetRevision:[ \t]*"?)v?` + util.ReleaseVersion)
)

// Source is what Fetch found for one Item.
type Source struct {
	SHA  string // git blob sha of the source
	File string // what the plan reads, relative to the repository
	Dead string // set when the source is gone: the URL that no longer resolves
}

// Fetch hashes an Item's source: a docs page by its commands (pageCommands) on ray master, a
// repository path as it is at version, the tag under test, another URL as served, a procedure as
// written. Everything but the procedure is cached for the skills.
func Fetch(ctx context.Context, p util.Paths, item util.Item, version string) (Source, error) {
	switch {
	case item.Source == "":
		return Source{SHA: BlobSHA([]byte(item.Procedure)), File: p.Rel(p.Items)}, nil
	case strings.HasPrefix(item.Source, util.DocsSite):
		return fetchDocsPage(ctx, p, item)
	case strings.HasPrefix(item.Source, "https://"):
		return fetchFile(ctx, p, item.Source, item.ID+".src")
	default:
		// Units apply a sample from the tag, not from this checkout, so the tag's copy is what they test.
		return fetchFile(ctx, p, kuberayRaw+version+"/"+item.Source, item.ID+filepath.Ext(item.Source))
	}
}

// fetchFile caches the content of url as name; a 404 makes the source dead.
func fetchFile(ctx context.Context, p util.Paths, url, name string) (Source, error) {
	body, _, found, err := FetchURL(ctx, url)
	if err != nil {
		return Source{}, err
	}
	if !found {
		return Source{Dead: url}, nil
	}
	return cacheSource(p, name, body)
}

func fetchDocsPage(ctx context.Context, p util.Paths, item util.Item) (Source, error) {
	// Pages move; follow docs.ray.io's redirects on master to find where the markdown is today.
	requested := docsVersion.ReplaceAllString(item.Source, util.DocsMaster)
	requested, _, _ = strings.Cut(requested, "#")
	requested, _, _ = strings.Cut(requested, "?")
	_, final, found, err := FetchURL(ctx, requested)
	if err != nil {
		return Source{}, err
	}
	page, ok := docsPagePath(requested, final)
	if !found || !ok {
		return Source{Dead: final}, nil
	}
	for _, ext := range []string{"md", "rst", "ipynb"} {
		body, _, found, err := FetchURL(ctx, fmt.Sprintf(util.RayDocsRaw, page, ext))
		if err != nil {
			return Source{}, err
		}
		if found {
			src, err := cacheSource(p, item.ID+"."+ext, body)
			if err != nil || ext != "md" {
				return src, err
			}
			src.SHA = BlobSHA(pageCommands(body))
			return src, nil
		}
	}
	return Source{Dead: final}, nil
}

// docsPagePath maps the URL docs.ray.io redirected to onto a path under doc/source/. ok is false
// when the page was removed: docs.ray.io then redirects to the section index with status 200.
func docsPagePath(requested, final string) (page string, ok bool) {
	page = strings.TrimSuffix(strings.TrimPrefix(final, util.DocsMaster), ".html")
	removed := path.Base(page) == "index" && !strings.HasSuffix(requested, "/index.html")
	return page, !removed
}

func cacheSource(p util.Paths, name string, body []byte) (Source, error) {
	if err := os.MkdirAll(p.Cache, 0o750); err != nil {
		return Source{}, err
	}
	file := filepath.Join(p.Cache, name)
	if err := os.WriteFile(file, body, 0o600); err != nil {
		return Source{}, err
	}
	return Source{SHA: BlobSHA(body), File: p.Rel(file)}, nil
}

// FetchURL returns the body and the URL after redirects; found is false on 404. Any other non-200
// status is an error, so a rate limit or an outage is never taken for a removed page.
func FetchURL(ctx context.Context, url string) (body []byte, final string, found bool, err error) {
	status, body, final, err := get(ctx, url)
	switch {
	case err != nil:
		return nil, "", false, err
	case status == http.StatusNotFound:
		return nil, final, false, nil
	case status != http.StatusOK:
		return nil, "", false, fmt.Errorf("GET %s: HTTP %d", url, status)
	}
	return body, final, true, nil
}

func get(ctx context.Context, url string) (int, []byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, "", err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, resp.Request.URL.String(), err
}

// BlobSHA is git's blob object id, so `git hash-object` reproduces it.
func BlobSHA(content []byte) string {
	hash := sha1.New() //nolint:gosec // git blob ids are SHA-1; this is not a security boundary
	fmt.Fprintf(hash, "blob %d\x00", len(content))
	hash.Write(content)
	return hex.EncodeToString(hash.Sum(nil))
}

// pageCommands is what the plan turns into steps: a page's fenced code blocks, without comment
// lines and with KubeRay version references normalized, so prose edits, reworded comments and the
// version bump after each release leave the unit fresh. A page without code blocks is hashed whole.
func pageCommands(page []byte) []byte {
	blocks := codeBlocks(page)
	if len(blocks) == 0 {
		return page
	}
	commands := strings.Join(blocks, "\n")
	commands = continuedLine.ReplaceAllString(commands, " ")
	commands = commentLine.ReplaceAllString(commands, "")
	commands = kuberayVersion.ReplaceAllString(commands, "${1}VERSION")
	return []byte(commands)
}

// codeBlocks are a page's fenced code blocks, parsed as CommonMark like MyST does. A directive fenced
// with backticks, like {note} or {admonition}, is a fenced block too, but its body is markdown, so
// the code blocks in it count instead of its prose.
func codeBlocks(page []byte) []string {
	var blocks []string
	root := goldmark.DefaultParser().Parse(text.NewReader(page))
	_ = ast.Walk(root, func(node ast.Node, entering bool) (ast.WalkStatus, error) {
		if code, ok := node.(*ast.FencedCodeBlock); ok && entering {
			body := code.Lines().Value(page)
			if info := string(code.Language(page)); strings.HasPrefix(info, "{") && !codeDirective.MatchString(info) {
				blocks = append(blocks, codeBlocks(body)...)
			} else {
				blocks = append(blocks, strings.TrimSuffix(string(body), "\n"))
			}
		}
		return ast.WalkContinue, nil
	})
	return blocks
}
