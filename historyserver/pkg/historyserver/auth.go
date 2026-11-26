package historyserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/emicklei/go-restful/v3"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/sirupsen/logrus"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// AuthCookieName holds the user's own Kubernetes bearer token. It never
	// leaves the history server trust domain: the egress path strips it before
	// proxying to a live Ray cluster.
	AuthCookieName = "hs_auth_token"

	// LoginPath and LogoutPath are raw handlers registered outside the
	// go-restful filter chain, so they stay reachable without a credential.
	LoginPath  = "/login"
	LogoutPath = "/logout"

	// SelectClusterPath renders the cluster selector page. It is the default
	// post-login landing page: the History Server registers no handler for "/",
	// so a redirect to "/" answers 404.
	SelectClusterPath = "/select_cluster"

	// authCacheSize bounds the number of cached TokenReview results.
	authCacheSize = 1024
)

const (
	// DefaultAuthCacheTTL caps how long a revoked token can still be accepted.
	DefaultAuthCacheTTL = 60 * time.Second
	// DefaultAuthCookieMaxAge matches the lifetime of the cluster context cookies.
	DefaultAuthCookieMaxAge = 600 * time.Second
)

// Authenticator validates user-supplied Kubernetes bearer tokens via the
// TokenReview API and caches the outcome for a short window.
//
// Failure semantics are deliberately asymmetric:
//   - an invalid or expired token is a client error (401),
//   - a TokenReview infrastructure failure is a server error (503), never a
//     silent pass-through. Authentication fails closed.
type Authenticator struct {
	client kubernetes.Interface
	// cache maps sha256(token) -> authenticated username, so raw credentials
	// are not retained in memory.
	cache *lru.LRU[string, string]
}

// NewAuthenticator builds an Authenticator from the primary cluster config.
func NewAuthenticator(cfg *rest.Config, cacheTTL time.Duration) (*Authenticator, error) {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes clientset for TokenReview: %w", err)
	}
	return &Authenticator{
		client: clientset,
		cache:  lru.NewLRU[string, string](authCacheSize, nil, cacheTTL),
	}, nil
}

// hashToken keys the cache without retaining the raw credential.
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// authError distinguishes "bad credential" from "cannot tell right now".
type authError struct {
	msg    string
	status int
}

func (e *authError) Error() string { return e.msg }

// authStatus returns the HTTP status an error should map to, defaulting to 401.
func authStatus(err error) int {
	var aerr *authError
	if errors.As(err, &aerr) {
		return aerr.status
	}
	return http.StatusUnauthorized
}

// Authenticate validates the token and returns the authenticated username.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (string, error) {
	if token == "" {
		return "", &authError{status: http.StatusUnauthorized, msg: "missing authentication token"}
	}

	key := hashToken(token)
	if user, ok := a.cache.Get(key); ok {
		return user, nil
	}

	review := &authv1.TokenReview{Spec: authv1.TokenReviewSpec{Token: token}}
	result, err := a.client.AuthenticationV1().TokenReviews().Create(ctx, review, metav1.CreateOptions{})
	if err != nil {
		// The API server could not answer: refuse rather than guess.
		logrus.Errorf("TokenReview request failed: %v", err)
		return "", &authError{status: http.StatusServiceUnavailable, msg: "authentication backend unavailable"}
	}
	if !result.Status.Authenticated {
		reason := result.Status.Error
		if reason == "" {
			reason = "token is not authenticated"
		}
		return "", &authError{status: http.StatusUnauthorized, msg: reason}
	}

	user := result.Status.User.Username
	a.cache.Add(key, user)
	return user, nil
}

// extractToken pulls the credential from the request, header first.
//
// A malformed Authorization header is a hard failure instead of falling back to
// the cookie: a misconfigured client should fail loudly rather than silently
// authenticate as whoever the browser session belongs to.
func extractToken(req *http.Request) (string, error) {
	if raw := req.Header.Get("Authorization"); raw != "" {
		const prefix = "Bearer "
		if !strings.HasPrefix(raw, prefix) {
			return "", &authError{status: http.StatusUnauthorized, msg: "Authorization header must use the Bearer scheme"}
		}
		token := strings.TrimSpace(strings.TrimPrefix(raw, prefix))
		if token == "" {
			return "", &authError{status: http.StatusUnauthorized, msg: "Authorization header carries an empty bearer token"}
		}
		return token, nil
	}
	if cookie, err := req.Cookie(AuthCookieName); err == nil && cookie.Value != "" {
		return cookie.Value, nil
	}
	return "", &authError{status: http.StatusUnauthorized, msg: "missing authentication token"}
}

// wantsHTML reports whether the request is a browser navigation, which should
// be redirected to the login page instead of receiving a bare 401.
func wantsHTML(req *http.Request) bool {
	if req.Header.Get("X-Requested-With") != "" {
		return false
	}
	if mode := req.Header.Get("Sec-Fetch-Mode"); mode != "" {
		return mode == "navigate"
	}
	return strings.Contains(req.Header.Get("Accept"), "text/html")
}

// AuthFilter authenticates every request flowing through the go-restful
// container. It runs before any handler, so dead-cluster serving and live
// pass-through are both gated by it.
func (s *ServerHandler) AuthFilter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	if s.authenticator == nil {
		chain.ProcessFilter(req, resp)
		return
	}

	token, err := extractToken(req.Request)
	if err == nil {
		var user string
		if user, err = s.authenticator.Authenticate(req.Request.Context(), token); err == nil {
			req.SetAttribute(ATTRIBUTE_AUTH_USER, user)
			chain.ProcessFilter(req, resp)
			return
		}
	}

	status := authStatus(err)
	// Browser navigations get a login round-trip so a single link keeps working;
	// XHR callers get a machine-readable error instead of an HTML page.
	if status == http.StatusUnauthorized && wantsHTML(req.Request) {
		next := req.Request.URL.RequestURI()
		http.Redirect(resp.ResponseWriter, req.Request, LoginPath+"?next="+url.QueryEscape(next), http.StatusFound)
		return
	}
	writeAuthError(resp.ResponseWriter, status, err.Error())
}

// writeAuthError follows the repository-wide error contract {"code","message"}.
func writeAuthError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", restful.MIME_JSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": status, "message": msg})
}

// authCookie builds the session cookie. It is HttpOnly so page scripts cannot
// read the Kubernetes token, and SameSite=Lax so it survives a top-level
// navigation arriving from an external link.
func (s *ServerHandler) authCookie(value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     AuthCookieName,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   s.authCookieSecure,
		SameSite: http.SameSiteLaxMode,
	}
}

// safeNext keeps the post-login redirect inside this origin, so a crafted link
// cannot bounce a freshly authenticated browser to another host.
//
// An empty or rejected next lands on the cluster selector rather than "/". The
// History Server registers no handler for "/" (routerRoot is disabled), so "/"
// answers 404 -- which is what the documented paste-a-token flow and every
// re-login after /logout used to hit, since /logout sends the browser to /login
// without a next parameter.
func safeNext(raw string) string {
	if raw == "" {
		return SelectClusterPath
	}
	// Browsers treat a backslash as a path separator, so "/\evil.example" is a
	// protocol-relative URL pointing at evil.example. url.Parse leaves Host empty
	// for that input, so the backslash has to be rejected on the raw value.
	if strings.Contains(raw, "\\") {
		return SelectClusterPath
	}
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(u.Path, "/") {
		return SelectClusterPath
	}
	return u.String()
}

// routerAuth registers /login and /logout as raw handlers, deliberately outside
// the go-restful filter chain so they remain reachable while unauthenticated.
func routerAuth(s *ServerHandler) {
	http.HandleFunc(LoginPath, func(w http.ResponseWriter, r *http.Request) {
		if s.authenticator == nil {
			http.Redirect(w, r, safeNext(r.URL.Query().Get("next")), http.StatusFound)
			return
		}

		switch r.Method {
		case http.MethodGet:
			writeLoginPage(w, http.StatusOK, safeNext(r.URL.Query().Get("next")), "")
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
			if err := r.ParseForm(); err != nil {
				writeLoginError(w, r, http.StatusBadRequest, safeNext(r.URL.Query().Get("next")), "cannot parse login form")
				return
			}
			token := strings.TrimSpace(r.PostFormValue("token"))
			next := safeNext(r.PostFormValue("next"))

			// Validate before setting any cookie: a rejected credential must not
			// leave state behind in the browser.
			if _, err := s.authenticator.Authenticate(r.Context(), token); err != nil {
				writeLoginError(w, r, authStatus(err), next, err.Error())
				return
			}

			http.SetCookie(w, s.authCookie(token, int(s.authCookieMaxAge.Seconds())))
			http.Redirect(w, r, next, http.StatusFound)
		default:
			w.Header().Set("Allow", "GET, POST")
			writeAuthError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})

	http.HandleFunc(LogoutPath, func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, s.authCookie("", -1))
		http.Redirect(w, r, LoginPath, http.StatusFound)
	})
}

// writeLoginError keeps browser users on the form while API callers retain JSON errors.
func writeLoginError(w http.ResponseWriter, r *http.Request, status int, next, message string) {
	w.Header().Set("Cache-Control", "no-store")
	if wantsHTML(r) {
		writeLoginPage(w, status, next, message)
		return
	}
	writeAuthError(w, status, message)
}

func writeLoginPage(w http.ResponseWriter, status int, next, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := loginPageTemplate.Execute(w, struct{ Next, Message string }{Next: next, Message: message}); err != nil {
		logrus.Errorf("Failed to write login page: %v", err)
	}
}

// loginPageTemplate escapes the retry message and next target automatically.
// The token is submitted only in the POST body and is never echoed into the form.
var loginPageTemplate = template.Must(template.New("login").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Ray History Server sign in</title>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; background: #f5f6f8; margin: 0;
         display: flex; align-items: center; justify-content: center; height: 100vh; }
  .card { background: #fff; padding: 32px 36px; border-radius: 8px; width: 420px;
          box-shadow: 0 1px 4px rgba(0,0,0,.12); }
  h1 { font-size: 18px; margin: 0 0 6px; }
  p { color: #5b6472; font-size: 13px; line-height: 1.5; margin: 0 0 18px; }
  code { background: #f0f1f3; padding: 1px 5px; border-radius: 3px; }
  textarea { width: 100%; box-sizing: border-box; height: 96px; font-family: ui-monospace, monospace;
             font-size: 12px; padding: 8px; border: 1px solid #d0d4da; border-radius: 4px; resize: vertical; }
  button { margin-top: 14px; width: 100%; padding: 9px; font-size: 14px; color: #fff;
           background: #1668dc; border: 0; border-radius: 4px; cursor: pointer; }
</style>
</head>
<body>
  <div class="card">
    <h1>Ray History Server</h1>
    <p>Paste a Kubernetes token, for example
       <code>kubectl create token &lt;serviceaccount&gt;</code>.</p>
    {{if .Message}}<p role="alert">{{.Message}}</p>{{end}}
    <form method="POST" action="/login">
      <input type="hidden" name="next" value="{{.Next}}">
      <textarea name="token" placeholder="eyJhbGciOi..." autofocus required></textarea>
      <button type="submit">Sign in</button>
    </form>
  </div>
</body>
</html>`))

// htmlAttrEscape escapes a value interpolated into an HTML attribute.
func htmlAttrEscape(s string) string {
	return html.EscapeString(s)
}
