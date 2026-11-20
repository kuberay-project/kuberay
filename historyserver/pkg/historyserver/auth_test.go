package historyserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/emicklei/go-restful/v3"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	authv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// newTestAuthenticator wires an Authenticator onto a fake clientset whose
// TokenReview reaction is supplied by the caller.
func newTestAuthenticator(t *testing.T, reaction k8stesting.ReactionFunc) *Authenticator {
	t.Helper()
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "tokenreviews", reaction)
	return &Authenticator{
		client: client,
		cache:  lru.NewLRU[string, string](authCacheSize, nil, time.Minute),
	}
}

func authenticatedReaction() k8stesting.ReactionFunc {
	return func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authv1.TokenReview{
			Status: authv1.TokenReviewStatus{
				Authenticated: true,
				User:          authv1.UserInfo{Username: "alice"},
			},
		}, nil
	}
}

func TestAuthenticateRejectsInvalidTokenWith401(t *testing.T) {
	a := newTestAuthenticator(t, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authv1.TokenReview{
			Status: authv1.TokenReviewStatus{Authenticated: false, Error: "token expired"},
		}, nil
	})

	_, err := a.Authenticate(context.Background(), "bad-token")
	if err == nil {
		t.Fatal("expected an error for a rejected token")
	}
	if got := authStatus(err); got != http.StatusUnauthorized {
		t.Fatalf("invalid token must map to 401, got %d", got)
	}
}

func TestAuthenticateFailsClosedWith503WhenBackendIsDown(t *testing.T) {
	// A TokenReview outage must never be mistaken for a valid credential.
	a := newTestAuthenticator(t, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unreachable")
	})

	_, err := a.Authenticate(context.Background(), "some-token")
	if err == nil {
		t.Fatal("expected an error when TokenReview fails")
	}
	if got := authStatus(err); got != http.StatusServiceUnavailable {
		t.Fatalf("backend failure must map to 503, got %d", got)
	}
}

func TestAuthenticateCachesSuccessfulReview(t *testing.T) {
	calls := 0
	a := newTestAuthenticator(t, func(action k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		return authenticatedReaction()(action)
	})

	for range 3 {
		user, err := a.Authenticate(context.Background(), "good-token")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if user != "alice" {
			t.Fatalf("expected alice, got %q", user)
		}
	}
	if calls != 1 {
		t.Fatalf("expected the review to be cached after the first call, got %d calls", calls)
	}
}

func TestExtractTokenPrefersHeaderAndFailsHardOnBadScheme(t *testing.T) {
	cookie := &http.Cookie{Name: AuthCookieName, Value: "cookie-token"}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/", nil)
	req.Header.Set("Authorization", "Bearer header-token")
	req.AddCookie(cookie)
	got, err := extractToken(req)
	if err != nil || got != "header-token" {
		t.Fatalf("header must win over cookie, got %q err=%v", got, err)
	}

	// A malformed header must not silently fall back to the cookie: that would
	// authenticate a misconfigured client as the browser's user.
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	req.AddCookie(cookie)
	if _, err = extractToken(req); err == nil {
		t.Fatal("a non-Bearer Authorization header must be a hard failure")
	}

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/", nil)
	req.AddCookie(cookie)
	got, err = extractToken(req)
	if err != nil || got != "cookie-token" {
		t.Fatalf("cookie fallback failed: %q err=%v", got, err)
	}
}

func TestStripAuthCookieRemovesOnlyTheAuthCookie(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"removes auth cookie", "a=1; " + AuthCookieName + "=secret; b=2", "a=1; b=2"},
		{"only auth cookie", AuthCookieName + "=secret", ""},
		{"no auth cookie", "cluster_name=c1; session_name=live", "cluster_name=c1; session_name=live"},
		{"keeps similarly named cookie", AuthCookieName + "_other=v", AuthCookieName + "_other=v"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripAuthCookie(tc.in); got != tc.want {
				t.Fatalf("stripAuthCookie(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSafeNextRejectsOffOriginRedirects(t *testing.T) {
	tests := map[string]string{
		"/#/overview":           "/#/overview",
		"/enter_cluster/ns/c/s": "/enter_cluster/ns/c/s",
		"https://evil.test/x":   SelectClusterPath,
		"//evil.test/x":         SelectClusterPath,
		"javascript:alert(1)":   SelectClusterPath,
		"":                      SelectClusterPath,
		// Browsers read a backslash as a path separator, so this is a
		// protocol-relative URL to evil.test even though url.Parse leaves Host
		// empty and the value looks like a same-origin path.
		`/\evil.test/x`:     SelectClusterPath,
		`/\evil.test`:       SelectClusterPath,
		"/select_cluster":   SelectClusterPath,
		"/clusters?a=b&c=d": "/clusters?a=b&c=d",
	}
	for in, want := range tests {
		if got := safeNext(in); got != want {
			t.Fatalf("safeNext(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSafeNextDefaultIsServed pins the reason the default is the cluster
// selector: "/" has no registered handler, so the old default sent the
// paste-a-token flow and every re-login after /logout to a 404.
func TestSafeNextDefaultIsServed(t *testing.T) {
	if got := safeNext(""); got != SelectClusterPath {
		t.Fatalf("safeNext(empty) = %q, want %q", got, SelectClusterPath)
	}
	if SelectClusterPath == "/" {
		t.Fatal("SelectClusterPath must not be \"/\"; the History Server registers no handler for it")
	}
}

func TestAuthFilterRedirectsBrowserButReturnsJSONToXHR(t *testing.T) {
	s := &ServerHandler{authenticator: newTestAuthenticator(t, authenticatedReaction())}

	// Browser navigation without a credential: keep the single-link flow working.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/enter_cluster/ns/raycluster/c/s", nil)
	req.Header.Set("Accept", "text/html")
	s.AuthFilter(restfulRequest(req), restfulResponse(rec), emptyChain())
	if rec.Code != http.StatusFound {
		t.Fatalf("browser navigation should redirect, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc == "" || loc[:len(LoginPath)] != LoginPath {
		t.Fatalf("expected redirect to %s, got %q", LoginPath, loc)
	}

	// XHR without a credential: machine-readable 401, no HTML.
	rec = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/", nil)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	s.AuthFilter(restfulRequest(req), restfulResponse(rec), emptyChain())
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("XHR should get 401, got %d", rec.Code)
	}
}

func TestSelectClusterAuthentication(t *testing.T) {
	t.Run("unauthenticated request redirects to login", func(t *testing.T) {
		s := &ServerHandler{authenticator: newTestAuthenticator(t, authenticatedReaction())}
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/select_cluster?from=test", nil)
		selectClusterHandler(s).ServeHTTP(rec, req)

		if rec.Code != http.StatusFound {
			t.Fatalf("unauthenticated selector should redirect, got %d", rec.Code)
		}
		if got, want := rec.Header().Get("Location"), "/login?next=%2Fselect_cluster%3Ffrom%3Dtest"; got != want {
			t.Fatalf("redirect location = %q, want %q", got, want)
		}
	})

	t.Run("authenticated request renders selector", func(t *testing.T) {
		s := &ServerHandler{authenticator: newTestAuthenticator(t, authenticatedReaction())}
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/select_cluster", nil)
		req.AddCookie(&http.Cookie{Name: AuthCookieName, Value: "good-token"})
		selectClusterHandler(s).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("authenticated selector should render, got %d", rec.Code)
		}
	})

	t.Run("TokenReview failure returns service unavailable", func(t *testing.T) {
		s := &ServerHandler{authenticator: newTestAuthenticator(t, func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("apiserver unavailable")
		})}
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/select_cluster", nil)
		req.AddCookie(&http.Cookie{Name: AuthCookieName, Value: "token"})
		selectClusterHandler(s).ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("TokenReview failure should return 503, got %d", rec.Code)
		}
	})

	t.Run("auth disabled renders selector", func(t *testing.T) {
		rec := httptest.NewRecorder()
		selectClusterHandler(&ServerHandler{}).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/select_cluster", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("selector should render when auth is disabled, got %d", rec.Code)
		}
	})
}

func TestAuthFilterPassesThroughWhenAuthDisabled(t *testing.T) {
	s := &ServerHandler{} // authenticator nil => --enable-auth off
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/", nil)

	called := false
	chain := chainCalling(&called)
	s.AuthFilter(restfulRequest(req), restfulResponse(rec), chain)

	if !called {
		t.Fatal("with auth disabled the request must reach the handler chain")
	}
}

// --- go-restful test helpers -------------------------------------------------

func restfulRequest(r *http.Request) *restful.Request {
	return restful.NewRequest(r)
}

func restfulResponse(w http.ResponseWriter) *restful.Response {
	return restful.NewResponse(w)
}

// emptyChain fails the test if the filter lets an unauthenticated request pass.
func emptyChain() *restful.FilterChain {
	return &restful.FilterChain{
		Target: func(_ *restful.Request, _ *restful.Response) {
			panic("handler must not be reached for an unauthenticated request")
		},
	}
}

// chainCalling records whether the filter forwarded the request.
func chainCalling(called *bool) *restful.FilterChain {
	return &restful.FilterChain{
		Target: func(_ *restful.Request, _ *restful.Response) { *called = true },
	}
}

func TestLoginFailureAllowsBrowserRetry(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		accept      string
		xhr         bool
		backendDown bool
		status      int
		wantHTML    bool
	}{
		{name: "expired token", body: "token=expired-secret&next=%2Fclusters%3Fa%3Db%26c%3Dd", accept: "text/html", status: http.StatusUnauthorized, wantHTML: true},
		{name: "missing token", body: "next=%2Fclusters", accept: "text/html", status: http.StatusUnauthorized, wantHTML: true},
		{name: "backend unavailable", body: "token=expired-secret", accept: "text/html", backendDown: true, status: http.StatusServiceUnavailable, wantHTML: true},
		{name: "oversized form", body: "token=" + strings.Repeat("x", 64<<10), accept: "text/html", status: http.StatusBadRequest, wantHTML: true},
		{name: "malformed form", body: "token=%zz", accept: "text/html", status: http.StatusBadRequest, wantHTML: true},
		{name: "unsafe next", body: "token=expired-secret&next=https%3A%2F%2Fevil.test", accept: "text/html", status: http.StatusUnauthorized, wantHTML: true},
		{name: "JSON client", body: "token=expired-secret", accept: "application/json", status: http.StatusUnauthorized},
		{name: "XHR client", body: "token=expired-secret", accept: "text/html", xhr: true, status: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousMux := http.DefaultServeMux
			http.DefaultServeMux = http.NewServeMux()
			t.Cleanup(func() { http.DefaultServeMux = previousMux })
			s := &ServerHandler{authCookieMaxAge: DefaultAuthCookieMaxAge, authenticator: newTestAuthenticator(t, func(action k8stesting.Action) (bool, runtime.Object, error) {
				if tc.backendDown {
					return true, nil, errors.New("unavailable")
				}
				review := action.(k8stesting.CreateAction).GetObject().(*authv1.TokenReview)
				if review.Spec.Token == "good-token" {
					return authenticatedReaction()(action)
				}
				return true, &authv1.TokenReview{Status: authv1.TokenReviewStatus{Error: "expired <script>alert(1)</script>"}}, nil
			})}
			routerAuth(s)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, LoginPath, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Accept", tc.accept)
			if tc.xhr {
				req.Header.Set("X-Requested-With", "XMLHttpRequest")
			}
			rec := httptest.NewRecorder()
			http.DefaultServeMux.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if len(rec.Result().Cookies()) != 0 {
				t.Fatal("failed login set cookies")
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Error("login failure must not be cached")
			}
			if strings.Contains(rec.Body.String(), "expired-secret") {
				t.Fatal("response echoed submitted token")
			}
			if !tc.wantHTML {
				var body struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if body.Code != tc.status || body.Message == "" {
					t.Fatalf("unexpected JSON error: %+v", body)
				}
				return
			}
			if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
				t.Fatal("expected HTML response")
			}
			body := rec.Body.String()
			for _, part := range []string{`role="alert"`, `<form method="POST" action="/login">`, `<textarea name="token"`} {
				if !strings.Contains(body, part) {
					t.Errorf("missing %s", part)
				}
			}
			if strings.Contains(body, "<script>") {
				t.Fatal("error message was not escaped")
			}
			values, _ := url.ParseQuery(tc.body)
			next := safeNext(values.Get("next"))
			if !strings.Contains(body, `name="next" value="`+htmlAttrEscape(next)+`"`) {
				t.Error("safe next target not preserved")
			}
			if tc.backendDown {
				return
			}
			// A fresh submission from the retry form can complete authentication.
			form := url.Values{"token": {"good-token"}, "next": {next}}
			retry := httptest.NewRequestWithContext(t.Context(), http.MethodPost, LoginPath, strings.NewReader(form.Encode()))
			retry.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			retryRec := httptest.NewRecorder()
			http.DefaultServeMux.ServeHTTP(retryRec, retry)
			if retryRec.Code != http.StatusFound || retryRec.Header().Get("Location") != next || len(retryRec.Result().Cookies()) != 1 {
				t.Fatalf("retry did not complete login: status=%d headers=%v", retryRec.Code, retryRec.Header())
			}
		})
	}
}
