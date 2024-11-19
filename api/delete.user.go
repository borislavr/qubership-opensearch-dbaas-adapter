package api

import (
	"context"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"net/http"
	"strconv"
	"strings"
)

func newDeleteUserFunc(t opensearchapi.Transport) DeleteUser {
	return func(username string, o ...func(request *DeleteUserRequest)) (*opensearchapi.Response, error) {
		var r = DeleteUserRequest{Username: username}
		for _, f := range o {
			f(&r)
		}
		return r.Do(r.ctx, t)
	}
}

// ----- API Definition -------------------------------------------------------

// DeleteUser deletes a user
type DeleteUser func(username string, o ...func(request *DeleteUserRequest)) (*opensearchapi.Response, error)

// DeleteUserRequest configures the User API request.
type DeleteUserRequest struct {
	Username string

	WaitForCompletion *bool

	Pretty     bool
	Human      bool
	ErrorTrace bool
	FilterPath []string

	Header http.Header

	ctx context.Context
}

// Do function executes the request and returns response or error.
func (r DeleteUserRequest) Do(ctx context.Context, transport opensearchapi.Transport) (*opensearchapi.Response, error) {
	var (
		method string
		path   strings.Builder
		params map[string]string
	)

	method = http.MethodDelete
	path.Grow(1 + len("_plugins/_security/api/internalusers") + 1 + len(r.Username))
	path.WriteString("/_plugins/_security/api/internalusers")
	path.WriteString("/")
	path.WriteString(r.Username)

	params = make(map[string]string)

	if r.WaitForCompletion != nil {
		params["wait_for_completion"] = strconv.FormatBool(*r.WaitForCompletion)
	}

	if r.Pretty {
		params["pretty"] = "true"
	}

	if r.Human {
		params["human"] = "true"
	}

	if r.ErrorTrace {
		params["error_trace"] = "true"
	}

	if len(r.FilterPath) > 0 {
		params["filter_path"] = strings.Join(r.FilterPath, ",")
	}

	req, err := http.NewRequest(method, path.String(), nil)
	if err != nil {
		return nil, err
	}

	if len(params) > 0 {
		q := req.URL.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	if len(r.Header) > 0 {
		if len(req.Header) == 0 {
			req.Header = r.Header
		} else {
			for k, vv := range r.Header {
				for _, v := range vv {
					req.Header.Add(k, v)
				}
			}
		}
	}

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	res, err := transport.Perform(req)
	if err != nil {
		return nil, err
	}

	response := opensearchapi.Response{
		StatusCode: res.StatusCode,
		Body:       res.Body,
		Header:     res.Header,
	}

	return &response, nil
}

// WithUsername sets the request username.
func (f DeleteUser) WithUsername(v string) func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		r.Username = v
	}
}

// WithContext sets the request context.
func (f DeleteUser) WithContext(v context.Context) func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		r.ctx = v
	}
}

// WithPretty makes the response body pretty-printed.
func (f DeleteUser) WithPretty() func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		r.Pretty = true
	}
}

// WithHuman makes statistical values human-readable.
func (f DeleteUser) WithHuman() func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		r.Human = true
	}
}

// WithErrorTrace includes the stack trace for errors in the response body.
func (f DeleteUser) WithErrorTrace() func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		r.ErrorTrace = true
	}
}

// WithFilterPath filters the properties of the response body.
func (f DeleteUser) WithFilterPath(v ...string) func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		r.FilterPath = v
	}
}

// WithHeader adds the headers to the HTTP request.
func (f DeleteUser) WithHeader(h map[string]string) func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		if r.Header == nil {
			r.Header = make(http.Header)
		}
		for k, v := range h {
			r.Header.Add(k, v)
		}
	}
}

// WithOpaqueID adds the X-Opaque-Id header to the HTTP request.
func (f DeleteUser) WithOpaqueID(s string) func(*DeleteUserRequest) {
	return func(r *DeleteUserRequest) {
		if r.Header == nil {
			r.Header = make(http.Header)
		}
		r.Header.Set("X-Opaque-Id", s)
	}
}
