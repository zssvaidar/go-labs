package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRoutes(t *testing.T) {
	e := newServer()
	tests := []struct {
		path, want string
	}{
		{"/health", `{"status":"ok"}`},
		{"/hello", `{"message":"Hello, world!"}`},
		{"/hello?name=Aidar", `{"message":"Hello, Aidar!"}`},
	}
	for _, tt := range tests {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s: status %d, want 200", tt.path, rec.Code)
		}
		if got := strings.TrimSpace(rec.Body.String()); got != tt.want {
			t.Errorf("GET %s: body %s, want %s", tt.path, got, tt.want)
		}
	}
}
