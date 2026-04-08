package verify

import "net/http"

type Verifier interface {
	Verify(r *http.Request) error
}

type NoopVerifier struct{}

func (NoopVerifier) Verify(_ *http.Request) error { return nil }
