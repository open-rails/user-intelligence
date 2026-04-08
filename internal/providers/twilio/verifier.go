package twilio

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

const signatureHeader = "X-Twilio-Signature"

type SignatureVerifier struct {
	AuthToken string
}

func (v SignatureVerifier) Verify(r *http.Request) error {
	if strings.TrimSpace(v.AuthToken) == "" {
		return errors.New("twilio auth token is required for signature verification")
	}
	sig := strings.TrimSpace(r.Header.Get(signatureHeader))
	if sig == "" {
		return errors.New("missing X-Twilio-Signature")
	}

	if err := r.ParseForm(); err != nil {
		return fmt.Errorf("parse form for signature: %w", err)
	}

	base := buildPublicURL(r)
	expected := computeSignature(v.AuthToken, base, r.Form)
	if !hmac.Equal([]byte(sig), []byte(expected)) {
		return errors.New("invalid twilio signature")
	}
	return nil
}

func buildPublicURL(r *http.Request) string {
	scheme := "https"
	if r.TLS == nil {
		scheme = "http"
	}
	if proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		scheme = proto
	}
	base := scheme + "://" + r.Host + r.URL.Path
	if r.URL.RawQuery != "" {
		base += "?" + r.URL.RawQuery
	}
	return base
}

func computeSignature(authToken, baseURL string, form url.Values) string {
	keys := make([]string, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	payload := baseURL
	for _, k := range keys {
		vals := append([]string(nil), form[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			payload += k + v
		}
	}

	h := hmac.New(sha1.New, []byte(authToken))
	_, _ = h.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
