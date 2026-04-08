package twilio

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestVerifierVerify(t *testing.T) {
	form := url.Values{}
	form.Set("From", "+15550001111")
	form.Set("To", "+15550002222")
	form.Set("Body", "ping")

	req := httptest.NewRequest("POST", "https://example.com/messages/sms", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_ = req.ParseForm()

	sig := computeSignature("token", "https://example.com/messages/sms", req.Form)
	req.Header.Set(signatureHeader, sig)

	v := SignatureVerifier{AuthToken: "token"}
	if err := v.Verify(req); err != nil {
		t.Fatalf("verify should succeed: %v", err)
	}
}
