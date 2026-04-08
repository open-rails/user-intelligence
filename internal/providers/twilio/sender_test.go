package twilio

import (
	"context"
	"testing"
)

func TestSenderSendEmailValidation(t *testing.T) {
	s := Sender{}
	if err := s.SendEmail(context.Background(), "to@example.com", "subject", "body"); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestSenderSendSMSValidation(t *testing.T) {
	s := Sender{}
	if _, err := s.SendSMS(context.Background(), "+1555", "hello"); err == nil {
		t.Fatalf("expected validation error")
	}
}
