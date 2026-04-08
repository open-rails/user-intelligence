package ingest

import "testing"

func TestClassifySecurity(t *testing.T) {
	cases := []struct {
		channel SourceChannel
		want    SecurityClass
	}{
		{SourceChannelWeb, SecurityClassSecure},
		{SourceChannelDiscord, SecurityClassSecure},
		{SourceChannelTwilioSMS, SecurityClassInsecure},
		{SourceChannelTwilioMail, SecurityClassInsecure},
		{SourceChannel("unknown"), SecurityClassInsecure},
	}
	for _, tc := range cases {
		if got := ClassifySecurity(tc.channel); got != tc.want {
			t.Fatalf("channel=%s got=%s want=%s", tc.channel, got, tc.want)
		}
	}
}
