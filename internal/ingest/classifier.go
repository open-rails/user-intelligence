package ingest

func ClassifySecurity(channel SourceChannel) SecurityClass {
	switch channel {
	case SourceChannelWeb, SourceChannelDiscord:
		return SecurityClassSecure
	case SourceChannelTwilioSMS, SourceChannelTwilioMail:
		return SecurityClassInsecure
	default:
		return SecurityClassInsecure
	}
}
