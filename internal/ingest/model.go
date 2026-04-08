package ingest

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type SourceChannel string

const (
	SourceChannelWeb        SourceChannel = "web"
	SourceChannelTwilioSMS  SourceChannel = "twilio_sms"
	SourceChannelTwilioMail SourceChannel = "twilio_email"
	SourceChannelDiscord    SourceChannel = "discord"
)

type Direction string

const (
	DirectionInbound  Direction = "inbound"
	DirectionOutbound Direction = "outbound"
)

type SecurityClass string

const (
	SecurityClassSecure   SecurityClass = "secure"
	SecurityClassInsecure SecurityClass = "insecure"
)

type InboundMessage struct {
	SourceChannel     SourceChannel
	ConversationKey   string
	BodyText          string
	SourceIdentity    string
	SourceContext     json.RawMessage
	SecurityClass     SecurityClass
	UserID            *uuid.UUID
	ProviderMessageID *string
}

type IngestResult struct {
	ConversationID uuid.UUID
	MessageID      uuid.UUID
	Deduped        bool
	CreatedAt      time.Time
}

type OutboundMessage struct {
	ConversationID    uuid.UUID
	SourceChannel     SourceChannel
	SourceIdentity    string
	SourceContext     json.RawMessage
	SecurityClass     SecurityClass
	BodyText          string
	ProviderMessageID *string
}

type StoredMessage struct {
	ID                uuid.UUID
	ConversationID    uuid.UUID
	SourceChannel     SourceChannel
	Direction         Direction
	SourceIdentity    string
	SecurityClass     SecurityClass
	BodyText          string
	ProviderMessageID *string
	CreatedAt         time.Time
}

type Receiver interface {
	Receive(ctx context.Context, msg InboundMessage) (IngestResult, error)
}
