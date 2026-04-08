package ingest

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type SignatureVerifier interface {
	Verify(r *http.Request) error
}

type Normalizer interface {
	Normalize(ctx context.Context, r *http.Request) (InboundMessage, error)
}

type UserResolver interface {
	UserFromContext(ctx context.Context) (*uuid.UUID, error)
	ResolveBySourceIdentity(ctx context.Context, channel SourceChannel, sourceIdentity string) (*uuid.UUID, error)
}
