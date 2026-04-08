package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	userintelligence "github.com/open-rails/user-intelligence"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	app, err := userintelligence.New(
		pool,
		userintelligence.OpenAIConfig{
			APIKey: os.Getenv("OPENAI_API_KEY"),
			Model:  os.Getenv("OPENAI_MODEL"),
		},
		userintelligence.TwilioConfig{
			AccountSID:  os.Getenv("TWILIO_ACCOUNT_SID"),
			AuthToken:   os.Getenv("TWILIO_AUTH_TOKEN"),
			SMSFrom:     os.Getenv("TWILIO_SMS_FROM"),
			EmailFrom:   os.Getenv("TWILIO_EMAIL_FROM"),
			EmailAPIKey: os.Getenv("TWILIO_EMAIL_API_KEY"),
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	for _, rt := range app.Routes("/api/v1/messages") {
		mux.Handle(rt.Method+" "+rt.Path, rt.Handler)
	}

	// Testing-only debug route for the example host; not part of library API surface.
	mux.HandleFunc("GET /debug/messages/latest", func(w http.ResponseWriter, r *http.Request) {
		type row struct {
			ID             string    `json:"id"`
			ConversationID string    `json:"conversation_id"`
			SourceChannel  string    `json:"source_channel"`
			Direction      string    `json:"direction"`
			CreatedAt      time.Time `json:"created_at"`
		}

		rows, err := pool.Query(r.Context(), `
			SELECT id::text, conversation_id::text, source_channel, direction, created_at
			FROM user_intelligence.messages
			ORDER BY created_at DESC
			LIMIT 20
		`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		out := make([]row, 0, 20)
		for rows.Next() {
			var v row
			if err := rows.Scan(&v.ID, &v.ConversationID, &v.SourceChannel, &v.Direction, &v.CreatedAt); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out = append(out, v)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
