# user-intelligence

`user-intelligence` is an embeddable Go library that lets a host app receive user messages from web, Twilio (SMS/email), and Discord, store them in Postgres conversation/message tables, and optionally run an OpenAI Responses-based agent loop to produce replies and tool actions.

## How to embed it

1. Create the app with a `*pgxpool.Pool`, optional OpenAI key/model, and Twilio credentials.
2. Mount library routes where you want (for example `/api/v1/messages`):
   - `POST /messages` (web)
   - `POST /messages/sms` (Twilio SMS webhook)
   - `POST /messages/email` (Twilio email webhook)
3. Start Discord runtime if needed.
4. Apply this library’s embedded migrations before serving traffic.

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"

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
            Model:  os.Getenv("OPENAI_MODEL"), // optional; default is gpt-5.4-mini
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

    if token := os.Getenv("DISCORD_BOT_TOKEN"); token != "" {
        if err := app.StartDiscord(ctx, userintelligence.DiscordConfig{BotToken: token}); err != nil {
            log.Fatal(err)
        }
        defer app.StopDiscord(context.Background())
    }

    log.Fatal(http.ListenAndServe(":8080", mux))
}
```
