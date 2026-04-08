package discord

import (
	"encoding/json"
	"testing"

	"github.com/bwmarrin/discordgo"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

func TestNormalizeMessageCreate_DirectMessageAccepted(t *testing.T) {
	msg := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "m1",
			ChannelID: "c_dm",
			Content:   "hello",
			Author:    &discordgo.User{ID: "u1"},
		},
	}

	in, ok := NormalizeMessageCreate(msg, InboundFilterConfig{BotUserID: "bot1", MentionTarget: "doujins"})
	if !ok {
		t.Fatalf("expected dm message accepted")
	}
	if in.SourceChannel != ingest.SourceChannelDiscord {
		t.Fatalf("unexpected source channel: %s", in.SourceChannel)
	}
	if in.SecurityClass != ingest.SecurityClassSecure {
		t.Fatalf("expected secure class")
	}
	if in.ConversationKey != "discord:dm:u1" {
		t.Fatalf("unexpected conversation key: %s", in.ConversationKey)
	}

	var ctx map[string]any
	if err := json.Unmarshal(in.SourceContext, &ctx); err != nil {
		t.Fatalf("unmarshal source_context: %v", err)
	}
	if isDM, _ := ctx["is_dm"].(bool); !isDM {
		t.Fatalf("expected is_dm=true")
	}
}

func TestNormalizeMessageCreate_MentionAccepted(t *testing.T) {
	msg := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "m2",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "@doujins ping",
			Author:    &discordgo.User{ID: "u1"},
		},
	}

	in, ok := NormalizeMessageCreate(msg, InboundFilterConfig{BotUserID: "bot1", MentionTarget: "doujins"})
	if !ok {
		t.Fatalf("expected mention accepted")
	}
	if in.ConversationKey != "discord:guild:g1:channel:c1" {
		t.Fatalf("unexpected conversation key: %s", in.ConversationKey)
	}
}

func TestNormalizeMessageCreate_UnrelatedRejected(t *testing.T) {
	msg := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "m3",
			GuildID:   "g1",
			ChannelID: "c1",
			Content:   "hello everyone",
			Author:    &discordgo.User{ID: "u1"},
		},
	}

	_, ok := NormalizeMessageCreate(msg, InboundFilterConfig{BotUserID: "bot1", MentionTarget: "doujins"})
	if ok {
		t.Fatalf("expected unrelated guild message rejected")
	}
}

func TestNormalizeMessageCreate_Allowlist(t *testing.T) {
	msg := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "m4",
			GuildID:   "g2",
			ChannelID: "c1",
			Content:   "@doujins ping",
			Author:    &discordgo.User{ID: "u1"},
		},
	}

	_, ok := NormalizeMessageCreate(msg, InboundFilterConfig{
		BotUserID:     "bot1",
		MentionTarget: "doujins",
		AllowedGuildIDs: map[string]struct{}{
			"g1": {},
		},
	})
	if ok {
		t.Fatalf("expected guild outside allowlist rejected")
	}
}
