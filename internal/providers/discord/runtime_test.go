package discord

import (
	"testing"

	"github.com/open-rails/user-intelligence/internal/db"
)

func TestBuildSendPlan_ThreadReply(t *testing.T) {
	target := db.DiscordReplyTarget{
		GuildID:   "g1",
		ChannelID: "c1",
		ThreadID:  "t1",
		MessageID: "m1",
	}
	channelID, ref := buildSendPlan(target)
	if channelID != "t1" {
		t.Fatalf("expected thread id destination, got %s", channelID)
	}
	if ref == nil || ref.MessageID != "m1" || ref.ChannelID != "t1" {
		t.Fatalf("expected reply reference using thread channel")
	}
}

func TestBuildSendPlan_ChannelWithoutReply(t *testing.T) {
	target := db.DiscordReplyTarget{
		ChannelID: "c1",
	}
	channelID, ref := buildSendPlan(target)
	if channelID != "c1" {
		t.Fatalf("expected channel destination")
	}
	if ref != nil {
		t.Fatalf("expected nil reference when no message id")
	}
}
