package discord

import (
	"encoding/json"
	"strings"

	"github.com/bwmarrin/discordgo"

	"github.com/open-rails/user-intelligence/internal/ingest"
)

const defaultMentionTarget = "doujins"

type InboundFilterConfig struct {
	BotUserID       string
	MentionTarget   string
	AllowedGuildIDs map[string]struct{}
}

func NormalizeMessageCreate(m *discordgo.MessageCreate, cfg InboundFilterConfig) (ingest.InboundMessage, bool) {
	if m == nil || m.Message == nil || m.Author == nil || m.Author.Bot {
		return ingest.InboundMessage{}, false
	}

	body := strings.TrimSpace(m.Content)
	if body == "" {
		return ingest.InboundMessage{}, false
	}

	isDM := strings.TrimSpace(m.GuildID) == ""
	if !isDM && len(cfg.AllowedGuildIDs) > 0 {
		if _, ok := cfg.AllowedGuildIDs[m.GuildID]; !ok {
			return ingest.InboundMessage{}, false
		}
	}

	mentionTarget := strings.TrimSpace(cfg.MentionTarget)
	if mentionTarget == "" {
		mentionTarget = defaultMentionTarget
	}

	if !isDM && !isMentioned(m.Message, cfg.BotUserID, mentionTarget) {
		return ingest.InboundMessage{}, false
	}
	if isDM {
		mentionTarget = ""
	}

	channelID := strings.TrimSpace(m.ChannelID)
	threadID := ""
	if m.Thread != nil {
		threadID = strings.TrimSpace(m.Thread.ID)
		if p := strings.TrimSpace(m.Thread.ParentID); p != "" {
			channelID = p
		}
	}

	conversationKey := buildConversationKey(m.GuildID, channelID, threadID, m.Author.ID)
	sourceContext, _ := json.Marshal(map[string]any{
		"guild_id":       strings.TrimSpace(m.GuildID),
		"channel_id":     channelID,
		"thread_id":      threadID,
		"message_id":     strings.TrimSpace(m.ID),
		"author_id":      strings.TrimSpace(m.Author.ID),
		"mention_target": mentionTarget,
		"is_dm":          isDM,
	})

	providerMessageID := strings.TrimSpace(m.ID)
	msg := ingest.InboundMessage{
		SourceChannel:   ingest.SourceChannelDiscord,
		ConversationKey: conversationKey,
		BodyText:        body,
		SourceIdentity:  strings.TrimSpace(m.Author.ID),
		SourceContext:   sourceContext,
		SecurityClass:   ingest.ClassifySecurity(ingest.SourceChannelDiscord),
	}
	if providerMessageID != "" {
		msg.ProviderMessageID = &providerMessageID
	}
	return msg, true
}

func buildConversationKey(guildID, channelID, threadID, authorID string) string {
	guildID = strings.TrimSpace(guildID)
	channelID = strings.TrimSpace(channelID)
	threadID = strings.TrimSpace(threadID)
	authorID = strings.TrimSpace(authorID)

	if guildID == "" {
		return "discord:dm:" + authorID
	}
	if threadID != "" {
		return "discord:guild:" + guildID + ":thread:" + threadID
	}
	return "discord:guild:" + guildID + ":channel:" + channelID
}

func isMentioned(m *discordgo.Message, botUserID, mentionTarget string) bool {
	botUserID = strings.TrimSpace(botUserID)
	mentionTarget = strings.TrimSpace(mentionTarget)
	for _, u := range m.Mentions {
		if u == nil {
			continue
		}
		if botUserID != "" && strings.TrimSpace(u.ID) == botUserID {
			return true
		}
	}
	content := strings.ToLower(m.Content)
	if mentionTarget != "" && strings.Contains(content, strings.ToLower("@"+mentionTarget)) {
		return true
	}
	if botUserID != "" && strings.Contains(content, "<@"+botUserID+">") {
		return true
	}
	if botUserID != "" && strings.Contains(content, "<@!"+botUserID+">") {
		return true
	}
	return false
}
