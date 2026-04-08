package userintelligence

type OpenAIConfig struct {
	APIKey string
	Model  string
}

type TwilioConfig struct {
	AccountSID  string
	AuthToken   string
	SMSFrom     string
	EmailFrom   string
	EmailAPIKey string
}

type DiscordConfig struct {
	BotToken string
}
