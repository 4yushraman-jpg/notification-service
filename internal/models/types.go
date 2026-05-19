package models

type CampaignRequest struct {
	CampaignID string `json:"campaign_id"`
	TemplateID string `json:"template_id"`
	AudienceID string `json:"audience_id"`
}

type EmailJob struct {
	JobID        string `json:"job_id"`
	EmailAddress string `json:"email_address"`
	TemplateID   string `json:"template_id"`
}
