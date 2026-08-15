from pydantic import BaseModel, Field


class CreateRuleRequest(BaseModel):
    keyword: str = Field(min_length=1)
    dm_message: str = Field(min_length=1)


class RuleResponse(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str


class WebhookUser(BaseModel):
    user_id: str
    username: str | None = None


class WebhookData(BaseModel):
    comment_id: str
    post_id: str | None = None
    text: str | None = None
    created_at: str | None = None
    from_: WebhookUser | None = Field(default=None, alias="from")


class WebhookEvent(BaseModel):
    event_id: str
    event_type: str
    sent_at: str
    data: WebhookData