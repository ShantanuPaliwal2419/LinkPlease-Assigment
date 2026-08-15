import uuid

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.rule import Rule
from app.schemas import CreateRuleRequest, RuleResponse


router = APIRouter(prefix="/rules", tags=["rules"])


@router.post(
    "",
    response_model=RuleResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_rule(
    request: CreateRuleRequest,
    db: Session = Depends(get_db),
):
    rule_id = str(uuid.uuid4())

    rule = Rule(
        id=rule_id,
        keyword=request.keyword,
        dm_message=request.dm_message,
    )

    db.add(rule)
    db.commit()
    db.refresh(rule)

    return RuleResponse(
        rule_id=rule.id,
        keyword=rule.keyword,
        dm_message=rule.dm_message,
    )