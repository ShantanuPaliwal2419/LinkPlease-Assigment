import uuid

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.rule import Rule
from app.schemas import CreateRuleRequest, RuleResponse


router = APIRouter(prefix="/rules", tags=["rules"])


@router.post(
    "",
    response_model=RuleResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_rule(
    request: CreateRuleRequest,
    db: AsyncSession = Depends(get_db),
):
    rule_id = str(uuid.uuid4())

    rule = Rule(
        id=rule_id,
        keyword=request.keyword,
        dm_message=request.dm_message,
    )

    db.add(rule)
    await db.commit()
    await db.refresh(rule)

    return RuleResponse(
        rule_id=rule.id,
        keyword=rule.keyword,
        dm_message=rule.dm_message,
    )