from __future__ import annotations

from fastapi import APIRouter, HTTPException

from cirdan.domain.models import ChatRequest

router = APIRouter(prefix='/api', tags=['chat'])


@router.post('/chat')
async def chat_stream(_: ChatRequest) -> None:
    raise HTTPException(status_code=501, detail='Chat streaming endpoint is planned for Epic 4 and not implemented yet.')
