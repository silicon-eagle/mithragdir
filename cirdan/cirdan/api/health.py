from __future__ import annotations

from fastapi import APIRouter, Depends

from cirdan.config import Settings, get_settings
from cirdan.data.qdrant_repository import QdrantRepository
from cirdan.data.sqlite_repository import SQLiteRepository
from cirdan.domain.exceptions import DependencyOfflineException
from cirdan.domain.models import DependencyCheck, ReadyStatus

router = APIRouter(tags=['health'])


def get_sqlite_repository(settings: Settings = Depends(get_settings)) -> SQLiteRepository:
    return SQLiteRepository(connection_string=settings.sqlite_connection_string)


def get_qdrant_repository(settings: Settings = Depends(get_settings)) -> QdrantRepository:
    return QdrantRepository(url=settings.qdrant_url, collection_name=settings.qdrant_collection)


@router.get('/health')
async def health() -> dict[str, str]:
    return {'status': 'ok'}


@router.get('/ready', response_model=ReadyStatus)
async def ready(
    sqlite_repository: SQLiteRepository = Depends(get_sqlite_repository),
    qdrant_repository: QdrantRepository = Depends(get_qdrant_repository),
) -> ReadyStatus:
    sqlite_ok = await sqlite_repository.ping()
    qdrant_ok = await qdrant_repository.ping()

    checks = [
        DependencyCheck(name='sqlite', ok=sqlite_ok),
        DependencyCheck(name='qdrant', ok=qdrant_ok),
    ]

    if sqlite_ok and qdrant_ok:
        return ReadyStatus(status='ready', dependencies=checks)

    raise DependencyOfflineException('One or more dependencies are offline.')
