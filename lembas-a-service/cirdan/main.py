from __future__ import annotations

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from cirdan.api.chat import router as chat_router
from cirdan.api.health import router as health_router
from cirdan.config import Settings, get_settings
from cirdan.domain.exceptions import DependencyOfflineException, ValidationException


def _requires_llm_api_key(settings: Settings) -> bool:
    return settings.llm_provider.lower() in {'openai', 'gemini', 'anthropic'}


def _validate_startup_config(settings: Settings) -> None:
    if _requires_llm_api_key(settings=settings) and not settings.llm_api_key:
        raise RuntimeError(f'Missing llm_api_key for provider {settings.llm_provider!r}.')


def create_app() -> FastAPI:
    settings = get_settings()
    _validate_startup_config(settings=settings)

    app = FastAPI(title='cirdan-api', version='0.1.0')
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
    )

    @app.exception_handler(ValidationException)
    async def handle_validation_exception(_: Request, exc: ValidationException) -> JSONResponse:
        return JSONResponse(status_code=400, content={'error': {'code': exc.code, 'message': exc.message}})

    @app.exception_handler(DependencyOfflineException)
    async def handle_dependency_exception(_: Request, exc: DependencyOfflineException) -> JSONResponse:
        return JSONResponse(status_code=503, content={'error': {'code': exc.code, 'message': exc.message}})

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(_: Request, exc: Exception) -> JSONResponse:
        logger.exception('Unhandled exception in cirdan-api', exc_info=exc)
        return JSONResponse(status_code=500, content={'error': {'code': 'internal_error', 'message': 'Unexpected server error.'}})

    app.include_router(health_router)
    app.include_router(chat_router)

    return app


app = create_app()


def main() -> None:
    uvicorn.run('cirdan.main:app', host='0.0.0.0', port=8000, reload=False)
