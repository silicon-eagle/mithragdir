import os

from langchain_ollama import ChatOllama


def get_llm_client() -> ChatOllama:
    model = os.getenv('OLLAMA_MODEL', None)
    if model is None:
        raise ValueError('OLLAMA_MODEL environment variable must be set to specify the model for ChatOllama client.')
    base_url = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')

    return ChatOllama(
        model=model,
        base_url=base_url,
        temperature=0.0,
    )
