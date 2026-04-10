import os

from langchain_ollama import ChatOllama


def get_llm_client() -> ChatOllama:
    model = os.getenv('OLLAMA_MODEL', 'gemma4:e2b')
    base_url = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')

    return ChatOllama(
        model=model,
        base_url=base_url,
        temperature=0.0,
    )
