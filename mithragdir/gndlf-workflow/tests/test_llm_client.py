from workflow.llm_client import get_llm_client


def test_llm_client_integration() -> None:
    """
    Integration test for the LLM client using a local Ollama instance.
    This test does not mock anything and expects a running Ollama instance
    with the configured model (default: gemma3:1b).
    """
    llm = get_llm_client()

    response = llm.invoke("Hi. Please reply with only the word 'Hello'.")

    assert response.content is not None
    assert isinstance(response.content, str)
    assert len(response.content) > 0
