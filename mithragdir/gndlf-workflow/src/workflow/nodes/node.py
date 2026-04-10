from typing import Any

from langchain_ollama import ChatOllama
from loguru import logger

from ..graph.state import GraphState
from ..llm_client import get_llm_client


class Node:
    def __init__(self, name: str) -> None:
        self.name: str = name
        self.prompt_path: str | None = None
        self.model: ChatOllama = get_llm_client()
        logger.debug(f"Initialized node '{self.name}'")

    def __call__(self, state: GraphState) -> dict[str, Any]:
        """Generic  method to be overwritten by subclasses to implement node-specific logic.

        Args:
            state (GraphState): State of the graph to be used

        Returns:
            dict[str, Any]: Updated state values to be merged into the graph state. The keys should correspond to GraphState fields.
        """
        assert state
        return {}
