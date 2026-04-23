import inspect
from abc import ABC, abstractmethod
from collections.abc import Sequence
from functools import wraps
from pathlib import Path
from typing import Any, Self

from langchain_ollama import ChatOllama
from loguru import logger

from ..graph.state import GraphState
from ..llm_client import get_llm_client


class Node(ABC):
    def __init__(self, name: str) -> None:
        self.name: str = name
        self.model: ChatOllama = get_llm_client()
        self.logger = logger.bind(node=self.name)

    def __init_subclass__(cls) -> None:
        """Wrap subclass __call__ to add logging before execution."""
        super().__init_subclass__()
        original_call = cls.__call__

        @wraps(original_call)
        async def logged_call(self: Self, state: GraphState) -> dict[str, Any]:
            self.logger.info(f'Starting call for node {self.name}')
            return await original_call(self, state)

        cls.__call__ = logged_call

    @abstractmethod
    async def __call__(self, state: GraphState) -> dict[str, Any]:
        """Generic method to be overwritten by subclasses to implement node-specific logic.

        Args:
            state (GraphState): State of the graph to be used

        Returns:
            dict[str, Any]: Updated state values to be merged into the graph state. The keys should correspond to GraphState fields.
        """
        pass

    @property
    def prompt_path(self) -> Path:
        """Resolve prompt file path from node module location and node name."""
        node_file = Path(inspect.getfile(self.__class__)).resolve()
        return node_file.parent.parent / 'prompts' / f'{self.name}.yml'

    def load_prompt(self) -> str:
        """Utility to read the prompt file."""
        return self.prompt_path.read_text(encoding='utf-8')

    def validate_state(self, state: GraphState, required_keys: Sequence[str]) -> None:
        """Utility to ensure required variables exist in the state before execution."""
        for key in required_keys:
            if getattr(state, key, None) is None:
                self.logger.error(f"State validation failed: missing '{key}'")
                raise KeyError(f"Node '{self.name}' requires '{key}' in state.")
