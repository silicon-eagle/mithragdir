from typing import Any

from loguru import logger

from ..graph.state import GraphState


class Node:
    def __init__(self, name: str) -> None:
        self.name: str = name
        self.prompt_path: str | None = None
        self.model: None = None
        logger.debug(f"Initialized node '{self.name}'")

    def __call__(self, state: GraphState) -> dict[str, Any]:
        """Generic  method to

        Args:
            state (GraphState): State of the graph to be used

        Returns:
            dict[str, Any]: Updated state values to be merged into the graph state. The keys should correspond to GraphState fields.
        """
        assert state
        return {}
