from collections.abc import Callable

from langchain_core.documents import Document as LangchainDocument


def create_document_reducer(max_documents: int) -> Callable:
    """Create a reducer function that limits the number of documents in the graph state.

    Args:
        max_documents: Maximum number of documents to keep in the graph state.

    Returns:
        A reducer function that can be used with GraphState to limit documents.
    """

    def reduce_documetns(new: list[LangchainDocument], existing: list[LangchainDocument]) -> list[LangchainDocument]:
        combined = existing + new
        if len(combined) <= max_documents:
            return combined
        else:
            return combined[-max_documents:]

    return reduce_documetns
