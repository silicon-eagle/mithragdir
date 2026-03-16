from typing import TypedDict


class GraphState(TypedDict):
    question: str
    generation: str
    documents: list
    retrieval_query: str
    relevant_found: bool


# define graph nodes and edges
