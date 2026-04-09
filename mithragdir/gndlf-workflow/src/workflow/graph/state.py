from typing import Annotated, Literal

from core.models import Document
from langchain_core.messages import AnyMessage
from langgraph.graph import add_messages
from pydantic import BaseModel, Field

from workflow.utils import create_document_reducer

MAX_RETRIEVED_DOCUMENTS = 10


class GraphState(BaseModel):
    messages: Annotated[list[AnyMessage], add_messages] = Field(
        default_factory=list, description='The list of messages in the graph state, appended automatically'
    )
    documents: Annotated[list[Document], create_document_reducer(MAX_RETRIEVED_DOCUMENTS)] = Field(
        default_factory=list, description='The list of retrieved documents, reduced to the most recent ones automatically'
    )
    query: str = Field(default='', description='The original query that triggered the graph execution')
    generated_query: str = Field(default='', description='The retrieval-optimized query generated from the original user query')
    route: Literal['', 'conversational', 'retrieval'] = Field(default='', description='The query route selected by the routing node')
    guardrail_passed: bool | None = Field(default=None, description='Whether the guardrail check accepted the user query')
    documents_relevant: bool | None = Field(default=None, description='Whether the retrieved documents were graded as relevant')
    retry_count: int = Field(default=0, ge=0, description='How many retrieval rewrite retries have been attempted')
    generation: str = Field(default='', description='The latest drafted answer generated from retrieved context')
    generation_grounded: bool | None = Field(default=None, description='Whether the drafted answer is grounded in retrieved context')
    generation_helpful: bool | None = Field(default=None, description='Whether the drafted answer is helpful for the user query')
    refusal_reason: str = Field(default='', description='Reason used when the workflow refuses to answer')
