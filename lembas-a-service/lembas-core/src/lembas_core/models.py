from datetime import datetime
import json
from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    Field,
    ForeignKeyField,
    IntegerField,
    Model,
    Proxy,
    TextField,
)

database = Proxy()


class JsonField(Field):
    field_type = "JSON"

    def db_value(self, value):
        if value is None:
            return None
        return json.dumps(value)

    def python_value(self, value):
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        return json.loads(value)


class BaseModel(Model):
    class Meta:
        database = database


class Document(BaseModel):
    document_id = AutoField(column_name="document_id")
    title = CharField()
    url = CharField(null=True)
    raw_content = TextField()
    created_at = DateTimeField(default=datetime.now)


class Index(BaseModel):
    page_id = IntegerField(unique=True)
    title = CharField()
    url = CharField()

    class Meta:
        table_name = "index"


class WikiPage(BaseModel):
    document = ForeignKeyField(Document, backref="wiki_page")
    page_id = IntegerField()
    categories = JsonField(default=list)
    images = JsonField(default=list)
    links = JsonField(default=list)
    external_links = JsonField(default=list)
    sections = JsonField(default=list)
    revid = IntegerField(null=True)
    displaytitle = CharField(null=True)
    properties = JsonField(default=list)

    class Meta:
        table_name = "wiki_page"


class Text(BaseModel):
    document = ForeignKeyField(Document, backref="text_source")
    author = CharField(null=True)
    publisher = CharField(null=True)
    published_year = IntegerField(null=True)
    isbn = CharField(null=True)
    language = CharField(null=True)
    source_path = CharField(null=True)
    file_format = CharField(null=True)

    class Meta:
        table_name = "text"


class Chunk(BaseModel):
    document = ForeignKeyField(Document, backref="chunks")
    chunk_index = IntegerField()
    content = TextField()
    token_count = IntegerField()
    meta_data = JsonField(null=True)
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "chunks"

    def build_metadata_payload(self) -> dict:
        payload = {
            "document_id": self.document_id,
            "chunk_index": self.chunk_index,
            "token_count": self.token_count,
            "content": self.content,
        }
        if self.meta_data:
            payload.update(self.meta_data)
        return payload
