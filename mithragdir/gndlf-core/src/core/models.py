import json
from datetime import datetime

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
    field_type = 'JSON'

    def db_value(self, value: object | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value)

    def python_value(self, value: str | bytes | bytearray | dict | list | None) -> object | None:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        return json.loads(value)


class Document(Model):
    document_id = AutoField(column_name='document_id')
    title = CharField()
    url = CharField(null=True)
    raw_content = TextField()
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        database = database
        table_name = 'document'


class PageIndex(Model):
    page_id = AutoField(column_name='page_id')
    title = CharField()
    url = CharField()

    class Meta:
        database = database
        table_name = 'page_index'


class WikiPage(Model):
    document = ForeignKeyField(Document, backref='wiki_page')
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
        database = database
        table_name = 'wiki_page'


class Text(Model):
    document = ForeignKeyField(Document, backref='text_source')
    author = CharField(null=True)
    publisher = CharField(null=True)
    published_year = IntegerField(null=True)
    isbn = CharField(null=True)
    language = CharField(null=True)
    source_path = CharField(null=True)
    file_format = CharField(null=True)

    class Meta:
        database = database
        table_name = 'text'


class Chunk(Model):
    chunk_id = AutoField(column_name='chunk_id')
    document = ForeignKeyField(Document, backref='chunks')
    chunk_index = IntegerField()
    content = TextField()
    token_count = IntegerField()
    meta_data = JsonField(null=True)
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        database = database
        table_name = 'chunks'

    def build_metadata_payload(self) -> dict:
        payload = {
            'document_id': self.document.document_id,
            'chunk_index': self.chunk_index,
            'token_count': self.token_count,
            'content': self.content,
        }
        if self.meta_data:
            payload.update(self.meta_data)
        return payload
