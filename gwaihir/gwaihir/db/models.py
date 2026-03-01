import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class Index:
    title: str
    pageid: int
    url: str


@dataclass
class Page:
    title: str
    pageid: int
    url: str
    content: str
    categories: list[dict[str, object]] | None = None
    images: list[str] | None = None
    links: list[dict[str, object]] | None = None
    external_links: list[str] | None = None
    sections: list[dict[str, object]] | None = None
    revid: int | str | None = None
    displaytitle: str | None = None
    properties: list[dict[str, object]] | None = None

    def dump_to_json(self, base_path: str | Path | None = None, filename: str | None = None) -> Path:
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            safe_title = re.sub(r'[^A-Za-z0-9._-]+', '_', self.title).strip('_')
            if not safe_title:
                safe_title = 'page'
            filename = f'{timestamp}_{safe_title}.json'

        base_path = Path.cwd() if base_path is None else Path(base_path)
        path = base_path / filename
        with path.open('w', encoding='utf-8') as file:
            json.dump(asdict(self), file, ensure_ascii=False, indent=2)

        return path
