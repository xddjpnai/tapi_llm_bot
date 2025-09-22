from html import escape as html_escape
import re
from typing import List, Any


def tg_bold(text: str) -> str:
    return f"<b>{html_escape(str(text))}</b>"


def tg_italic(text: str) -> str:
    return f"<i>{html_escape(str(text))}</i>"


def tg_link(text: str, url: str) -> str:
    return f"<a href=\"{html_escape(url)}\">{html_escape(text)}</a>"


def tg_code(text: str) -> str:
    return f"<code>{html_escape(str(text))}</code>"


def chunk_text(text: str, max_len: int) -> List[str]:
    if not text:
        return [""]
    chunks: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for line in text.split("\n"):
        add = len(line) + 1
        if cur_len + add > max_len:
            chunks.append("\n".join(cur))
            cur = [line]
            cur_len = add
        else:
            cur.append(line)
            cur_len += add
    if cur:
        chunks.append("\n".join(cur))
    return chunks


def apply_citation_links_markdown_to_html(text: str, citations: List[Any]) -> str:
    escaped = html_escape(text or "")
    # Заменяем [i] на ссылки, если есть URL
    if citations:
        for idx, cit in enumerate(citations, start=1):
            url = None
            if isinstance(cit, dict):
                url = (cit.get("url") or cit.get("source") or "").strip() or None
            elif isinstance(cit, str):
                url = cit.strip() or None
            elif isinstance(cit, list) and cit:
                first = cit[0]
                if isinstance(first, str):
                    url = first.strip() or None
            if not url:
                continue
            pattern = re.compile(re.escape(f"[{idx}]"))
            escaped = pattern.sub(tg_link(f"[{idx}]", url), escaped)
    # Преобразуем базовый markdown для жирного шрифта **...** -> <b>...</b>
    def bold_sub(m: re.Match) -> str:
        return tg_bold(m.group(1))
    escaped = re.sub(r"\*\*(.+?)\*\*", bold_sub, escaped)
    return escaped


