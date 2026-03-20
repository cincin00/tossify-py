#!/usr/bin/env python3
"""Export faq_b.csv into Channel Talk article import Markdown files."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path


QUESTION_ANSWER_PATTERN = re.compile(r"^질문:\s*(.*?)\n답변:\s*(.*)$", re.S)
CODE_FENCE_LINE_PATTERN = re.compile(r"^\s*```")
INLINE_TAG_PATTERN = re.compile(r"</?[A-Za-z][^>\n]{0,120}>")
INLINE_TEMPLATE_PATTERN = re.compile(r"<!--\{.*?\}-->|(?:\{[=@?/.!][^{}\n]{0,200}\})")
COLON_CODE_LINE_PATTERN = re.compile(
    r"^(\s*(?:[-*]\s+|\d+\.\s+)?[^:\n]{0,120}:\s*)(.+)$"
)
INVALID_FILENAME_CHARS_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
LIST_PREFIX_PATTERN = re.compile(r"^\s*(?:[-*]|\d+[.)]|[①-⑳])\s*")
BRACKET_HEADING_PATTERN = re.compile(r"^\[(.+?)\]$")
COLON_HEADING_PATTERN = re.compile(r"^([^:\n]{1,60}?):$")
PURE_HTML_LINE_PATTERN = re.compile(r"^</?[A-Za-z][^>]*>$")
PURE_TEMPLATE_LINE_PATTERN = re.compile(r"^(?:<!--.*-->|[\{\}:/@?!=.\sA-Za-z0-9_\-\"'>]+)$")
CSS_SELECTOR_LINE_PATTERN = re.compile(r"^[.#][A-Za-z0-9_\-:#.\s>\[\]=,'\"()/*+~]+(?:\{|;)?$")
PATH_LIKE_PATTERN = re.compile(r"(?<![`\\w])(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+")

CHANNELTALK_ARTICLE_IMPORT_DOCS_URL = (
    "https://docs.channel.io/help/en/articles/Create-an-Article-be177b0b"
)
CHANNELTALK_IMPORT_FILE_LIMIT = 20
CHANNELTALK_IMPORT_MAX_FILE_SIZE_BYTES = 1_000_000


@dataclass
class MarkdownArticle:
    csv_row_number: int
    source_id: str
    question: str
    answer: str
    markdown_body: str
    markdown_document: str
    file_name: str
    relative_path: str
    batch_name: str
    byte_size: int


@dataclass
class MarkdownExportSummary:
    input_csv: str
    output_dir: str
    total_articles: int
    batch_count: int
    batch_size: int
    max_file_size_bytes: int
    max_generated_file_size_bytes: int
    oversized_files: list[str]
    docs_url: str


def normalize_text(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n").strip()


def parse_transformed_text(value: str) -> tuple[str, str]:
    normalized = normalize_text(value)
    match = QUESTION_ANSWER_PATTERN.match(normalized)
    if not match:
        msg = "transformed_text must start with '질문:' and contain a following '답변:' block."
        raise ValueError(msg)
    return normalize_text(match.group(1)), normalize_text(match.group(2))


def sanitize_file_name_component(value: str, max_length: int = 80) -> str:
    sanitized = INVALID_FILENAME_CHARS_PATTERN.sub(" ", value)
    sanitized = sanitized.replace("\n", " ")
    sanitized = re.sub(r"\s+", " ", sanitized).strip(" .")
    if not sanitized:
        sanitized = "article"
    return sanitized[:max_length].rstrip(" .") or "article"


def is_template_like_line(stripped: str) -> bool:
    return any(
        token in stripped
        for token in ("{=", "{?", "{/", "<!--{", "{ @", "{ ? ", "{ / ", "{.")
    )


def prose_without_inline_codeish_fragments(stripped: str) -> str:
    text = INLINE_TAG_PATTERN.sub(" ", stripped)
    text = INLINE_TEMPLATE_PATTERN.sub(" ", text)
    text = text.replace("`", " ")
    text = LIST_PREFIX_PATTERN.sub("", text)
    text = re.sub(r"[~<>{}=/\\|_*#;,+\[\]\"']", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def has_prose_context(stripped: str) -> bool:
    text = prose_without_inline_codeish_fragments(stripped)
    return bool(re.search(r"[가-힣]", text))


def is_code_like_line(stripped: str) -> bool:
    if not stripped:
        return False
    if CODE_FENCE_LINE_PATTERN.match(stripped):
        return False
    if stripped.startswith(("<!--", "<", "$(", "{", "function(", "});", "}", "</")):
        return True
    if PURE_HTML_LINE_PATTERN.match(stripped):
        return True
    if stripped.endswith(("{", "}", ";")) and not has_prose_context(stripped):
        return True
    if CSS_SELECTOR_LINE_PATTERN.match(stripped):
        return True
    if is_template_like_line(stripped):
        return not has_prose_context(stripped)
    if has_prose_context(stripped):
        return False
    if any(
        token in stripped
        for token in (
            "</",
            "/>",
            "class=",
            "style=",
            "href=",
            "src=",
            "onerror=",
            "onclick=",
            "$(",
            "function(",
            "{=",
            "{?",
            "{/",
            "<!--{",
        )
    ):
        return True
    if PURE_TEMPLATE_LINE_PATTERN.match(stripped) and any(char in stripped for char in "<>{}=/"):
        return True
    return False


def guess_code_language(code_lines: list[str]) -> str:
    joined = "\n".join(line.strip() for line in code_lines).lower()
    if any(token in joined for token in ("$(function", "function(", "console.log(", "});")):
        return "javascript"
    if any(token in joined for token in (".", "#")) and "{" in joined and ":" in joined:
        if "<" not in joined and "{=" not in joined and "{?" not in joined:
            return "css"
    if any(token in joined for token in ("<", "</", "<!--", "{=", "{?", "{/", "<!--{")):
        return "html"
    return ""


def wrap_inline_codeish_fragments(line: str) -> str:
    if "`" in line or CODE_FENCE_LINE_PATTERN.match(line):
        return line

    colon_match = COLON_CODE_LINE_PATTERN.match(line)
    if colon_match:
        prefix = colon_match.group(1)
        rest = colon_match.group(2).strip()
        if is_code_like_line(rest) or INLINE_TAG_PATTERN.search(rest) or INLINE_TEMPLATE_PATTERN.search(rest):
            return f"{prefix}`{rest}`"

    line = INLINE_TAG_PATTERN.sub(lambda match: f"`{match.group(0)}`", line)
    line = INLINE_TEMPLATE_PATTERN.sub(lambda match: f"`{match.group(0)}`", line)
    line = PATH_LIKE_PATTERN.sub(lambda match: f"`{match.group(0)}`", line)
    return line


def flush_code_block(buffer: list[str], output_lines: list[str]) -> None:
    if not buffer:
        return

    non_empty_lines = [line for line in buffer if line.strip()]
    if non_empty_lines:
        indent = min(len(line) - len(line.lstrip(" ")) for line in non_empty_lines)
    else:
        indent = 0

    indent_prefix = " " * indent
    language = guess_code_language(buffer)
    fence = f"{indent_prefix}```{language}".rstrip()

    if output_lines and output_lines[-1] != "":
        output_lines.append("")
    output_lines.append(fence)
    output_lines.extend(buffer)
    output_lines.append(f"{indent_prefix}```")
    output_lines.append("")
    buffer.clear()


def maybe_promote_heading(line: str) -> str | None:
    stripped = line.strip()
    if not stripped:
        return None
    if LIST_PREFIX_PATTERN.match(stripped):
        return None

    bracket_match = BRACKET_HEADING_PATTERN.match(stripped)
    if bracket_match:
        return f"## {bracket_match.group(1).strip()}"

    colon_match = COLON_HEADING_PATTERN.match(stripped)
    if colon_match:
        title = colon_match.group(1).strip()
        if title and len(title) <= 40 and not is_code_like_line(title):
            return f"## {title}"

    return None


def convert_answer_to_markdown(answer: str) -> str:
    normalized = normalize_text(answer)
    lines = normalized.split("\n")

    output_lines: list[str] = []
    code_buffer: list[str] = []
    in_existing_fence = False

    for line in lines:
        stripped = line.strip()

        if CODE_FENCE_LINE_PATTERN.match(line):
            flush_code_block(code_buffer, output_lines)
            output_lines.append(line.rstrip())
            in_existing_fence = not in_existing_fence
            continue

        if in_existing_fence:
            output_lines.append(line.rstrip())
            continue

        promoted_heading = maybe_promote_heading(line)
        if promoted_heading is not None:
            flush_code_block(code_buffer, output_lines)
            if output_lines and output_lines[-1] != "":
                output_lines.append("")
            output_lines.append(promoted_heading)
            output_lines.append("")
            continue

        if is_code_like_line(stripped):
            code_buffer.append(line.rstrip())
            continue

        flush_code_block(code_buffer, output_lines)
        output_lines.append(wrap_inline_codeish_fragments(line.rstrip()))

    flush_code_block(code_buffer, output_lines)

    markdown = "\n".join(output_lines)
    markdown = re.sub(r"\n{3,}", "\n\n", markdown)
    return normalize_text(markdown)


def build_markdown_document(question: str, markdown_body: str) -> str:
    return f"{markdown_body}\n"


def build_unique_file_name(question: str, used_file_names: dict[str, int]) -> str:
    base_name = sanitize_file_name_component(question, max_length=120)
    occurrence = used_file_names.get(base_name, 0) + 1
    used_file_names[base_name] = occurrence
    if occurrence == 1:
        return f"{base_name}.md"
    return f"{base_name} ({occurrence}).md"


def export_articles(
    input_csv: Path,
    output_dir: Path,
    batch_size: int,
) -> tuple[list[MarkdownArticle], MarkdownExportSummary]:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    articles: list[MarkdownArticle] = []
    oversized_files: list[str] = []
    max_generated_file_size = 0
    used_file_names: dict[str, int] = {}

    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for csv_row_number, row in enumerate(reader, start=2):
            question, answer = parse_transformed_text(row.get("transformed_text") or "")
            markdown_body = convert_answer_to_markdown(answer)
            markdown_document = build_markdown_document(question, markdown_body)

            article_index = len(articles) + 1
            batch_index = ((article_index - 1) // batch_size) + 1
            batch_name = f"batch_{batch_index:03d}"
            batch_dir = output_dir / batch_name
            batch_dir.mkdir(parents=True, exist_ok=True)

            file_name = build_unique_file_name(question, used_file_names)
            file_path = batch_dir / file_name
            file_path.write_text(markdown_document, encoding="utf-8")

            byte_size = file_path.stat().st_size
            max_generated_file_size = max(max_generated_file_size, byte_size)
            if byte_size > CHANNELTALK_IMPORT_MAX_FILE_SIZE_BYTES:
                oversized_files.append(str(file_path.relative_to(output_dir)))

            articles.append(
                MarkdownArticle(
                    csv_row_number=csv_row_number,
                    source_id=(row.get("source_id") or "").strip(),
                    question=question,
                    answer=answer,
                    markdown_body=markdown_body,
                    markdown_document=markdown_document,
                    file_name=file_name,
                    relative_path=str(file_path.relative_to(output_dir)),
                    batch_name=batch_name,
                    byte_size=byte_size,
                )
            )

    summary = MarkdownExportSummary(
        input_csv=str(input_csv),
        output_dir=str(output_dir),
        total_articles=len(articles),
        batch_count=math.ceil(len(articles) / batch_size) if articles else 0,
        batch_size=batch_size,
        max_file_size_bytes=CHANNELTALK_IMPORT_MAX_FILE_SIZE_BYTES,
        max_generated_file_size_bytes=max_generated_file_size,
        oversized_files=oversized_files,
        docs_url=CHANNELTALK_ARTICLE_IMPORT_DOCS_URL,
    )
    return articles, summary


def write_manifest(output_dir: Path, articles: list[MarkdownArticle]) -> None:
    manifest_path = output_dir / "manifest.csv"
    with manifest_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "article_index",
                "csv_row_number",
                "source_id",
                "batch_name",
                "relative_path",
                "file_name",
                "question",
                "answer_length",
                "markdown_body_length",
                "byte_size",
            ],
        )
        writer.writeheader()
        for article_index, article in enumerate(articles, start=1):
            writer.writerow(
                {
                    "article_index": article_index,
                    "csv_row_number": article.csv_row_number,
                    "source_id": article.source_id,
                    "batch_name": article.batch_name,
                    "relative_path": article.relative_path,
                    "file_name": article.file_name,
                    "question": article.question,
                    "answer_length": len(article.answer),
                    "markdown_body_length": len(article.markdown_body),
                    "byte_size": article.byte_size,
                }
            )


def write_summary(output_dir: Path, summary: MarkdownExportSummary) -> None:
    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(asdict(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_import_notes(output_dir: Path, summary: MarkdownExportSummary) -> None:
    notes_path = output_dir / "IMPORT_NOTES.txt"
    notes_path.write_text(
        "\n".join(
            [
                "Channel Talk article import notes",
                f"- Official docs: {summary.docs_url}",
                f"- Supported import file type used here: md",
                f"- Upload up to {CHANNELTALK_IMPORT_FILE_LIMIT} files at once",
                f"- File encoding must be UTF-8",
                f"- Non-PDF file size limit: {CHANNELTALK_IMPORT_MAX_FILE_SIZE_BYTES} bytes",
                f"- Generated article count: {summary.total_articles}",
                f"- Generated batch count: {summary.batch_count}",
                "- Upload one batch folder at a time",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert faq_b.csv into Channel Talk article import Markdown files, "
            "batched for the 20-file import limit."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/faq_b.csv"),
        help="Path to faq_b.csv.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/channeltalk_articles_md"),
        help="Directory where Markdown article files will be created.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=CHANNELTALK_IMPORT_FILE_LIMIT,
        help="How many files to place into each batch folder.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size < 1:
        msg = "--batch-size must be at least 1"
        raise ValueError(msg)

    articles, summary = export_articles(
        input_csv=args.input,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
    )
    write_manifest(args.output_dir, articles)
    write_summary(args.output_dir, summary)
    write_import_notes(args.output_dir, summary)

    print(
        json.dumps(
            {
                "output_dir": str(args.output_dir),
                "total_articles": summary.total_articles,
                "batch_count": summary.batch_count,
                "batch_size": summary.batch_size,
                "max_generated_file_size_bytes": summary.max_generated_file_size_bytes,
                "oversized_files": summary.oversized_files,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
