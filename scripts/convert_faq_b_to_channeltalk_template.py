#!/usr/bin/env python3
"""Build a Channel Talk FAQ bulk upload workbook from faq_b.csv."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile
from xml.etree import ElementTree as ET


NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_MC = "http://schemas.openxmlformats.org/markup-compatibility/2006"
NS_X14AC = "http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac"
NS_XR = "http://schemas.microsoft.com/office/spreadsheetml/2014/revision"
NS_XR2 = "http://schemas.microsoft.com/office/spreadsheetml/2015/revision2"
NS_XR3 = "http://schemas.microsoft.com/office/spreadsheetml/2016/revision3"
NS_XML = "http://www.w3.org/XML/1998/namespace"

NSMAP = {
    "main": NS_MAIN,
    "rel": NS_REL,
}

ET.register_namespace("", NS_MAIN)
ET.register_namespace("r", NS_REL)
ET.register_namespace("mc", NS_MC)
ET.register_namespace("x14ac", NS_X14AC)
ET.register_namespace("xr", NS_XR)
ET.register_namespace("xr2", NS_XR2)
ET.register_namespace("xr3", NS_XR3)


UPLOAD_SHEET_NAME = "FAQs(Fix Me)"
GUIDE_SHEET_NAME = "Guide"
QUESTION_LIMIT = 100
ANSWER_LIMIT = 500
QUESTION_ANSWER_PATTERN = re.compile(r"^질문:\s*(.*?)\n답변:\s*(.*)$", re.S)
CODE_FENCE_PATTERN = re.compile(r"```(?:[a-zA-Z0-9_+-]+)?\n?|```")
MARKDOWN_BOLD_PATTERN = re.compile(r"\*\*(.*?)\*\*")
INLINE_CODE_PATTERN = re.compile(r"`([^`\n]+)`")


@dataclass
class TemplateAnalysis:
    sheet_names: list[str]
    upload_sheet: str
    upload_columns: list[str]
    uploadable_columns: list[str]
    answer_formats: list[str]
    non_reflected_columns: list[str]
    notes: list[str]
    docs_url: str | None


@dataclass
class ConversionIssue:
    source_id: str
    csv_row_number: int
    field: str
    action: str
    original_length: int | None = None
    final_length: int | None = None
    message: str | None = None


def qname(local_name: str) -> str:
    return f"{{{NS_MAIN}}}{local_name}"


def normalize_text(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n").strip()


def column_letter_to_index(column: str) -> int:
    value = 0
    for char in column:
        if not char.isalpha():
            continue
        value = (value * 26) + (ord(char.upper()) - 64)
    return value


def column_index_to_letter(index: int) -> str:
    if index < 1:
        msg = f"Column index must be >= 1, got {index}"
        raise ValueError(msg)

    letters: list[str] = []
    current = index
    while current:
        current, remainder = divmod(current - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def read_shared_strings(workbook: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        return []

    shared_root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    shared_strings: list[str] = []
    for item in shared_root.findall("main:si", NSMAP):
        fragments = [node.text or "" for node in item.iterfind(".//main:t", NSMAP)]
        shared_strings.append("".join(fragments))
    return shared_strings


def parse_shared_strings_metadata(workbook: ZipFile) -> tuple[ET.Element, list[str], int]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        msg = "The template workbook does not contain xl/sharedStrings.xml."
        raise ValueError(msg)

    shared_strings_xml = workbook.read("xl/sharedStrings.xml")
    shared_strings_root = ET.fromstring(shared_strings_xml)
    shared_strings = read_shared_strings(workbook)
    shared_string_count = int(shared_strings_root.attrib.get("count", len(shared_strings)))
    return shared_strings_root, shared_strings, shared_string_count


def workbook_sheet_paths(workbook: ZipFile) -> dict[str, str]:
    workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
    rels_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))

    relationships = {
        rel.attrib["Id"]: f"xl/{rel.attrib['Target']}"
        for rel in rels_root
        if rel.attrib.get("Type", "").endswith("/worksheet")
    }

    sheet_paths: dict[str, str] = {}
    for sheet in workbook_root.findall("main:sheets/main:sheet", NSMAP):
        name = sheet.attrib["name"]
        rel_id = sheet.attrib[f"{{{NS_REL}}}id"]
        sheet_paths[name] = relationships[rel_id]
    return sheet_paths


def cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("main:v", NSMAP)
    inline_string = cell.find("main:is", NSMAP)

    if cell_type == "s" and value_node is not None:
        return shared_strings[int(value_node.text or "0")]

    if cell_type == "inlineStr" and inline_string is not None:
        fragments = [node.text or "" for node in inline_string.iterfind(".//main:t", NSMAP)]
        return "".join(fragments)

    if value_node is not None and value_node.text is not None:
        return value_node.text

    return ""


def read_sheet_rows(
    workbook: ZipFile,
    sheet_path: str,
    shared_strings: list[str],
) -> list[tuple[int, dict[str, str]]]:
    sheet_root = ET.fromstring(workbook.read(sheet_path))
    rows: list[tuple[int, dict[str, str]]] = []

    for row in sheet_root.findall("main:sheetData/main:row", NSMAP):
        row_number = int(row.attrib["r"])
        row_cells: dict[str, str] = {}
        for cell in row.findall("main:c", NSMAP):
            reference = cell.attrib.get("r", "")
            column = "".join(ch for ch in reference if ch.isalpha())
            row_cells[column] = cell_value(cell, shared_strings)
        rows.append((row_number, row_cells))
    return rows


def ordered_row_values(row: dict[str, str]) -> list[str]:
    return [
        value
        for _, value in sorted(
            row.items(),
            key=lambda item: column_letter_to_index(item[0]),
        )
    ]


def analyze_template(template_path: Path) -> TemplateAnalysis:
    with ZipFile(template_path) as workbook:
        shared_strings = read_shared_strings(workbook)
        sheet_paths = workbook_sheet_paths(workbook)

        if UPLOAD_SHEET_NAME not in sheet_paths:
            msg = f"Upload sheet {UPLOAD_SHEET_NAME!r} was not found in {template_path}"
            raise ValueError(msg)
        if GUIDE_SHEET_NAME not in sheet_paths:
            msg = f"Guide sheet {GUIDE_SHEET_NAME!r} was not found in {template_path}"
            raise ValueError(msg)

        upload_rows = read_sheet_rows(workbook, sheet_paths[UPLOAD_SHEET_NAME], shared_strings)
        guide_rows = dict(read_sheet_rows(workbook, sheet_paths[GUIDE_SHEET_NAME], shared_strings))

        header_row = next((row for row_number, row in upload_rows if row_number == 1), None)
        if not header_row:
            msg = f"Header row was not found in sheet {UPLOAD_SHEET_NAME!r}"
            raise ValueError(msg)

        upload_columns = ordered_row_values(header_row)
        uploadable_columns = [
            column
            for column in upload_columns
            if column in {"id", "language", "answer", "publishState"}
            or column.startswith("question")
        ]

        answer_formats = [
            guide_rows.get(9, {}).get("A", ""),
            guide_rows.get(10, {}).get("A", ""),
        ]
        answer_formats = [value for value in answer_formats if value]

        note_row = guide_rows.get(13, {}).get("B", "")
        non_reflected_columns = re.findall(r"(createdAt|updatedAt|alfUsedCount)", note_row)

        docs_url = guide_rows.get(16, {}).get("B") or None
        notes = [
            guide_rows.get(2, {}).get("B", ""),
            guide_rows.get(3, {}).get("B", ""),
            guide_rows.get(4, {}).get("B", ""),
            guide_rows.get(5, {}).get("B", ""),
            guide_rows.get(6, {}).get("B", ""),
            note_row,
        ]
        notes = [note for note in notes if note]

        return TemplateAnalysis(
            sheet_names=list(sheet_paths.keys()),
            upload_sheet=UPLOAD_SHEET_NAME,
            upload_columns=upload_columns,
            uploadable_columns=uploadable_columns,
            answer_formats=answer_formats,
            non_reflected_columns=non_reflected_columns,
            notes=notes,
            docs_url=docs_url,
        )


def smart_truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text

    suffix = "..."
    cutoff = limit - len(suffix)
    if cutoff <= 0:
        return text[:limit]

    candidate_positions = [
        text.rfind("\n\n", 0, cutoff + 1),
        text.rfind("\n", 0, cutoff + 1),
        text.rfind(". ", 0, cutoff + 1),
        text.rfind("? ", 0, cutoff + 1),
        text.rfind("! ", 0, cutoff + 1),
        text.rfind(" ", 0, cutoff + 1),
    ]
    minimum_reasonable_cut = max(40, int(cutoff * 0.6))
    valid_positions = [position for position in candidate_positions if position >= minimum_reasonable_cut]

    cut_position = max(valid_positions) if valid_positions else cutoff
    truncated = text[:cut_position].rstrip()
    if not truncated:
        truncated = text[:cutoff].rstrip()
    return f"{truncated}{suffix}"


def sanitize_answer_for_channeltalk(answer: str) -> tuple[str, bool]:
    sanitized = normalize_text(answer)
    original = sanitized

    sanitized = CODE_FENCE_PATTERN.sub("", sanitized)
    sanitized = MARKDOWN_BOLD_PATTERN.sub(r"\1", sanitized)
    sanitized = INLINE_CODE_PATTERN.sub(r"\1", sanitized)
    sanitized = sanitized.replace("<", "＜").replace(">", "＞")
    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
    sanitized = normalize_text(sanitized)

    return sanitized, sanitized != original


def enforce_limit(
    text: str,
    limit: int,
    field_name: str,
    overflow_policy: str,
    source_id: str,
    csv_row_number: int,
    issues: list[ConversionIssue],
) -> tuple[str | None, bool]:
    if len(text) <= limit:
        return text, False

    if overflow_policy == "error":
        msg = (
            f"Row {csv_row_number} (source_id={source_id}) exceeds the {field_name} "
            f"limit of {limit} characters."
        )
        raise ValueError(msg)

    if overflow_policy == "skip":
        issues.append(
            ConversionIssue(
                source_id=source_id,
                csv_row_number=csv_row_number,
                field=field_name,
                action="skipped_row",
                original_length=len(text),
                final_length=None,
                message=f"{field_name} exceeded the {limit}-character limit.",
            )
        )
        return None, True

    truncated = smart_truncate(text, limit)
    issues.append(
        ConversionIssue(
            source_id=source_id,
            csv_row_number=csv_row_number,
            field=field_name,
            action="truncated",
            original_length=len(text),
            final_length=len(truncated),
            message=f"{field_name} was truncated to satisfy the {limit}-character limit.",
        )
    )
    return truncated, True


def parse_transformed_text(value: str) -> tuple[str, str]:
    normalized = normalize_text(value)
    match = QUESTION_ANSWER_PATTERN.match(normalized)
    if not match:
        msg = "transformed_text must start with '질문:' and contain a following '답변:' block."
        raise ValueError(msg)
    question = normalize_text(match.group(1))
    answer = normalize_text(match.group(2))
    return question, answer


def build_output_rows(
    input_csv: Path,
    language: str,
    publish_state: str,
    overflow_policy: str,
) -> tuple[list[dict[str, str]], dict[str, Any], list[ConversionIssue]]:
    rows: list[dict[str, str]] = []
    issues: list[ConversionIssue] = []

    total_rows = 0
    parsed_rows = 0
    skipped_rows = 0
    question_truncations = 0
    answer_truncations = 0
    sanitized_answers = 0

    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for csv_row_number, raw_row in enumerate(reader, start=2):
            total_rows += 1
            source_id = (raw_row.get("source_id") or "").strip()
            transformed_text = raw_row.get("transformed_text") or ""

            try:
                question, answer = parse_transformed_text(transformed_text)
            except ValueError as exc:
                skipped_rows += 1
                issues.append(
                    ConversionIssue(
                        source_id=source_id,
                        csv_row_number=csv_row_number,
                        field="transformed_text",
                        action="skipped_row",
                        message=str(exc),
                    )
                )
                continue

            parsed_rows += 1
            original_answer = answer
            answer, answer_sanitized = sanitize_answer_for_channeltalk(answer)
            if answer_sanitized:
                sanitized_answers += 1
                issues.append(
                    ConversionIssue(
                        source_id=source_id,
                        csv_row_number=csv_row_number,
                        field="answer",
                        action="sanitized_markup",
                        original_length=len(original_answer),
                        final_length=len(answer),
                        message=(
                            "Markdown/code-like markup was normalized so the answer can be "
                            "uploaded as plaintext."
                        ),
                    )
                )

            question_result, question_overflow = enforce_limit(
                text=question,
                limit=QUESTION_LIMIT,
                field_name="question1",
                overflow_policy=overflow_policy,
                source_id=source_id,
                csv_row_number=csv_row_number,
                issues=issues,
            )
            if question_result is None:
                skipped_rows += 1
                continue
            if question_overflow:
                question_truncations += 1

            answer_result, answer_overflow = enforce_limit(
                text=answer,
                limit=ANSWER_LIMIT,
                field_name="answer",
                overflow_policy=overflow_policy,
                source_id=source_id,
                csv_row_number=csv_row_number,
                issues=issues,
            )
            if answer_result is None:
                skipped_rows += 1
                continue
            if answer_overflow:
                answer_truncations += 1

            rows.append(
                {
                    "id": "",
                    "language": language,
                    "question1": question_result,
                    "question2": "",
                    "question3": "",
                    "question4": "",
                    "question5": "",
                    "question6": "",
                    "question7": "",
                    "question8": "",
                    "question9": "",
                    "question10": "",
                    "question11": "",
                    "answer": answer_result,
                    "publishState": publish_state,
                }
            )

    summary = {
        "input_rows": total_rows,
        "parsed_rows": parsed_rows,
        "output_rows": len(rows),
        "skipped_rows": skipped_rows,
        "question_truncations": question_truncations,
        "answer_truncations": answer_truncations,
        "sanitized_answers": sanitized_answers,
        "language": language,
        "publish_state": publish_state,
        "overflow_policy": overflow_policy,
    }
    return rows, summary, issues

def get_shared_string_index(
    value: str,
    shared_strings: list[str],
    shared_string_lookup: dict[str, int],
) -> int:
    existing_index = shared_string_lookup.get(value)
    if existing_index is not None:
        return existing_index

    new_index = len(shared_strings)
    shared_strings.append(value)
    shared_string_lookup[value] = new_index
    return new_index


def build_shared_string_cell(
    cell_reference: str,
    value: str,
    shared_strings: list[str],
    shared_string_lookup: dict[str, int],
) -> tuple[ET.Element, int]:
    cell_attributes = {"r": cell_reference, "s": "1"}
    if value == "":
        return ET.Element(qname("c"), cell_attributes), 0

    cell_attributes["t"] = "s"
    cell = ET.Element(qname("c"), cell_attributes)
    value_node = ET.SubElement(cell, qname("v"))
    value_node.text = str(
        get_shared_string_index(
            value=value,
            shared_strings=shared_strings,
            shared_string_lookup=shared_string_lookup,
        )
    )
    return cell, 1


def render_sheet_xml(
    template_sheet_xml: bytes,
    headers: list[str],
    rows: list[dict[str, str]],
    shared_strings: list[str],
    shared_string_lookup: dict[str, int],
) -> tuple[bytes, int]:
    root = ET.fromstring(template_sheet_xml)
    sheet_data = root.find(qname("sheetData"))
    if sheet_data is None:
        msg = "sheetData node was not found in the template upload sheet."
        raise ValueError(msg)

    header_row = None
    for row in list(sheet_data):
        if row.attrib.get("r") == "1":
            header_row = row
            continue
        sheet_data.remove(row)

    if header_row is None:
        msg = "The template upload sheet does not contain a header row."
        raise ValueError(msg)

    shared_string_references = 0
    for excel_row_number, row_data in enumerate(rows, start=2):
        row_element = ET.Element(
            qname("row"),
            {
                "r": str(excel_row_number),
                "spans": f"1:{len(headers)}",
                "ht": "17",
            },
        )
        for column_index, header in enumerate(headers, start=1):
            cell_reference = f"{column_index_to_letter(column_index)}{excel_row_number}"
            cell, reference_count = build_shared_string_cell(
                cell_reference=cell_reference,
                value=row_data.get(header, ""),
                shared_strings=shared_strings,
                shared_string_lookup=shared_string_lookup,
            )
            shared_string_references += reference_count
            row_element.append(cell)
        sheet_data.append(row_element)

    dimension = root.find(qname("dimension"))
    if dimension is not None:
        last_row = max(len(rows) + 1, 1)
        last_column = column_index_to_letter(len(headers))
        dimension.set("ref", f"A1:{last_column}{last_row}")

    selection = root.find("main:sheetViews/main:sheetView/main:selection", NSMAP)
    if selection is not None:
        active_row = 2 if rows else 1
        active_cell = f"A{active_row}"
        selection.set("activeCell", active_cell)
        selection.set("sqref", active_cell)

    rendered_xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    rendered_xml = preserve_template_worksheet_envelope(
        template_sheet_xml=template_sheet_xml,
        rendered_sheet_xml=rendered_xml,
    )
    return rendered_xml, shared_string_references


def build_shared_string_item(value: str) -> ET.Element:
    item = ET.Element(qname("si"))
    text_node = ET.SubElement(item, qname("t"))
    text_node.set(f"{{{NS_XML}}}space", "preserve")
    text_node.text = value
    return item


def preserve_template_worksheet_envelope(
    template_sheet_xml: bytes,
    rendered_sheet_xml: bytes,
) -> bytes:
    template_declaration = re.match(rb"^<\?xml[^>]+\?>\s*", template_sheet_xml)
    template_opening_tag = re.search(rb"<worksheet\b[^>]*>", template_sheet_xml)
    rendered_opening_tag = re.search(rb"<worksheet\b[^>]*>", rendered_sheet_xml)
    if template_opening_tag is None or rendered_opening_tag is None:
        return rendered_sheet_xml

    rendered_without_declaration = re.sub(
        rb"^<\?xml[^>]+\?>\s*",
        b"",
        rendered_sheet_xml,
        count=1,
    )
    replaced = rendered_without_declaration.replace(
        rendered_opening_tag.group(0),
        template_opening_tag.group(0),
        1,
    )

    if template_declaration is None:
        return replaced
    return template_declaration.group(0) + replaced.lstrip()


def render_shared_strings_xml(
    shared_strings_root: ET.Element,
    shared_strings: list[str],
    original_unique_count: int,
    original_count: int,
    additional_references: int,
) -> bytes:
    for value in shared_strings[original_unique_count:]:
        shared_strings_root.append(build_shared_string_item(value))

    shared_strings_root.set("count", str(original_count + additional_references))
    shared_strings_root.set("uniqueCount", str(len(shared_strings)))
    return ET.tostring(shared_strings_root, encoding="utf-8", xml_declaration=True)


def write_output_workbook(
    template_path: Path,
    output_path: Path,
    upload_sheet_xml: bytes,
    shared_strings_xml: bytes,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(template_path) as source_workbook:
        sheet_paths = workbook_sheet_paths(source_workbook)
        upload_sheet_path = sheet_paths[UPLOAD_SHEET_NAME]
        shared_strings_path = "xl/sharedStrings.xml"

        with ZipFile(output_path, "w", compression=ZIP_DEFLATED) as destination_workbook:
            for info in source_workbook.infolist():
                file_bytes = source_workbook.read(info.filename)
                if info.filename == upload_sheet_path:
                    file_bytes = upload_sheet_xml
                if info.filename == shared_strings_path:
                    file_bytes = shared_strings_xml
                destination_workbook.writestr(info, file_bytes)


def default_report_path(output_path: Path) -> Path:
    return output_path.with_suffix(".report.json")


def build_report(
    template_path: Path,
    input_csv: Path,
    output_path: Path,
    template_analysis: TemplateAnalysis,
    conversion_summary: dict[str, Any],
    issues: list[ConversionIssue],
) -> dict[str, Any]:
    return {
        "template_path": str(template_path),
        "input_csv": str(input_csv),
        "output_xlsx": str(output_path),
        "template_analysis": asdict(template_analysis),
        "mapping": {
            "id": "leave blank to create a new FAQ",
            "language": conversion_summary["language"],
            "question1": "parsed question from transformed_text",
            "question2_to_question11": "left blank because faq_b provides one transformed question per row",
            "answer": "parsed answer from transformed_text as plaintext",
            "publishState": conversion_summary["publish_state"],
        },
        "limits": {
            "question1_max_characters": QUESTION_LIMIT,
            "answer_max_characters": ANSWER_LIMIT,
        },
        "conversion_summary": conversion_summary,
        "issues": [asdict(issue) for issue in issues],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze a Channel Talk FAQ upload template and convert faq_b.csv "
            "into an uploadable XLSX workbook."
        )
    )
    parser.add_argument(
        "--template",
        type=Path,
        default=Path("data/faq_upload_template_ko.xlsx"),
        help="Path to the Channel Talk FAQ upload template workbook.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/faq_b.csv"),
        help="Path to the faq_b CSV file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/faq_b_channeltalk_upload.xlsx"),
        help="Path to write the converted XLSX workbook.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path for the JSON conversion report.",
    )
    parser.add_argument(
        "--language",
        default="ko",
        help="Language code to write into the upload sheet.",
    )
    parser.add_argument(
        "--publish-state",
        choices=["published", "unpublished"],
        default="unpublished",
        help="Publish state to set for all generated FAQs.",
    )
    parser.add_argument(
        "--overflow-policy",
        choices=["truncate", "skip", "error"],
        default="truncate",
        help=(
            "How to handle Channel Talk character limits. "
            "'truncate' keeps every row uploadable, 'skip' drops overflowing rows, "
            "and 'error' stops on the first overflow."
        ),
    )
    return parser.parse_args()


def validate_template_headers(template_analysis: TemplateAnalysis) -> None:
    expected_headers = [
        "id",
        "language",
        "question1",
        "question2",
        "question3",
        "question4",
        "question5",
        "question6",
        "question7",
        "question8",
        "question9",
        "question10",
        "question11",
        "answer",
        "publishState",
    ]
    if template_analysis.upload_columns != expected_headers:
        msg = (
            "The upload sheet headers do not match the expected Channel Talk FAQ "
            f"template structure.\nExpected: {expected_headers}\n"
            f"Actual:   {template_analysis.upload_columns}"
        )
        raise ValueError(msg)


def main() -> int:
    args = parse_args()
    report_path = args.report or default_report_path(args.output)

    template_analysis = analyze_template(args.template)
    validate_template_headers(template_analysis)

    rows, conversion_summary, issues = build_output_rows(
        input_csv=args.input,
        language=args.language,
        publish_state=args.publish_state,
        overflow_policy=args.overflow_policy,
    )

    with ZipFile(args.template) as template_workbook:
        upload_sheet_path = workbook_sheet_paths(template_workbook)[UPLOAD_SHEET_NAME]
        shared_strings_root, shared_strings, shared_string_count = parse_shared_strings_metadata(
            template_workbook
        )
        original_unique_count = len(shared_strings)
        shared_string_lookup = {value: index for index, value in enumerate(shared_strings)}

        upload_sheet_xml, additional_shared_string_references = render_sheet_xml(
            template_sheet_xml=template_workbook.read(upload_sheet_path),
            headers=template_analysis.upload_columns,
            rows=rows,
            shared_strings=shared_strings,
            shared_string_lookup=shared_string_lookup,
        )
        shared_strings_xml = render_shared_strings_xml(
            shared_strings_root=shared_strings_root,
            shared_strings=shared_strings,
            original_unique_count=original_unique_count,
            original_count=shared_string_count,
            additional_references=additional_shared_string_references,
        )

    write_output_workbook(
        template_path=args.template,
        output_path=args.output,
        upload_sheet_xml=upload_sheet_xml,
        shared_strings_xml=shared_strings_xml,
    )

    report = build_report(
        template_path=args.template,
        input_csv=args.input,
        output_path=args.output,
        template_analysis=template_analysis,
        conversion_summary=conversion_summary,
        issues=issues,
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    json.dump(
        {
            "output_xlsx": str(args.output),
            "report_json": str(report_path),
            "conversion_summary": conversion_summary,
        },
        sys.stdout,
        ensure_ascii=False,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
