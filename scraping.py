import argparse
import csv
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import requests


DEFAULT_BASE_URL = "https://www.firstmall.kr/customer/faq/search"
DEFAULT_DETAIL_URL_TEMPLATE = "https://www.firstmall.kr/customer/faq/{source_id}"
DEFAULT_OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
A_CSV_FIELDS = [
    "source_id",
    "category",
    "title",
    "question",
    "answer",
    "created_at",
    "updated_at",
    "raw_json",
]
B_CSV_FIELDS = [
    "source_id",
    "original_title",
    "original_question",
    "original_answer",
    "transformed_text",
    "model",
    "prompt_instruction",
]
DEFAULT_STYLE_INSTRUCTION = """역할: FAQ 문체 변환기
목표: 원문의 사실과 의미를 유지하고, 표현만 Toss 스타일의 친근하고 명확한 한국어로 다듬습니다.

핵심 규칙:
1) 사실 보존: 숫자/날짜/요금/기간/조건/URL/버튼명/메뉴명은 변경하지 않습니다.
2) 추정 금지: 원문에 없는 정보는 추가하지 않습니다.
3) 문체: 간결하고 정중한 대화체(~해요/~할 수 있어요). 불필요한 사과/장식/이모지는 금지합니다.
4) 구조: 결론 먼저, 단계가 있으면 bullet을 사용합니다.
5) 답변 원문이 비어 있으면 새 답변을 생성하지 않고 status를 insufficient_source로 반환합니다.

출력 형식:
- 반드시 JSON 객체 하나만 출력합니다.
- 키는 question, answer, status, reason만 사용합니다.
- status는 ok 또는 insufficient_source 중 하나입니다.
- status가 insufficient_source면 reason에 원인을 짧게 작성합니다."""


@dataclass
class FaqItem:
    source_id: str
    category: str
    title: str
    question: str
    answer: str
    created_at: str
    updated_at: str
    raw_json: str


@dataclass
class FaqTransformed:
    source_id: str
    original_title: str
    original_question: str
    original_answer: str
    transformed_text: str
    model: str
    prompt_instruction: str


class OpenAIRequestError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def build_headers(user_agent: str) -> dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
    }


def http_get_json(
        url: str, params: dict[str, Any], headers: dict[str, str], timeout: int) -> Any:
    query = urlencode(params)
    request = Request(f"{url}?{query}", headers=headers, method="GET")
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def http_get_text(url: str, headers: dict[str, str], timeout: int) -> str:
    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", "ignore")


def extract_items(
        response_json: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if isinstance(response_json, list):
        return response_json, {}

    if not isinstance(response_json, dict):
        raise ValueError("FAQ API response must be dict or list")

    candidate_keys = ["items", "data", "results", "faq", "faqs", "list"]
    for key in candidate_keys:
        value = response_json.get(key)
        if isinstance(value, list):
            return value, response_json
        if isinstance(value, dict):
            nested_list = value.get("items") or value.get("list")
            if isinstance(nested_list, list):
                return nested_list, response_json

    for value in response_json.values():
        if isinstance(value, list):
            return value, response_json

    return [], response_json


def normalize_item(item: dict[str, Any]) -> FaqItem:
    source_id = str(
        item.get("id")
        or item.get("idx")
        or item.get("faq_id")
        or item.get("seq")
        or ""
    )
    category = str(item.get("category") or item.get("category_name") or "")
    title = str(item.get("title") or item.get(
        "subject") or item.get("question") or "")
    question = str(item.get("question") or item.get(
        "title") or item.get("subject") or "")
    answer = str(item.get("answer") or item.get(
        "contents") or item.get("content") or "")
    created_at = str(item.get("created_at") or item.get("regist_date") or "")
    updated_at = str(item.get("updated_at") or item.get("modify_date") or "")

    return FaqItem(
        source_id=source_id,
        category=category,
        title=title,
        question=question,
        answer=answer,
        created_at=created_at,
        updated_at=updated_at,
        raw_json=json.dumps(item, ensure_ascii=False),
    )


def html_fragment_to_text(html_fragment: str) -> str:
    cleaned = re.sub(
        r"<(script|style)\b[^>]*>.*?</\1>",
        "",
        html_fragment,
        flags=re.IGNORECASE | re.DOTALL,
    )
    cleaned = re.sub(r"<br\s*/?>", "\n", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"</(p|li|div|h[1-6]|tr|section|article)>",
        "\n",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    cleaned = unescape(cleaned)
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = cleaned.replace("\r", "")

    lines = []
    for line in cleaned.split("\n"):
        normalized = re.sub(r"[ \t]+", " ", line).strip()
        if normalized:
            lines.append(normalized)

    return "\n".join(lines).strip()


def extract_detail_fields(detail_html: str) -> tuple[str, str, str]:
    view_match = re.search(
        r'<div\s+class="customer_view"\s*>(.*?)<!--\s*//FAQ 보기\s*-->',
        detail_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    view_html = view_match.group(1) if view_match else detail_html

    category_match = re.search(
        r'<p\s+class="gray">\s*(.*?)\s*</p>',
        view_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    title_match = re.search(
        r"<dt\b[^>]*>.*?<h3>\s*(.*?)\s*</h3>",
        view_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    contents_match = re.search(
        r'<div\s+class="faq_contents"\s*>(.*?)</div>\s*</dd>',
        view_html,
        flags=re.IGNORECASE | re.DOTALL,
    )

    category = (
        html_fragment_to_text(category_match.group(1))
        if category_match
        else ""
    )
    title = html_fragment_to_text(title_match.group(1)) if title_match else ""
    answer = (
        html_fragment_to_text(contents_match.group(1))
        if contents_match
        else ""
    )
    return category, title, answer


def build_detail_url(detail_url_template: str, source_id: str) -> str:
    try:
        return detail_url_template.format(
            source_id=source_id,
            seq=source_id,
            id=source_id,
        )
    except KeyError:
        return detail_url_template.replace("{source_id}", source_id)


def enrich_item_with_detail(
    item: FaqItem,
    detail_url_template: str,
    headers: dict[str, str],
    timeout: int,
) -> FaqItem:
    if not item.source_id:
        return item

    detail_url = build_detail_url(detail_url_template, item.source_id)
    detail_html = http_get_text(detail_url, headers=headers, timeout=timeout)
    detail_category, detail_title, detail_answer = extract_detail_fields(detail_html)

    enriched = FaqItem(
        source_id=item.source_id,
        category=detail_category or item.category,
        title=detail_title or item.title,
        question=detail_title or item.question or item.title,
        answer=detail_answer or item.answer,
        created_at=item.created_at,
        updated_at=item.updated_at,
        raw_json=json.dumps(
            {
                "list_item": json.loads(item.raw_json),
                "detail_url": detail_url,
                "detail_answer_collected": bool(detail_answer),
            },
            ensure_ascii=False,
        ),
    )
    return enriched


def collect_all_faq(
    base_url: str,
    detail_url_template: str,
    per_page: int,
    max_pages: int | None,
    timeout: int,
    user_agent: str,
    sleep_sec: float,
) -> list[FaqItem]:
    headers = build_headers(user_agent)
    page = 1
    results: list[FaqItem] = []
    seen_ids: set[str] = set()
    detail_enriched_count = 0
    detail_fail_count = 0

    while True:
        if max_pages is not None and page > max_pages:
            break

        payload = {
            "page": str(page),
            "per_page": str(per_page),
            "order_by": "regist_date",
            "category": "",
            "keyword": "",
        }

        response_json = http_get_json(
            base_url, payload, headers=headers, timeout=timeout)
        raw_items, metadata = extract_items(response_json)

        if not raw_items:
            break

        new_count = 0
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            normalized = normalize_item(item)
            try:
                normalized = enrich_item_with_detail(
                    item=normalized,
                    detail_url_template=detail_url_template,
                    headers=headers,
                    timeout=timeout,
                )
                if normalized.answer.strip():
                    detail_enriched_count += 1
            except URLError as exc:
                detail_fail_count += 1
                print(
                    f"[A][WARN] 상세 페이지 수집 실패 (source_id={normalized.source_id}): {exc}"
                )
            dedup_key = normalized.source_id or normalized.raw_json
            if dedup_key in seen_ids:
                continue
            seen_ids.add(dedup_key)
            results.append(normalized)
            new_count += 1

        print(f"[A] page {page}: 신규 {new_count}건, 누적 {len(results)}건")

        if new_count == 0:
            break

        total_pages = metadata.get("total_pages") if isinstance(
            metadata, dict) else None
        total_count = metadata.get("total_count") if isinstance(
            metadata, dict) else None
        if isinstance(total_pages, int) and page >= total_pages:
            break
        if isinstance(total_count, int) and len(results) >= total_count:
            break

        page += 1
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    print(
        f"[A] 상세 본문 채움 {detail_enriched_count}건, 상세 수집 실패 {detail_fail_count}건"
    )
    return results


def save_a_to_csv(path: str, items: list[FaqItem]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=A_CSV_FIELDS)
        writer.writeheader()
        for item in items:
            writer.writerow(item.__dict__)


def save_a_to_sqlite(path: str, items: list[FaqItem]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS faq_a (
                source_id TEXT,
                category TEXT,
                title TEXT,
                question TEXT,
                answer TEXT,
                created_at TEXT,
                updated_at TEXT,
                raw_json TEXT,
                collected_at TEXT
            )
            """
        )
        conn.execute("DELETE FROM faq_a")
        now = datetime.utcnow().isoformat()
        conn.executemany(
            """
            INSERT INTO faq_a (
                source_id, category, title, question, answer,
                created_at, updated_at, raw_json, collected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    x.source_id,
                    x.category,
                    x.title,
                    x.question,
                    x.answer,
                    x.created_at,
                    x.updated_at,
                    x.raw_json,
                    now,
                )
                for x in items
            ],
        )
        conn.commit()
    finally:
        conn.close()


def build_transform_prompt(item: FaqItem) -> str:
    return (
        "[원본 FAQ]\n"
        f"카테고리: {item.category}\n"
        f"제목: {item.title}\n"
        f"질문: {item.question}\n"
        f"답변: {item.answer}\n\n"
        "[작업]\n"
        "원문의 의미와 사실은 유지하고 문장만 다듬어 주세요.\n"
        "출력은 JSON 객체 하나만 작성하세요."
    )


def normalize_transform_payload(
        payload: dict[str, Any], item: FaqItem) -> dict[str, str]:
    question = str(payload.get("question") or "").strip(
    ) or item.question or item.title
    answer = str(payload.get("answer") or "").strip()
    status = str(payload.get("status") or "").strip()
    reason = str(payload.get("reason") or "").strip()

    if status not in {"ok", "insufficient_source"}:
        raise ValueError(f"unexpected status: {status}")
    if status == "ok" and not answer:
        raise ValueError("status=ok requires non-empty answer")
    if status == "insufficient_source":
        reason = reason or "insufficient_source"
        answer = answer or "원문 답변이 없어 안전하게 변환하지 않았어요."

    return {
        "question": question,
        "answer": answer,
        "status": status,
        "reason": reason,
    }


def parse_transform_response(text: str, item: FaqItem) -> dict[str, str]:
    raw = text.strip()
    if not raw:
        raise ValueError("empty model response")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("response is not valid JSON object") from None
        payload = json.loads(raw[start: end + 1])

    if not isinstance(payload, dict):
        raise ValueError("response JSON must be an object")

    return normalize_transform_payload(payload, item)


def build_insufficient_payload(item: FaqItem, reason: str) -> dict[str, str]:
    return {
        "question": item.question or item.title,
        "answer": "원문 답변이 없어 안전하게 변환하지 않았어요.",
        "status": "insufficient_source",
        "reason": reason,
    }


def render_transformed_text(payload: dict[str, str]) -> str:
    return f"질문: {payload['question']}\n답변: {payload['answer']}"


def build_transformed_item(
    item: FaqItem,
    payload: dict[str, str],
    model: str,
    instruction: str,
) -> FaqTransformed:
    return FaqTransformed(
        source_id=item.source_id,
        original_title=item.title,
        original_question=item.question,
        original_answer=item.answer,
        transformed_text=render_transformed_text(payload),
        model=model,
        prompt_instruction=instruction,
    )


def request_openai_json_response(
    api_key: str,
    model: str,
    instruction: str,
    prompt: str,
    connect_timeout_sec: float,
    read_timeout_sec: float,
    max_retries: int,
) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt},
        ],
    }

    last_reason = "api_error:unknown"
    total_attempts = max(1, max_retries + 1)
    # 재시도 가능한 오류(네트워크 지연/서버 과부하)만 backoff 후 반복한다.
    for attempt in range(1, total_attempts + 1):
        try:
            response = requests.post(
                DEFAULT_OPENAI_API_URL,
                headers=headers,
                data=json.dumps(body),
                timeout=(connect_timeout_sec, read_timeout_sec),
            )
        except requests.Timeout:
            last_reason = "api_error:Timeout"
        except requests.RequestException as exc:
            last_reason = f"api_error:{type(exc).__name__}"
        else:
            if response.status_code == 200:
                try:
                    payload = response.json()
                except json.JSONDecodeError as exc:
                    raise OpenAIRequestError(
                        "api_error:InvalidJSONResponse") from exc
                message = (
                    payload.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
                if not isinstance(message, str) or not message.strip():
                    raise OpenAIRequestError("api_error:EmptyResponse")
                return message

            status_reason = f"api_status_{response.status_code}"
            if response.status_code in RETRYABLE_STATUS_CODES:
                last_reason = status_reason
            else:
                raise OpenAIRequestError(status_reason)

        if attempt < total_attempts:
            time.sleep(min(8.0, 1.5 * attempt))

    raise OpenAIRequestError(last_reason)


def transform_with_openai(
    items: list[FaqItem],
    api_key: str,
    model: str,
    instruction: str,
    request_interval_sec: float,
    connect_timeout_sec: float,
    read_timeout_sec: float,
    max_retries: int,
    progress_step: int,
) -> list[FaqTransformed]:
    transformed: list[FaqTransformed] = []
    total = len(items)
    ok_count = 0
    insufficient_count = 0

    for index, item in enumerate(items, start=1):
        if not item.answer.strip():
            payload = build_insufficient_payload(item, reason="answer_empty")
        else:
            prompt = build_transform_prompt(item)
            try:
                raw_text = request_openai_json_response(
                    api_key=api_key,
                    model=model,
                    instruction=instruction,
                    prompt=prompt,
                    connect_timeout_sec=connect_timeout_sec,
                    read_timeout_sec=read_timeout_sec,
                    max_retries=max_retries,
                )
                payload = parse_transform_response(raw_text, item)
            except OpenAIRequestError as exc:
                payload = build_insufficient_payload(item, reason=exc.reason)
            except ValueError:
                payload = build_insufficient_payload(
                    item, reason="invalid_response_format")

        transformed.append(
            build_transformed_item(
                item=item,
                payload=payload,
                model=model,
                instruction=instruction,
            )
        )

        if payload.get("status") == "ok":
            ok_count += 1
        else:
            insufficient_count += 1

        if progress_step > 0 and (index % progress_step == 0 or index == total):
            print(
                f"[B] progress {index}/{total} | ok={ok_count} "
                f"insufficient={insufficient_count}"
            )

        if request_interval_sec > 0:
            time.sleep(request_interval_sec)

    return transformed


def save_b_to_csv(path: str, items: list[FaqTransformed]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=B_CSV_FIELDS)
        writer.writeheader()
        for item in items:
            writer.writerow(item.__dict__)


def save_b_to_sqlite(path: str, items: list[FaqTransformed]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS faq_b (
                source_id TEXT,
                original_title TEXT,
                original_question TEXT,
                original_answer TEXT,
                transformed_text TEXT,
                model TEXT,
                prompt_instruction TEXT,
                transformed_at TEXT
            )
            """
        )
        conn.execute("DELETE FROM faq_b")
        now = datetime.utcnow().isoformat()
        conn.executemany(
            """
            INSERT INTO faq_b (
                source_id, original_title, original_question, original_answer,
                transformed_text, model, prompt_instruction, transformed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    x.source_id,
                    x.original_title,
                    x.original_question,
                    x.original_answer,
                    x.transformed_text,
                    x.model,
                    x.prompt_instruction,
                    now,
                )
                for x in items
            ],
        )
        conn.commit()
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Firstmall FAQ 수집 + OpenAI Toss체 변환 파이프라인")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--detail-url-template", default=DEFAULT_DETAIL_URL_TEMPLATE)
    parser.add_argument("--per-page", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (compatible; tossify-py/1.0)")
    parser.add_argument("--sleep-sec", type=float, default=0.0)

    parser.add_argument("--a-csv", default="data/faq_a.csv")
    parser.add_argument("--a-db", default="data/faq.db")

    parser.add_argument(
        "--openai-api-key",
        "--gemini-api-key",
        dest="openai_api_key",
        default=os.getenv("OPENAI_API_KEY", ""),
    )
    parser.add_argument(
        "--openai-model",
        "--gemini-model",
        dest="openai_model",
        default="gpt-4o-mini",
    )
    parser.add_argument(
        "--style-instruction",
        default=DEFAULT_STYLE_INSTRUCTION)
    parser.add_argument(
        "--openai-interval-sec",
        "--gemini-interval-sec",
        dest="openai_interval_sec",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--openai-connect-timeout-sec",
        type=float,
        default=8.0,
    )
    parser.add_argument(
        "--openai-read-timeout-sec",
        type=float,
        default=25.0,
    )
    parser.add_argument(
        "--openai-max-retries",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--openai-progress-step",
        type=int,
        default=50,
    )
    parser.add_argument("--skip-transform", action="store_true")

    parser.add_argument("--b-csv", default="data/faq_b.csv")
    parser.add_argument("--b-db", default="data/faq.db")
    return parser.parse_args()


def validate_transform_args(args: argparse.Namespace) -> None:
    if args.openai_interval_sec < 0:
        raise ValueError("--openai-interval-sec는 0 이상이어야 합니다.")
    if args.openai_connect_timeout_sec <= 0:
        raise ValueError("--openai-connect-timeout-sec는 0보다 커야 합니다.")
    if args.openai_read_timeout_sec <= 0:
        raise ValueError("--openai-read-timeout-sec는 0보다 커야 합니다.")
    if args.openai_max_retries < 0:
        raise ValueError("--openai-max-retries는 0 이상이어야 합니다.")
    if args.openai_progress_step < 0:
        raise ValueError("--openai-progress-step은 0 이상이어야 합니다.")


def main() -> None:
    args = parse_args()
    validate_transform_args(args)

    try:
        faq_a = collect_all_faq(
            base_url=args.base_url,
            detail_url_template=args.detail_url_template,
            per_page=args.per_page,
            max_pages=args.max_pages,
            timeout=args.timeout,
            user_agent=args.user_agent,
            sleep_sec=args.sleep_sec,
        )
    except URLError as exc:
        raise RuntimeError(
            "Firstmall FAQ 수집 요청에 실패했습니다. 네트워크 정책 또는 대상 서버 차단(예: 403) 여부를 확인하세요."
        ) from exc
    save_a_to_csv(args.a_csv, faq_a)
    save_a_to_sqlite(args.a_db, faq_a)
    print(f"[A] 수집 완료: {len(faq_a)}건")
    print(f"[A] CSV 저장: {args.a_csv}")
    print(f"[A] DB 저장: {args.a_db} (table=faq_a)")

    if args.skip_transform:
        print("[B] OpenAI 변환을 건너뜁니다 (--skip-transform)")
        return

    if not args.openai_api_key:
        raise ValueError(
            "OpenAI 변환을 수행하려면 --openai-api-key 또는 OPENAI_API_KEY가 필요합니다.")

    faq_b = transform_with_openai(
        items=faq_a,
        api_key=args.openai_api_key,
        model=args.openai_model,
        instruction=args.style_instruction,
        request_interval_sec=args.openai_interval_sec,
        connect_timeout_sec=args.openai_connect_timeout_sec,
        read_timeout_sec=args.openai_read_timeout_sec,
        max_retries=args.openai_max_retries,
        progress_step=args.openai_progress_step,
    )
    save_b_to_csv(args.b_csv, faq_b)
    save_b_to_sqlite(args.b_db, faq_b)
    print(f"[B] 변환 완료: {len(faq_b)}건")
    print(f"[B] CSV 저장: {args.b_csv}")
    print(f"[B] DB 저장: {args.b_db} (table=faq_b)")


if __name__ == "__main__":
    main()
