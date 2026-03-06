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
    """원본 FAQ(A) 저장에 사용하는 정규화된 FAQ 레코드다.

    목록 API와 상세 페이지에서 가져온 값을 한 구조로 모아 CSV/SQLite 저장과
    후속 문체 변환 단계에서 공통으로 사용할 수 있게 한다.
    """

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
    """OpenAI 변환 결과 FAQ(B)를 저장하는 레코드다.

    원문 제목/질문/답변과 함께 최종 변환 텍스트, 사용 모델, 지시어를 보관해
    결과 추적과 재현이 가능하도록 한다.
    """

    source_id: str
    original_title: str
    original_question: str
    original_answer: str
    transformed_text: str
    model: str
    prompt_instruction: str


class OpenAIRequestError(RuntimeError):
    """OpenAI 호출 실패 사유를 상위 흐름으로 전달하는 예외다."""

    def __init__(self, reason: str) -> None:
        """실패 원인을 예외 메시지와 속성으로 함께 저장한다.

        Args:
            reason: 재시도 종료 후 호출자가 그대로 기록하거나 fallback 처리에
                사용할 축약 사유 문자열이다.
        """

        super().__init__(reason)
        self.reason = reason


def build_headers(user_agent: str) -> dict[str, str]:
    """FAQ 수집 요청에 공통으로 사용할 HTTP 헤더를 생성한다.

    Args:
        user_agent: 대상 서버가 요청 출처를 식별할 수 있도록 전달할
            사용자 에이전트 문자열이다.

    Returns:
        JSON 응답 요청에 필요한 최소 헤더 사전이다.
    """

    return {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
    }


def http_get_json(
        url: str, params: dict[str, Any], headers: dict[str, str], timeout: int) -> Any:
    """GET 요청으로 JSON 응답을 받아 파이썬 객체로 역직렬화한다.

    Args:
        url: 호출할 FAQ 목록 API 주소다.
        params: 쿼리스트링으로 붙일 요청 파라미터다.
        headers: 요청 시 함께 보낼 HTTP 헤더다.
        timeout: `urlopen`에 전달할 전체 타임아웃(초)이다.

    Returns:
        JSON 본문을 `json.loads`로 파싱한 파이썬 객체다.
    """

    query = urlencode(params)
    request = Request(f"{url}?{query}", headers=headers, method="GET")
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def http_get_text(url: str, headers: dict[str, str], timeout: int) -> str:
    """GET 요청으로 HTML 같은 텍스트 본문을 내려받는다.

    상세 FAQ 페이지는 JSON이 아닌 HTML을 반환하므로, 인코딩 오류가 있더라도
    수집을 이어갈 수 있게 `ignore` 옵션으로 문자열을 복원한다.

    Args:
        url: 호출할 상세 페이지 주소다.
        headers: 요청 시 사용할 HTTP 헤더다.
        timeout: `urlopen` 타임아웃(초)이다.

    Returns:
        디코딩된 응답 본문 문자열이다.
    """

    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", "ignore")


def extract_items(
        response_json: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """FAQ 목록 응답에서 실제 게시물 배열과 메타데이터를 분리한다.

    대상 API는 환경이나 버전에 따라 리스트를 바로 반환하거나, `items`,
    `data`, `list` 등 여러 키 아래에 목록을 담을 수 있다. 이 함수는 흔한
    패턴을 순차적으로 탐색해 목록과 원본 메타데이터를 추출한다.

    Args:
        response_json: FAQ 검색 API가 반환한 역직렬화 JSON 객체다.

    Returns:
        `(items, metadata)` 튜플이다. 목록을 찾지 못하면 빈 리스트를 반환하고,
        메타데이터는 이후 페이지 종료 조건 판단에 그대로 사용한다.
    """

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
    """원본 API 항목을 `FaqItem` 구조로 정규화한다.

    서로 다른 키 이름(`id`, `seq`, `faq_id`, `subject`, `contents` 등)을
    폭넓게 허용해 API 응답 형태가 조금 달라도 파이프라인이 동작하도록 한다.

    Args:
        item: 목록 API의 FAQ 한 건 원본 dict다.

    Returns:
        저장 및 후처리에 바로 사용할 수 있는 `FaqItem` 인스턴스다.
    """

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
    """HTML 조각을 사람이 읽기 좋은 일반 텍스트로 정리한다.

    스크립트/스타일 태그를 제거하고, 줄바꿈 의미가 있는 태그를 개행으로 치환한
    뒤 나머지 태그를 벗겨낸다. 마지막에는 HTML 엔티티와 공백을 정리해 저장용
    본문으로 쓸 수 있게 만든다.

    Args:
        html_fragment: 상세 페이지에서 잘라낸 HTML 일부다.

    Returns:
        불필요한 태그와 공백이 정리된 텍스트다.
    """

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
    """상세 FAQ HTML에서 카테고리, 제목, 답변 본문을 추출한다.

    우선 FAQ 본문이 포함된 `customer_view` 블록을 좁게 찾고, 그 안에서
    카테고리/제목/내용 영역을 정규식으로 추출한다. 추출된 HTML은 다시
    `html_fragment_to_text`로 정리해 최종 텍스트 값을 만든다.

    Args:
        detail_html: 상세 FAQ 페이지 전체 HTML 문자열이다.

    Returns:
        `(category, title, answer)` 튜플이다. 각 값은 추출 실패 시 빈 문자열이
        될 수 있다.
    """

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
    """상세 페이지 URL 템플릿에 FAQ 식별자를 안전하게 주입한다.

    템플릿이 `{source_id}` 외에 `{seq}` 또는 `{id}` 같은 플레이스홀더를
    사용할 수 있어 세 이름을 모두 지원한다. 포맷 키가 일부 다르더라도 기본
    치환으로 최대한 URL 생성을 이어간다.

    Args:
        detail_url_template: 상세 URL 템플릿 문자열이다.
        source_id: FAQ 식별자다.

    Returns:
        실제 호출 가능한 상세 페이지 URL이다.
    """

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
    """목록 API 항목에 상세 페이지 본문을 합쳐 더 풍부한 FAQ로 만든다.

    목록 응답에는 답변 전문이 없거나 제목 정보가 축약되어 있을 수 있다. 이 함수는
    상세 URL을 호출해 더 정확한 카테고리/제목/답변을 가져오고, 어떤 상세 URL을
    사용했는지도 `raw_json`에 함께 남긴다.

    Args:
        item: 목록 API에서 정규화한 기본 FAQ 항목이다.
        detail_url_template: 상세 페이지 URL 템플릿이다.
        headers: 상세 요청용 HTTP 헤더다.
        timeout: 상세 페이지 요청 타임아웃(초)이다.

    Returns:
        상세 페이지 정보로 보강된 새 `FaqItem`이다. `source_id`가 없으면 원본을
        그대로 반환한다.
    """

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
    """FAQ 목록 API를 끝까지 순회하며 전체 FAQ 원본 데이터를 수집한다.

    각 페이지의 목록 응답을 읽은 뒤 항목을 정규화하고, 가능하면 상세 페이지까지
    따라가 답변 본문을 채운다. 중복 식별자는 제거하고, 메타데이터의 전체 페이지 수
    또는 전체 건수 정보가 있으면 이를 종료 조건으로 활용한다.

    Args:
        base_url: FAQ 검색 API 주소다.
        detail_url_template: FAQ 상세 페이지 URL 템플릿이다.
        per_page: 페이지당 조회 건수다.
        max_pages: 최대 조회 페이지 수다. `None`이면 끝까지 순회한다.
        timeout: 목록/상세 요청에 공통 적용할 타임아웃(초)이다.
        user_agent: HTTP 요청 시 사용할 User-Agent 문자열이다.
        sleep_sec: 페이지 간 대기 시간(초)이다.

    Returns:
        중복 제거와 상세 보강이 적용된 `FaqItem` 목록이다.
    """

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
    """원본 FAQ(A) 목록을 CSV 파일로 저장한다.

    저장 전 상위 디렉터리를 자동 생성하고, Excel 호환성을 위해 UTF-8 BOM이 있는
    `utf-8-sig` 인코딩을 사용한다.

    Args:
        path: 생성할 CSV 파일 경로다.
        items: 저장할 원본 FAQ 목록이다.
    """

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=A_CSV_FIELDS)
        writer.writeheader()
        for item in items:
            writer.writerow(item.__dict__)


def save_a_to_sqlite(path: str, items: list[FaqItem]) -> None:
    """원본 FAQ(A) 목록을 SQLite `faq_a` 테이블에 저장한다.

    테이블이 없으면 먼저 생성하고, 이번 실행 결과만 남도록 기존 데이터를 지운 뒤
    현재 UTC 시각과 함께 일괄 삽입한다.

    Args:
        path: SQLite 파일 경로다.
        items: 저장할 원본 FAQ 목록이다.
    """

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
    """OpenAI에 전달할 FAQ 문체 변환용 사용자 프롬프트를 만든다.

    모델이 원문 맥락을 놓치지 않도록 카테고리, 제목, 질문, 답변을 모두 포함하고,
    출력 형식은 JSON 객체 하나로 제한하도록 지시를 덧붙인다.

    Args:
        item: 변환할 원본 FAQ 항목이다.

    Returns:
        Chat Completions API의 user 메시지로 넣을 문자열이다.
    """

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
    """모델 응답 payload를 검증하고 내부 표준 구조로 정리한다.

    질문/답변/status/reason 필드를 문자열로 맞추고, 필수 값이 없거나 허용되지 않은
    상태값이 오면 예외를 발생시킨다. `insufficient_source`는 사용자에게 보여줄
    안전한 기본 문구를 채워 넣는다.

    Args:
        payload: 모델이 반환한 JSON 객체다.
        item: 기본값 보정에 사용할 원본 FAQ다.

    Returns:
        `question`, `answer`, `status`, `reason`만 포함한 표준 dict다.
    """

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
    """모델의 텍스트 응답에서 JSON 객체를 파싱해 검증한다.

    정상적인 JSON 문자열뿐 아니라 모델이 앞뒤 설명 문구를 섞어 보낸 경우도
    대비해 첫 `{` 와 마지막 `}` 사이를 다시 파싱해본다.

    Args:
        text: 모델이 반환한 원시 텍스트다.
        item: payload 보정/검증에 사용할 원본 FAQ다.

    Returns:
        내부 표준 구조로 정규화된 변환 결과 dict다.
    """

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
    """변환을 수행할 수 없을 때 사용할 안전한 fallback payload를 만든다.

    원문 답변이 비어 있거나 API/파싱 오류가 발생했을 때도 후속 저장 포맷은 유지해야
    하므로, 일관된 `insufficient_source` 응답 구조를 생성한다.

    Args:
        item: 질문 기본값을 가져올 원본 FAQ다.
        reason: 변환 실패 또는 생략 사유 코드다.

    Returns:
        저장 가능한 fallback payload dict다.
    """

    return {
        "question": item.question or item.title,
        "answer": "원문 답변이 없어 안전하게 변환하지 않았어요.",
        "status": "insufficient_source",
        "reason": reason,
    }


def render_transformed_text(payload: dict[str, str]) -> str:
    """정규화된 변환 payload를 최종 저장용 문자열로 렌더링한다.

    Args:
        payload: 검증을 마친 변환 결과 dict다.

    Returns:
        뷰어와 CSV에서 바로 읽을 수 있는 `질문/답변` 2줄 포맷 문자열이다.
    """

    return f"질문: {payload['question']}\n답변: {payload['answer']}"


def build_transformed_item(
    item: FaqItem,
    payload: dict[str, str],
    model: str,
    instruction: str,
) -> FaqTransformed:
    """원본 FAQ와 변환 payload를 합쳐 `FaqTransformed` 레코드를 만든다.

    Args:
        item: 원본 FAQ 항목이다.
        payload: 검증이 끝난 변환 결과 dict다.
        model: 변환에 사용한 모델명이다.
        instruction: 시스템 지시어 원문이다.

    Returns:
        저장용 `FaqTransformed` 인스턴스다.
    """

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
    """OpenAI Chat Completions API를 호출해 JSON 텍스트 응답을 받는다.

    네트워크 타임아웃이나 일시적 서버 오류는 설정된 횟수만큼 backoff 후 재시도하고,
    재시도가 무의미한 상태 코드는 즉시 `OpenAIRequestError`로 올린다.

    Args:
        api_key: OpenAI API 키다.
        model: 호출할 모델명이다.
        instruction: system 메시지로 전달할 문체 변환 지시어다.
        prompt: user 메시지로 전달할 원본 FAQ 프롬프트다.
        connect_timeout_sec: 연결 타임아웃(초)이다.
        read_timeout_sec: 응답 읽기 타임아웃(초)이다.
        max_retries: 실패 후 재시도 횟수다.

    Returns:
        모델 메시지의 `content` 문자열이다.
    """

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
    """원본 FAQ 목록 전체를 순회하며 OpenAI 문체 변환 결과를 만든다.

    원문 답변이 없는 항목은 API 호출 없이 바로 보류 처리하고, 나머지는 모델 호출 후
    응답을 검증해 `FaqTransformed`로 변환한다. 진행률과 성공/보류 건수를 주기적으로
    출력하며, 요청 간 간격이 설정되어 있으면 호출 사이에 대기한다.

    Args:
        items: 변환할 원본 FAQ 목록이다.
        api_key: OpenAI API 키다.
        model: 사용할 모델명이다.
        instruction: 시스템 지시어다.
        request_interval_sec: 각 API 호출 사이 대기 시간(초)이다.
        connect_timeout_sec: 연결 타임아웃(초)이다.
        read_timeout_sec: 응답 읽기 타임아웃(초)이다.
        max_retries: 재시도 횟수다.
        progress_step: 몇 건마다 진행률을 출력할지 나타낸다. 0이면 출력하지 않는다.

    Returns:
        저장 가능한 `FaqTransformed` 목록이다.
    """

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
    """변환 FAQ(B) 목록을 CSV 파일로 저장한다.

    Args:
        path: 생성할 CSV 경로다.
        items: 저장할 변환 FAQ 목록이다.
    """

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=B_CSV_FIELDS)
        writer.writeheader()
        for item in items:
            writer.writerow(item.__dict__)


def save_b_to_sqlite(path: str, items: list[FaqTransformed]) -> None:
    """변환 FAQ(B) 목록을 SQLite `faq_b` 테이블에 저장한다.

    기존 실행 결과를 덮어쓰는 방식으로 테이블을 비운 뒤, 변환 시각과 함께 새 데이터를
    일괄 삽입한다.

    Args:
        path: SQLite 파일 경로다.
        items: 저장할 변환 FAQ 목록이다.
    """

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
    """CLI 실행 인자를 정의하고 파싱한다.

    수집 URL, 저장 경로, OpenAI 설정, 변환 건너뛰기 여부 등 파이프라인 전체를
    제어하는 옵션을 한곳에서 관리한다.

    Returns:
        사용자가 전달한 인자가 반영된 `argparse.Namespace` 객체다.
    """

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
    """OpenAI 변환 관련 인자가 유효한 범위인지 검사한다.

    Args:
        args: `parse_args`가 반환한 CLI 인자 객체다.

    Raises:
        ValueError: 음수 간격, 0 이하 타임아웃 등 잘못된 값이 들어온 경우 발생한다.
    """

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
    """FAQ 수집, 저장, OpenAI 변환, 결과 저장을 순서대로 실행한다.

    전체 CLI 파이프라인의 진입점으로서 인자를 읽고, A 데이터 수집/저장 후 필요하면
    B 데이터 변환/저장까지 수행한다. 네트워크 오류와 필수 인자 누락은 사용자가 바로
    조치할 수 있도록 설명적인 예외 메시지로 바꿔 전달한다.
    """

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
