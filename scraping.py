import argparse
import csv
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen



DEFAULT_BASE_URL = "https://www.firstmall.kr/customer/faq/search"
DEFAULT_STYLE_INSTRUCTION = """이름: 토스(Toss) 수석 UX 라이터
설명: 복잡하고 딱딱한 쇼핑몰 FAQ를 토스의 'Simplicity' 원칙에 따라 친근하고 명확한 대화체로 바꿔주는 전문가입니다.
요청사항:
Role: 대한민국 최고 핀테크 '토스(Toss)'의 수석 UX 라이터

Objective:
입력된 쇼핑몰 FAQ 데이터를 토스의 디자인 원칙인 'Simplicity(단순함)'와 'Toss Voice'에 따라 재작성합니다. 사용자에게 가장 명확하고 친근한 경험을 제공하는 것을 최우선으로 합니다.

Core Principles:
Tone & Manner: 친근함, 정중함, 단호함
- ~해요, ~이에요, ~인가요 등 대화형 구어체 사용.
- 극존칭(성함, 함자 등) 및 불필요한 사과 문구(양해 부탁드립니다) 제거.
- 자신감 있는 문장 맺음.

Vocabulary: 쉬운 우리말 및 능동태
- 한자어 탈피: 익일 → 내일, 수령 → 받기, 유선 문의 → 전화 문의, 오기재 → 잘못 입력.
- 능동태 전환: 확인됩니다 → 확인할 수 있어요, 진행됩니다 → 시작해요.
- 사용자 중심 서술: '시스템에서 ~됩니다' 대신 '고객님이 ~할 수 있어요'로 작성.

Structure: 두괄식 및 간결함
- 가장 중요한 결론을 문장 맨 앞에 배치.
- 한 문장은 최대 두 줄을 넘기지 않으며, 불필요한 접속사(그리고, 따라서 등) 생략.
- 나열형 정보는 반드시 개조식(Bullet points)으로 정리.

Constraints:
- 출력 시 설명, 서론, 사족을 전면 배제합니다.
- 오직 변환된 최종 결과물만 출력합니다.
- 이모지 사용을 금지하며, 텍스트의 가독성과 명확성에 집중합니다.

Reference Examples:
- 배송: "오후 2시까지 결제하면 오늘 출발해요. 그 이후에는 내일 보내드릴게요."
- 회원 정보: "비밀번호를 잊으셨나요? 로그인 화면 아래 [비밀번호 찾기]를 눌러 새로 설정해 보세요."
- 시스템 오류: "지금은 잠깐 이용할 수 없어요. 잠시 후에 다시 시도해 주세요."""


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


def build_headers(user_agent: str) -> dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
    }


def http_get_json(url: str, params: dict[str, Any], headers: dict[str, str], timeout: int) -> Any:
    query = urlencode(params)
    request = Request(f"{url}?{query}", headers=headers, method="GET")
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def extract_items(response_json: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
    title = str(item.get("title") or item.get("subject") or item.get("question") or "")
    question = str(item.get("question") or item.get("title") or item.get("subject") or "")
    answer = str(item.get("answer") or item.get("contents") or item.get("content") or "")
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


def collect_all_faq(
    base_url: str,
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

        response_json = http_get_json(base_url, payload, headers=headers, timeout=timeout)
        raw_items, metadata = extract_items(response_json)

        if not raw_items:
            break

        new_count = 0
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            normalized = normalize_item(item)
            dedup_key = normalized.source_id or normalized.raw_json
            if dedup_key in seen_ids:
                continue
            seen_ids.add(dedup_key)
            results.append(normalized)
            new_count += 1

        if new_count == 0:
            break

        total_pages = metadata.get("total_pages") if isinstance(metadata, dict) else None
        total_count = metadata.get("total_count") if isinstance(metadata, dict) else None
        if isinstance(total_pages, int) and page >= total_pages:
            break
        if isinstance(total_count, int) and len(results) >= total_count:
            break

        page += 1
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    return results


def save_a_to_csv(path: str, items: list[FaqItem]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "source_id",
                "category",
                "title",
                "question",
                "answer",
                "created_at",
                "updated_at",
                "raw_json",
            ],
        )
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


def build_transform_prompt(item: FaqItem, instruction: str) -> str:
    return (
        f"[지시]\n{instruction}\n\n"
        "[원본 FAQ]\n"
        f"카테고리: {item.category}\n"
        f"제목: {item.title}\n"
        f"질문: {item.question}\n"
        f"답변: {item.answer}\n\n"
        "[출력 형식]\n"
        "질문: ...\n답변: ..."
    )


def transform_with_gemini(
    items: list[FaqItem],
    api_key: str,
    model: str,
    instruction: str,
    request_interval_sec: float,
) -> list[FaqTransformed]:
    from google import genai

    client = genai.Client(api_key=api_key)
    transformed: list[FaqTransformed] = []

    for item in items:
        prompt = build_transform_prompt(item, instruction)
        response = client.models.generate_content(model=model, contents=prompt)
        text = response.text or ""
        transformed.append(
            FaqTransformed(
                source_id=item.source_id,
                original_title=item.title,
                original_question=item.question,
                original_answer=item.answer,
                transformed_text=text,
                model=model,
                prompt_instruction=instruction,
            )
        )
        if request_interval_sec > 0:
            time.sleep(request_interval_sec)

    return transformed


def save_b_to_csv(path: str, items: list[FaqTransformed]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "source_id",
                "original_title",
                "original_question",
                "original_answer",
                "transformed_text",
                "model",
                "prompt_instruction",
            ],
        )
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
    parser = argparse.ArgumentParser(description="Firstmall FAQ 수집 + Gemini Toss체 변환 파이프라인")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--per-page", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--user-agent", default="Mozilla/5.0 (compatible; tossify-py/1.0)")
    parser.add_argument("--sleep-sec", type=float, default=0.0)

    parser.add_argument("--a-csv", default="data/faq_a.csv")
    parser.add_argument("--a-db", default="data/faq.db")

    parser.add_argument("--gemini-api-key", default=os.getenv("GEMINI_API_KEY", ""))
    parser.add_argument("--gemini-model", default="gemini-2.0-flash")
    parser.add_argument("--style-instruction", default=DEFAULT_STYLE_INSTRUCTION)
    parser.add_argument("--gemini-interval-sec", type=float, default=0.0)
    parser.add_argument("--skip-transform", action="store_true")

    parser.add_argument("--b-csv", default="data/faq_b.csv")
    parser.add_argument("--b-db", default="data/faq.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        faq_a = collect_all_faq(
            base_url=args.base_url,
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
        print("[B] Gemini 변환을 건너뜁니다 (--skip-transform)")
        return

    if not args.gemini_api_key:
        raise ValueError("Gemini 변환을 수행하려면 --gemini-api-key 또는 GEMINI_API_KEY가 필요합니다.")

    faq_b = transform_with_gemini(
        items=faq_a,
        api_key=args.gemini_api_key,
        model=args.gemini_model,
        instruction=args.style_instruction,
        request_interval_sec=args.gemini_interval_sec,
    )
    save_b_to_csv(args.b_csv, faq_b)
    save_b_to_sqlite(args.b_db, faq_b)
    print(f"[B] 변환 완료: {len(faq_b)}건")
    print(f"[B] CSV 저장: {args.b_csv}")
    print(f"[B] DB 저장: {args.b_db} (table=faq_b)")


if __name__ == "__main__":
    main()
