# tossify-py

퍼스트몰 FAQ를 수집해 원본 데이터(A)로 저장하고, OpenAI API로 문체를 변환한 FAQ(B)를 저장하는 파이프라인입니다.

## 1. 로컬 환경 구성 방법

### 1) Python 설치

Python `3.12+`를 권장합니다.

- macOS (Homebrew)
```bash
brew install python@3.12
```

- Ubuntu/Debian
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
```

- Windows (PowerShell)
```powershell
winget install -e --id Python.Python.3.12
```

버전 확인:

```bash
python3 --version
```

### 2) 가상환경 생성/활성화

```bash
python3 -m venv venv
source venv/bin/activate
```

Windows PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3) 의존성 설치

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4) 설정 파일 관리

`.env.example`을 복사한 뒤 OpenAI 키를 설정합니다.

```bash
cp .env.example .env
```

`.env` 예시:

```bash
OPENAI_API_KEY=your_openai_api_key
```

기본 실행 예시:

```bash
python scraping.py \
  --detail-url-template "https://www.firstmall.kr/customer/faq/{source_id}" \
  --a-csv data/faq_a.csv \
  --a-db data/faq.db \
  --b-csv data/faq_b.csv \
  --b-db data/faq.db \
  --openai-api-key "$OPENAI_API_KEY"
```

수집만 실행(변환 생략):

```bash
python scraping.py --skip-transform
```

변환 안정성 옵션 예시:

```bash
python scraping.py \
  --openai-api-key "$OPENAI_API_KEY" \
  --openai-connect-timeout-sec 8 \
  --openai-read-timeout-sec 25 \
  --openai-max-retries 2 \
  --openai-progress-step 50
```

## 2. 서비스 구조

| 구성요소 | 역할 | 주요 입력 | 주요 출력 |
|---|---|---|---|
| `scraping.py` | CLI 진입점, 수집/변환/저장 파이프라인 실행 | 실행 인자, 환경변수 | A/B 데이터 |
| `collect_all_faq` | FAQ 목록 조회 + 상세 페이지 본문 추출 | `base_url`, `detail_url_template`, `per_page` | `FaqItem[]` |
| `save_a_to_csv` / `save_a_to_sqlite` | 원본 FAQ(A) 저장 | `FaqItem[]` | `data/faq_a.csv`, `faq_a` |
| `transform_with_openai` | OpenAI 호출, 재시도/타임아웃 처리, JSON 파싱 | `FaqItem[]`, API 키, 모델, 지시어 | `FaqTransformed[]` |
| `save_b_to_csv` / `save_b_to_sqlite` | 변환 FAQ(B) 저장 | `FaqTransformed[]` | `data/faq_b.csv`, `faq_b` |
| `data/faq.db` | A/B 통합 SQLite 저장소 | 저장 함수 호출 | `faq_a`, `faq_b` 테이블 |

## 3. 서비스 라이프 싸이클

1. 실행 인자와 `OPENAI_API_KEY`를 준비해 `scraping.py`를 실행합니다.
2. FAQ 목록 API에서 페이지 단위로 게시글 순번(`seq`)을 수집합니다.
3. 순번 기반 상세 URL(`.../faq/{source_id}`)에 접근해 질문/답변 본문을 추출합니다.
4. 원본 FAQ(A)를 CSV와 SQLite(`faq_a`)로 저장합니다.
5. `--skip-transform`이 아니면 OpenAI API로 FAQ를 변환합니다.
6. 변환 중 네트워크/서버 오류는 재시도하고, 실패 항목은 `insufficient_source`로 표시합니다.
7. 변환 FAQ(B)를 CSV와 SQLite(`faq_b`)로 저장합니다.
8. 실행 건수/진행률/저장 경로를 출력하고 종료합니다.

---

## 4. HTML 뷰어 사용 방법

별도 설치 없이 브라우저에서 `viewer.html`을 열어 `faq_b.csv`를 확인할 수 있습니다.

1. 브라우저에서 `viewer.html`을 엽니다.
2. `CSV 파일 선택` 또는 드래그 앤 드롭으로 `data/faq_b.csv`를 업로드합니다.
3. 검색/정렬/`보류건만 보기` 필터로 목록을 빠르게 좁힙니다.
4. 좌측 목록에서 게시글을 선택하면 우측 상세에서 원문/변환 결과를 확인할 수 있습니다.
5. `원문 전체 복사`, `변환 전체 복사`, 카드별 `복사` 버튼으로 즉시 복사할 수 있습니다.
6. 키보드 `↑` `↓`로 목록 이동이 가능합니다.

`file://`로 실행 시 자동 로드(`data/faq_b.csv`)가 차단될 수 있습니다. 자동 로드가 필요하면 프로젝트 루트에서 아래처럼 HTTP 서버를 띄워 접속하세요.

```bash
python -m http.server 8000
```

접속 주소:

```text
http://localhost:8000/viewer.html
```

서버 환경에서도 로드가 안 되면 브라우저 개발자 도구(Console) 오류를 확인해 원인을 점검하세요.

### 운영 메모

- 변환은 FAQ 건수만큼 OpenAI 호출이 발생하므로 쿼터/비용을 확인하세요.
- 퍼스트몰 API가 환경에 따라 403을 반환할 수 있어 `--user-agent` 조정이 필요할 수 있습니다.
- 원문 답변이 비어 있으면 안전 정책상 생성을 생략하고 `insufficient_source`로 저장합니다.
