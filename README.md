# tossify-py

퍼스트몰 FAQ를 수집해 원본 데이터(A)로 저장하고, OpenAI API로 문체를 변환한 FAQ(B)를 저장하는 파이프라인입니다.

## 1. 로컬 환경 구성 방법

### 1) Python 설치

Python `3.13+`를 권장합니다.

- macOS (Homebrew)
```bash
brew install python@3.13
```

- Ubuntu/Debian
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
```

- Windows (PowerShell)
```powershell
winget install -e --id Python.Python.3.13
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

중요:

- `scraping.py`는 현재 작업 디렉터리의 `.env`를 자동으로 읽습니다.
- `.env`에 `OPENAI_API_KEY`가 있으면 별도 `source .env` 없이 그대로 실행할 수 있습니다.
- `--openai-api-key`를 직접 넘기면 `.env`보다 CLI 인자가 우선합니다.

### 5) 가장 쉬운 실행 방법

macOS / Linux / Windows PowerShell 공통:

```bash
python scraping.py
```

위 명령은 아래 순서로 한 번에 실행됩니다.

1. 퍼스트몰 FAQ를 다시 수집합니다.
2. 원본 FAQ(A)를 `data/faq_a.csv`, `data/faq.db`에 저장합니다.
3. OpenAI로 토스체 변환을 수행합니다.
4. 변환 FAQ(B)를 `data/faq_b.csv`, `data/faq.db`에 저장합니다.

저장 동작 참고:

- `faq_a.csv`, `faq_b.csv`는 실행할 때마다 새 결과로 덮어씁니다.
- 기존 파일 뒤에 내용을 추가하는 방식이 아니므로, 실행 자체 때문에 CSV 중복이 누적되지는 않습니다.
- SQLite의 `faq_a`, `faq_b` 테이블도 기존 데이터를 비운 뒤 다시 저장합니다.
- 변환 중에는 기본적으로 `data/faq_b.csv.checkpoint.json` 체크포인트 파일을 함께 사용합니다.
- 변환이 길어지거나 중단되더라도, 다음 실행 시 체크포인트가 호환되면 이어서 재개합니다.

### 6) 자주 쓰는 실행 예시

기본 실행 예시(`--mode all`, 기본값):

```bash
python scraping.py \
  --detail-url-template "https://www.firstmall.kr/customer/faq/{source_id}" \
  --a-csv data/faq_a.csv \
  --a-db data/faq.db \
  --b-csv data/faq_b.csv \
  --b-db data/faq.db \
  --openai-api-key "$OPENAI_API_KEY"
```

`.env`를 사용 중이면 더 짧게 실행할 수 있습니다.

```bash
python scraping.py
```

수집만 실행하고 변환은 생략:

```bash
python scraping.py --mode collect
```

기존 `faq_a.csv`를 재사용해 변환만 다시 실행:

```bash
python scraping.py --mode transform
```

다른 원본 CSV를 입력으로 써서 변환만 실행:

```bash
python scraping.py \
  --mode transform \
  --a-csv data/faq_a.csv \
  --b-csv data/faq_b.csv
```

빠르게 점검할 때는 일부 페이지만 먼저 수집할 수 있습니다.

```bash
python scraping.py --mode collect --max-pages 1
```

변환 안정성 옵션 예시:

```bash
python scraping.py \
  --mode transform \
  --transform-save-every 10 \
  --openai-api-key "$OPENAI_API_KEY" \
  --openai-connect-timeout-sec 8 \
  --openai-read-timeout-sec 25 \
  --openai-max-retries 2 \
  --openai-progress-step 50
```

체크포인트를 명시적으로 지정해 변환 재개 가능하게 실행:

```bash
python scraping.py \
  --mode transform \
  --transform-save-every 5 \
  --transform-checkpoint data/faq_b.progress.json
```

CLI 옵션 전체 목록이 필요하면 아래 명령으로 확인할 수 있습니다.

```bash
python scraping.py --help
```

### 7) 주요 옵션 설명

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--mode` | `all` | 실행 모드입니다. `all`은 수집 후 변환, `collect`는 수집만, `transform`은 저장된 `faq_a.csv`를 읽어 변환만 수행합니다. |
| `--base-url` | `https://www.firstmall.kr/customer/faq/search` | FAQ 목록을 가져올 API 주소입니다. |
| `--detail-url-template` | `https://www.firstmall.kr/customer/faq/{source_id}` | FAQ 상세 페이지 주소 템플릿입니다. `{source_id}` 자리에 FAQ 번호가 들어갑니다. |
| `--per-page` | `100` | FAQ 목록 API를 한 번 호출할 때 가져올 건수입니다. |
| `--max-pages` | 없음 | 수집할 최대 페이지 수입니다. 테스트용으로 일부 페이지만 실행할 때 유용합니다. |
| `--timeout` | `20` | FAQ 수집 요청 타임아웃(초)입니다. |
| `--user-agent` | `Mozilla/5.0 (compatible; tossify-py/1.0)` | 퍼스트몰 요청 시 사용할 User-Agent 문자열입니다. 403 회피가 필요할 때 조정합니다. |
| `--sleep-sec` | `0.0` | FAQ 페이지 수집 사이에 넣을 대기 시간(초)입니다. |
| `--a-csv` | `data/faq_a.csv` | 원본 FAQ(A) CSV 경로입니다. `all`/`collect` 모드에서는 출력 파일, `transform` 모드에서는 입력 파일로 사용합니다. |
| `--a-db` | `data/faq.db` | 원본 FAQ(A) SQLite 저장 경로입니다. |
| `--openai-api-key` | `.env` 또는 환경변수 `OPENAI_API_KEY` | OpenAI API 키입니다. 인자로 직접 넘기거나 `.env`/환경변수로 준비할 수 있습니다. |
| `--openai-model` | `gpt-4o-mini` | FAQ 문체 변환에 사용할 OpenAI 모델명입니다. |
| `--style-instruction` | 내장 기본 프롬프트 | 모델에 전달할 시스템 지시문입니다. 토스체 규칙을 바꾸고 싶을 때만 조정하면 됩니다. |
| `--openai-interval-sec` | `0.0` | OpenAI 요청 사이 대기 시간(초)입니다. 너무 빠른 호출을 줄이고 싶을 때 사용합니다. |
| `--openai-connect-timeout-sec` | `8.0` | OpenAI 연결 타임아웃(초)입니다. |
| `--openai-read-timeout-sec` | `25.0` | OpenAI 응답 읽기 타임아웃(초)입니다. |
| `--openai-max-retries` | `2` | OpenAI 호출 실패 시 재시도 횟수입니다. |
| `--openai-progress-step` | `50` | 몇 건마다 진행률을 출력할지 정합니다. `0`이면 진행률을 출력하지 않습니다. |
| `--transform-save-every` | `10` | 변환 결과를 몇 건마다 `faq_b.csv`, DB, 체크포인트 파일로 중간 저장할지 정합니다. |
| `--transform-checkpoint` | `b-csv + .checkpoint.json` | 변환 재개용 체크포인트 파일 경로입니다. 비우면 `faq_b.csv.checkpoint.json`처럼 자동 생성합니다. |
| `--resume-transform` / `--no-resume-transform` | `resume-transform` | 체크포인트가 있을 때 이어서 변환할지 정합니다. 기본값은 재개 사용입니다. |
| `--skip-transform` | 꺼짐 | 하위 호환 옵션입니다. 지정 시 `--mode collect`처럼 동작합니다. |
| `--b-csv` | `data/faq_b.csv` | 변환 FAQ(B) CSV 저장 경로입니다. |
| `--b-db` | `data/faq.db` | 변환 FAQ(B) SQLite 저장 경로입니다. |

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

기본 모드(`--mode all`) 기준:

1. `scraping.py` 시작 시 `.env`가 있으면 자동으로 로드합니다.
2. FAQ 목록 API에서 페이지 단위로 게시글 순번(`seq`)을 수집합니다.
3. 순번 기반 상세 URL(`.../faq/{source_id}`)에 접근해 질문/답변 본문을 추출합니다.
4. 원본 FAQ(A)를 CSV와 SQLite(`faq_a`)로 저장합니다.
5. OpenAI API로 FAQ를 변환합니다.
6. 변환 중 네트워크/서버 오류는 재시도하고, 실패 항목은 `insufficient_source`로 표시합니다.
7. 변환 중간 결과를 배치 단위로 CSV, SQLite, 체크포인트에 저장합니다.
8. 실행이 중단되면 다음 실행 시 체크포인트를 읽어 이어서 진행할 수 있습니다.
9. 변환 FAQ(B)를 최종 CSV와 SQLite(`faq_b`)로 저장하고 체크포인트를 정리합니다.
10. 실행 건수/진행률/저장 경로를 출력하고 종료합니다.

모드별 차이:

- `--mode collect`: 2~4단계만 수행하고 종료합니다.
- `--mode transform`: FAQ를 다시 수집하지 않고, 기존 `faq_a.csv`를 읽어 5~8단계만 수행합니다.

---

## 4. HTML 뷰어 사용 방법

별도 설치 없이 브라우저에서 `viewer.html`을 열어 `faq_b.csv`를 확인할 수 있습니다.

1. 브라우저에서 `viewer.html`을 엽니다.
2. `CSV 파일 선택` 또는 드래그 앤 드롭으로 `data/faq_b.csv`를 업로드합니다.
3. 첫 실행 시 온보딩 가이드가 자동으로 표시되며, 상단 `가이드 다시 보기` 버튼으로 언제든 재확인할 수 있습니다.
4. 검색/정렬/`보류건만 보기` 필터로 목록을 빠르게 좁힙니다.
5. 좌측 목록에서 게시글을 선택하면 우측 상세에서 원문/변환 결과를 확인할 수 있습니다.
6. `원문 전체 복사`, `변환 전체 복사`, 카드별 `복사` 버튼으로 즉시 복사할 수 있습니다.
7. 키보드 `↑` `↓`로 목록 이동이 가능합니다.

`file://`로 실행 시에는 브라우저 보안 정책상 파일 선택(`<input type="file">`) 방식만 안정적으로 동작합니다. `faq_b.csv`를 직접 선택해 업로드하세요.

서버 모드가 필요하면 프로젝트 루트에서 아래처럼 HTTP 서버를 띄워 접속하세요.

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
