# tossify-py

퍼스트몰 FAQ를 수집해 원본 데이터(A)로 저장하고, OpenAI API를 통해 Toss 스타일 FAQ(B)로 변환해 저장하는 파이프라인입니다.

## 1. 로컬 환경 구성 방법

### 1) Python 설치

Python 3.12 이상을 권장합니다.

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

설치 확인:

```bash
python3 --version
```

### 2) 가상환경 생성/활성화

프로젝트 루트에서 실행하세요.

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

`.env.example`을 복사해 `.env`를 만들고 OpenAI 키를 설정하세요.

```bash
cp .env.example .env
```

`.env` 예시:

```bash
OPENAI_API_KEY=your_openai_api_key
```

실행 예시:

```bash
python scraping.py \
  --per-page 100 \
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

## 2. 서비스 구조

| 구성요소 | 역할 | 주요 입력 | 주요 출력 |
|---|---|---|---|
| `scraping.py` | 전체 파이프라인 진입점(CLI) | 실행 인자, 환경변수 | A/B 데이터 저장 |
| `collect_all_faq` | 퍼스트몰 FAQ API 페이지네이션 수집 | `base_url`, `per_page`, `max_pages` 등 | `FaqItem[]` |
| `save_a_to_csv` / `save_a_to_sqlite` | 원본 FAQ(A) 저장 | `FaqItem[]` | `data/faq_a.csv`, `faq_a` 테이블 |
| `transform_with_openai` | OpenAI 모델로 FAQ 문체 변환 | `FaqItem[]`, 모델명, API 키, 지시어 | `FaqTransformed[]` |
| `save_b_to_csv` / `save_b_to_sqlite` | 변환 FAQ(B) 저장 | `FaqTransformed[]` | `data/faq_b.csv`, `faq_b` 테이블 |
| `data/faq.db` | A/B 결과 통합 저장소(SQLite) | 저장 함수 호출 | `faq_a`, `faq_b` |

## 3. 서비스 라이프 싸이클

1. 사용자가 CLI 인자와 환경변수(`OPENAI_API_KEY`)를 준비해 실행합니다.
2. `scraping.py`가 퍼스트몰 FAQ를 페이지 단위로 수집하고 정규화합니다.
3. 원본 FAQ(A)를 CSV/SQLite(`faq_a`)로 저장합니다.
4. `--skip-transform`이 아니면 OpenAI API로 FAQ를 변환합니다.
5. 변환 FAQ(B)를 CSV/SQLite(`faq_b`)로 저장합니다.
6. 실행 결과(건수/저장 경로)를 콘솔에 출력하고 종료합니다.

---

### 참고

- 커스텀 지시어 적용:
```bash
python scraping.py \
  --openai-api-key "$OPENAI_API_KEY" \
  --style-instruction "다음 FAQ를 Toss 고객센터 스타일로 바꿔줘. 짧고 명확한 문장으로 작성해줘."
```

- 변환 요청은 FAQ 건수만큼 OpenAI API를 호출하므로 요금/쿼터를 확인하세요.
- 퍼스트몰 API가 환경에 따라 403을 반환할 수 있어 `--user-agent` 조정이 필요할 수 있습니다.
- 원문 답변(`answer`)이 비어 있으면 모델 추정을 막기 위해 해당 항목은 자동으로 변환 보류 처리됩니다.
