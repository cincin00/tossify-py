# tossify-py

퍼스트몰 FAQ를 수집해 원본 데이터(A)로 저장하고, Gemini API로 Toss 말투 FAQ(B)로 변환해 저장하는 파이프라인입니다.

## 기능

1. 퍼스트몰 FAQ 전체 페이지 수집
2. 원본 FAQ(A)를 CSV + SQLite 저장
3. Gemini API로 Toss 스타일 변환 (프롬프트/지시어 옵션화)
4. 변환 FAQ(B)를 CSV + SQLite 저장

## 실행

```bash
python scraping.py \
  --per-page 100 \
  --a-csv data/faq_a.csv \
  --a-db data/faq.db \
  --b-csv data/faq_b.csv \
  --b-db data/faq.db \
  --gemini-api-key "$GEMINI_API_KEY"
```


기본값으로는 소스 코드에 내장된 **"토스(Toss) 수석 UX 라이터" 프롬프트**가 적용됩니다. 필요하면 `--style-instruction`으로 덮어쓸 수 있습니다.

### 변환 지시어 커스텀

```bash
python scraping.py \
  --gemini-api-key "$GEMINI_API_KEY" \
  --style-instruction "다음 FAQ를 Toss 고객센터 스타일로 바꿔줘. 짧고 명확한 문장으로 작성해줘."
```

### 수집만 실행 (Gemini 생략)

```bash
python scraping.py --skip-transform
```

## 주의사항

- 환경에 따라 퍼스트몰 API가 403을 반환할 수 있습니다. 이 경우 `--user-agent` 등 요청 헤더를 조정해보세요.
- Gemini 변환은 FAQ 건수만큼 API를 호출합니다. 요금/쿼터를 확인하세요.
