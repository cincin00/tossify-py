import requests

target_url = "https://www.firstmall.kr/customer/faq/search?"
payload = {
    # 페이지 번호(필수)
    "page": "1",
    # 페이지당 게시글 수(필수)
    "per_page": "1000",
    # 정렬기준(필수)
    "order_by": "regist_date",
    # 카테고리(옵션)
    "category": "",
    # 검색어(옵션)
    "keyword": "",
}
response = requests.get(target_url, params=payload)
rj = response.json()
r_body = rj.text
# r_body = rj.text
# r_body = rj.text
