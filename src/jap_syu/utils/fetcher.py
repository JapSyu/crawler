# 크롤링

import hashlib # 해시 함수를 사용하기 위한 모듈
from datetime import datetime, timezone # 날짜 및 시간 관련 모듈
from typing import Tuple, Dict # 타입 힌트 모듈 (Tuple, Dict 등)
from tenacity import retry, stop_after_attempt, wait_exponential # 특정 조건에 따라 실패시 재시도 모듈
import httpx # HTTP 요청을 위한 비동기 라이브러리
from playwright.async_api import async_playwright # 브라우저 자동화를 위한 라이브러리

# 텍스트를 SHA256 해시로 변환하는 함수
# 웹페이지 내용이 변경되었는지 확인할 때 사용한다.
def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()

# HTML 문자열을 정규화하는 함수
# 해시 계산 시 일관성을 위해 사용
def _normalize(html: str) -> str:
    return " ".join(html.split())

# httpx로 정적 페이지 요청 -> HTML 문자열과 메타(mode, status) 반환
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6)) # 실패시 최대 3번까지 재시도
async def fetch_html(url: str) -> Tuple[str, Dict]:
    # 1) 정적 요청 먼저
    # 1. HTTP 클라이언트 생성 (리다이렉트 허용, 30초 타임아웃)
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        # 2. 웹페이지 요청 (브라우저 처럼 요청 (Bot 차단 방지))
        r = await client.get(url, headers={"User-Agent":"Mozilla/5.0"})
        # 3. 응답 확인 (성공 및 HTML 페이지인지)
        if r.status_code == 200 and ("</html>" in r.text or "<body" in r.text):
            # 4. 성공 시 HTML과 메타데이터 반환
            return r.text, {"mode":"httpx", "status": r.status_code}
        
    # 2) SPA/동적 렌더링 폴백
    # 1번 과정 실패 시 playwrite로 렌더링
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) # 브라우저 실행 (headless 모드(브라우저 창 띄우지 않음))
        page = await browser.new_page(user_agent="Mozilla/5.0") # 브라우저 탭 생성
        await page.goto(url, wait_until="domcontentloaded", timeout=45000) # URL로 이동, html 구조가 완성될 때까지 기다림
        await page.wait_for_load_state("networkidle") # 네트워크 요청이 전부 끝날 때까지 기다림
        html = await page.content() # 현재 페이지의 완전한 HTML 가져오기
        await browser.close() # 브라우저 닫기
        return html, {"mode":"playwright", "status": 200} # 성공 시 HTML과 메타데이터 반환

# 웹 페이지 정보를 구조화된 딕셔너리로 만듦
def build_source_page(label: str, url: str, html: str) -> dict:
    norm = _normalize(html)
    return {
        "label": label,
        "url": url,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "content_hash": _sha256(norm),
        "raw_html": html,
    }