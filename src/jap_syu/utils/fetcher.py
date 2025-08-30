import hashlib
from datetime import datetime
from typing import Tuple, Dict
from tenacity import retry, stop_after_attempt, wait_exponential
import httpx
from playwright.async_api import async_playwright

def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()

def _normalize(html: str) -> str:
    return " ".join(html.split())

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6))
async def fetch_html(url: str) -> Tuple[str, Dict]:
    # 1) 정적 요청 먼저
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        r = await client.get(url, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code == 200 and ("</html>" in r.text or "<body" in r.text):
            return r.text, {"mode":"httpx", "status": r.status_code}
    # 2) SPA/동적 렌더링 폴백
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent="Mozilla/5.0")
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_load_state("networkidle")
        html = await page.content()
        await browser.close()
        return html, {"mode":"playwright", "status": 200}

def build_source_page(label: str, url: str, html: str) -> dict:
    norm = _normalize(html)
    return {
        "label": label,
        "url": url,
        "fetched_at": datetime.utcnow().isoformat(),
        "content_hash": _sha256(norm),
    }