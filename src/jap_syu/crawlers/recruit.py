import asyncio
from typing import List, Tuple
from loguru import logger
from ..base_crawler import BaseCrawler
from ..models import CompanyReport, SourcePage
from ..utils.fetcher import fetch_html, build_source_page

SEED: List[Tuple[str, str]] = [
    ("Mission",       "https://example.com/recruit/mission"),
    ("BusinessModel", "https://example.com/recruit/business-model"),
    ("Careers",       "https://example.com/recruit/careers"),
]

class RecruitCrawler(BaseCrawler):
    def __init__(self):
        super().__init__("recruit-holdings", SEED)

    async def fetch_all(self) -> List[SourcePage]:
        pages: List[SourcePage] = []
        for label, url in self.seed:
            html, meta = await fetch_html(url)
            logger.info(f"[{label}] fetched via {meta['mode']}")
            sp = build_source_page(label, url, html)
            pages.append(SourcePage(**sp))
        return pages

    def parse(self, pages: List[SourcePage]) -> CompanyReport:
        org = {}
        phi = {"mission": {}, "vision": {}, "values": []}
        recruit = {}

        for sp in pages:
            if sp.label == "Mission":
                phi["mission"]["ja"] = "(미션 원문 추출 TODO)"
                phi["mission"]["ko"] = "(번역 TODO)"
            elif sp.label == "BusinessModel":
                org["website"] = str(sp.url)
            elif sp.label == "Careers":
                recruit["hiringTypes"] = ["新卒","中途"]

        return CompanyReport(
            company_key=self.company_key,
            collected_at=None,  # run()에서 채움
            source_pages=pages,
            organization=org,
            philosophy=phi,
            recruiting=recruit,
            summary_ko="(요약은 후처리 단계에서 GPT로 생성)"
        )

async def main():
    crawler = RecruitCrawler()
    report = await crawler.run()
    
    import json, os
    os.makedirs(".data", exist_ok=True)
    
    from json import dump
    data = report.model_dump(mode="json")
    with open(".data/recruit.json", "w", encoding="utf-8") as f:
        dump(data, f, ensure_ascii=False, indent=2)
    print("→ .data/recruit.json 저장 완료")

if __name__ == "__main__":
    asyncio.run(main())