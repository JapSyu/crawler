# 크롤러 공통 흐름 (추상 클래스)

from abc import ABC, abstractmethod
from typing import List, Tuple
from datetime import datetime, timezone
from loguru import logger
from .models import CompanyReport, SourcePage


class BaseCrawler(ABC):
    company_key: str
    seed: List[Tuple[str, str]]  # [(label, url)]

    def __init__(self, company_key: str, seed: List[Tuple[str, str]]):
        self.company_key = company_key
        self.seed = seed

    # seed URL들을 돌며 SourcePage 리스트 만들기
    @abstractmethod
    async def fetch_all(self) -> List[SourcePage]: ...

    # 페이지들을 분석해 CompanyReport 반환
    @abstractmethod
    async def parse(self, pages: List[SourcePage]) -> CompanyReport: ...

    # 페이지 수집 -> 구조화 -> 결과 반환
    async def run(self) -> CompanyReport:
        logger.info(f"==> Start: {self.company_key}")
        pages = await self.fetch_all()  # 페이지 수집
        report = await self.parse(pages)  # 구조화
        report.collected_at = datetime.now(timezone.utc)
        logger.info(f"<== Done:  {self.company_key}")
        return report
