from abc import ABC, abstractmethod
from typing import List, Tuple
from datetime import datetime
from loguru import logger
from .models import CompanyReport, SourcePage

class BaseCrawler(ABC):
    company_key: str
    seed: List[Tuple[str, str]]  # [(label, url)]

    def __init__(self, company_key: str, seed: List[Tuple[str, str]]):
        self.company_key = company_key
        self.seed = seed

    @abstractmethod
    async def fetch_all(self) -> List[SourcePage]:
        ...

    @abstractmethod
    def parse(self, pages: List[SourcePage]) -> CompanyReport:
        ...

    async def run(self) -> CompanyReport:
        logger.info(f"==> Start: {self.company_key}")
        pages = await self.fetch_all()
        report = self.parse(pages)
        report.collected_at = datetime.utcnow()
        logger.info(f"<== Done:  {self.company_key}")
        return report