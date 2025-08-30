from pydantic import BaseModel, HttpUrl
from typing import List, Literal, Optional
from datetime import datetime

class SourcePage(BaseModel):
    label: Literal["Mission","Values","BusinessModel","Careers","About","IR","News"]
    url: HttpUrl
    fetched_at: datetime
    content_hash: str

class CompanyReport(BaseModel):
    company_key: str
    collected_at: Optional[datetime] = None
    source_pages: List[SourcePage] = []
    organization: dict = {}
    philosophy: dict = {}
    business_units: List[dict] = []
    recruiting: dict = {}
    key_excerpts: List[dict] = []
    summary_ko: str = ""
    warnings: List[str] = []