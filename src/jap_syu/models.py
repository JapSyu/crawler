#(EDINET / Website / External)별로 그룹화된 스키마

from __future__ import annotations
from pydantic import BaseModel, HttpUrl, Field, ConfigDict
from typing import List, Literal, Optional, Dict
from datetime import datetime, timezone

# -----------------------------------------------------------------------------
# 공용 유틸
# -----------------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

# -----------------------------------------------------------------------------
# 소스 페이지 메타
# -----------------------------------------------------------------------------
class SourcePage(BaseModel):
    """
    크롤링/수집한 각 페이지의 메타데이터
    """
    model_config = ConfigDict(extra="ignore")

    label: Literal[
        # 기본 기업 정보
        "About","Company","Profile","CEOMessage",
        # 기업 철학/가치관
        "Philosophy","Mission","Vision","Values",
        # 기업 문화/복리후생
        "Culture","Benefits","WorkLife",
        # 근무 환경/오피스
        "Office","Environment",
        # 채용 정보
        "JobPostings","JobDetails","Careers",
        # 기업 정보 (통합 페이지)
        "CompanyInfo",
        # 추가 가능한 라벨들
        "History","Organization","Business","News","IR","Financial",
        "Products","Services","Technology","Research","Sustainability"
    ]
    url: HttpUrl
    fetched_at: datetime
    content_hash: str
    raw_html: Optional[str] = Field(default=None, description="HTML 원문 (디버깅용, 기본적으로 저장하지 않음)")

# -----------------------------------------------------------------------------
# IR 문서(예: 유가증권보고서) 메타
# -----------------------------------------------------------------------------
class IRDocument(BaseModel):
    model_config = ConfigDict(extra="ignore")

    title: str
    url: HttpUrl
    document_type: Literal["annual_report", "presentation", "financial_report", "press_release"]
    published_date: Optional[datetime] = None
    content_summary: Optional[str] = None
    extracted_at: datetime = Field(default_factory=utcnow)

# -----------------------------------------------------------------------------
# EDINET(유가증권보고서) 기반 데이터
# -----------------------------------------------------------------------------
class EdinetBasic(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str = ""                         # 제출회사명/상호
    address: str = ""                      # 본사 소재지(요약)
    founded_year: Optional[int] = None     # 설립연도(숫자)
    employee_count: Optional[int] = None   # (연결 기준 권장)

class EdinetHR(BaseModel):
    model_config = ConfigDict(extra="ignore")
    avgTenureYears: Optional[float] = None     # 평균근속연수
    avgAgeYears: Optional[float] = None        # 평균 연령
    avgAnnualSalaryJPY: Optional[int] = None   # 평균 연봉(정수 JPY)
    femaleRatio: Optional[float] = None        # 0~1 (있을 경우)

class EdinetFinancials(BaseModel):
    model_config = ConfigDict(extra="ignore")
    revenueJPY: Optional[int] = None           # FY 매출(정규화된 숫자)
    fiscalYear: Optional[int] = None           # 해당 FY (예: 2024)

class EdinetData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    basic: EdinetBasic = Field(default_factory=EdinetBasic)
    hr: EdinetHR = Field(default_factory=EdinetHR)
    financials: EdinetFinancials = Field(default_factory=EdinetFinancials)
    # 출처 근거(문서/섹션/페이지 등)
    provenance: Dict[str, str] = Field(default_factory=dict)

# -----------------------------------------------------------------------------
# 회사 공식 홈페이지 기반 데이터(브랜딩/문화/채용/뉴스)
# -----------------------------------------------------------------------------
class WebsiteBranding(BaseModel):
    model_config = ConfigDict(extra="ignore")
    logo: Optional[str] = None                 # 로고 URL
    industry: str = ""                         # 업종(요약)
    size: str = ""                             # 규모(대기업/중견 등)
    location: str = ""                         # 대표 도시 (간단한 도시명만)
    description: str = ""                      # 짧은 소개
    website: str = ""                          # 공식 URL

class WebsitePhilosophy(BaseModel):
    model_config = ConfigDict(extra="ignore")
    description: str = ""
    mission: str = ""
    vision: str = ""
    values: List[str] = Field(default_factory=list)

class WebsiteCulture(BaseModel):
    model_config = ConfigDict(extra="ignore")
    workCulture: str = ""
    benefitsDetail: List[str] = Field(default_factory=list)
    workEnvironment: Dict[str, str] = Field(default_factory=dict)  # remoteWork/flexTime 등

class WebsiteOffice(BaseModel):
    model_config = ConfigDict(extra="ignore")
    address: str = ""
    access: str = ""
    facilities: List[str] = Field(default_factory=list)

class WebsiteJobs(BaseModel):
    model_config = ConfigDict(extra="ignore")
    jobPostings: List[Dict] = Field(default_factory=list)  # {position, department, ...}
    # 필요한 경우 세부 설명/메타를 추가할 수 있음(예: detailURL 등)

class WebsiteNews(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ceoMessage: Optional[str] = None
    detailedDescription: Optional[str] = None
    careerProgram: List[str] = Field(default_factory=list)
    salaryBreakdown: Dict[str, str] = Field(default_factory=dict)  # junior/mid/senior/executive
    recentNews: List[Dict[str, str]] = Field(default_factory=list) # {title,date,summary}

class WebsiteData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    branding: WebsiteBranding = Field(default_factory=WebsiteBranding)
    philosophy: WebsitePhilosophy = Field(default_factory=WebsitePhilosophy)
    culture: WebsiteCulture = Field(default_factory=WebsiteCulture)
    office: WebsiteOffice = Field(default_factory=WebsiteOffice)
    jobs: WebsiteJobs = Field(default_factory=WebsiteJobs)
    news: WebsiteNews = Field(default_factory=WebsiteNews)

# -----------------------------------------------------------------------------
# 외부 사이트(OpenWork 등) 기반 데이터(평판/체감)
# -----------------------------------------------------------------------------
class ExternalRatings(BaseModel):
    model_config = ConfigDict(extra="ignore")
    openwork_rating: Optional[float] = None
    workLifeBalance: Optional[float] = None
    careerGrowth: Optional[float] = None
    benefits: Optional[float] = None

class ExternalData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ratings: ExternalRatings = Field(default_factory=ExternalRatings)
    salary_info: str = ""              # 텍스트 원문
    employee_count_note: str = ""      # 외부출처 기준의 체감 인원/메모

# -----------------------------------------------------------------------------
# 최상위 리포트
# -----------------------------------------------------------------------------
class CompanyReport(BaseModel):
    model_config = ConfigDict(extra="ignore")

    company_key: str
    collected_at: Optional[datetime] = None
    source_pages: List[SourcePage] = Field(default_factory=list)

    # 출처별 데이터 그룹
    edinet: EdinetData = Field(default_factory=EdinetData)       # 공시(표준/연1회)
    website: WebsiteData = Field(default_factory=WebsiteData)     # 공식 사이트(브랜딩/채용/문화)
    external: ExternalData = Field(default_factory=ExternalData)  # 평판/체감(OpenWork 등)

    # IR 문서 목록(근거 링크/버전 관리)
    ir_documents: List[IRDocument] = Field(default_factory=list)

    # 요약/경고
    summary_ko: str = ""
    warnings: List[str] = Field(default_factory=list)
    
    # 데이터 통합 유틸리티 메서드들
    def get_company_name(self) -> str:
        """회사명 (EDINET 우선, 없으면 Website)"""
        if self.edinet.basic.name:
            return self.edinet.basic.name
        return self.website.branding.name or "정보 없음"
    
    def get_company_address(self) -> str:
        """본사 주소 (EDINET 우선, 없으면 Website)"""
        if self.edinet.basic.address:
            return self.edinet.basic.address
        return self.website.branding.location or "정보 없음"
    
    def get_company_location(self) -> str:
        """대표 도시 (Website에서 간단한 도시명)"""
        return self.website.branding.location or "정보 없음"
    
    def get_founded_year(self) -> Optional[int]:
        """설립년도 (EDINET 우선)"""
        return self.edinet.basic.founded_year
    
    def get_employee_count(self) -> Optional[int]:
        """직원 수 (EDINET 우선, 없으면 External)"""
        if self.edinet.basic.employee_count:
            return self.edinet.basic.employee_count
        # External에서 숫자 추출 시도
        if self.external.employee_count_note:
            import re
            match = re.search(r'(\d+)', self.external.employee_count_note)
            if match:
                return int(match.group(1))
        return None
    
    def get_avg_salary(self) -> Optional[int]:
        """평균 급여 (EDINET 우선)"""
        return self.edinet.hr.avgAnnualSalaryJPY