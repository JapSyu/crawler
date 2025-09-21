"""
EDINET API 유틸리티
일본 금융청의 기업정보공개시스템(EDINET) API를 사용하여 기업 정보를 가져옵니다.
"""

import httpx
import asyncio
import zipfile
import io
import re
import os
import json
from typing import Dict, List, Optional, Tuple, NamedTuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass

# .env 파일 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
from pathlib import Path
from loguru import logger
from bs4 import BeautifulSoup
from ..models import EdinetData, EdinetBasic, EdinetHR, EdinetFinancials, IRDocument

# .env 파일 로드 (python-dotenv가 설치되어 있으면)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv가 없으면 환경변수만 사용
    pass

# EDINET API 설정
EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"
EDINET_API_KEY = os.getenv("EDINET_API_KEY")

@dataclass
class CompanyDocument:
    """발견된 기업 문서 정보"""
    document_id: str
    company_name: str 
    submitted_date: str
    doc_type: str
    company_key: str  # 내부 식별용
    sec_code: str = None  # 상장번호

class EdinetAPI:
    """EDINET API 클라이언트"""
    
    def __init__(self):
        self.base_url = EDINET_API_BASE
        self.session = None
        
    
    
    async def __aenter__(self):
        self.session = httpx.AsyncClient(timeout=30.0)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.aclose()
    
    async def get_document_list(self, date: str) -> List[dict]:
        """특정 날짜의 문서 리스트 조회"""
        try:
            response = await self.session.get(
                f"{self.base_url}/documents.json",
                params={"date": date, "type": 2},  # type=2: 메타데이터만
                headers={"Ocp-Apim-Subscription-Key": EDINET_API_KEY}
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("results", [])
            else:
                logger.warning(f"문서 리스트 조회 실패 ({date}): {response.status_code}")
                
        except Exception as e:
            logger.error(f"문서 리스트 조회 중 오류 ({date}): {e}")
        
        return []

    async def get_document_package(self, document_id: str, doc_type: str = "1") -> Optional[bytes]:
        """유가증권보고서 패키지 다운로드 (type=1: ZIP 파일)"""
        try:
            response = await self.session.get(
                f"{self.base_url}/documents/{document_id}",
                params={"type": doc_type},  # 1: ZIP 파일 (iXBRL 포함)
                headers={"Ocp-Apim-Subscription-Key": EDINET_API_KEY}
            )
            
            if response.status_code == 200:
                return response.content
            else:
                logger.error(f"EDINET 문서 다운로드 실패: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"EDINET 문서 다운로드 중 오류: {e}")
            return None
    
    def extract_honbun_files(self, zip_content: bytes) -> List[Tuple[str, str]]:
        """ZIP 파일에서 honbun iXBRL 파일들 추출"""
        honbun_files = []
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                for file_info in zip_file.filelist:
                    filename = file_info.filename
                    if "honbun" in filename and ("ixbrl" in filename or "ix:" in filename):
                        content = zip_file.read(filename).decode('utf-8', errors='ignore')
                        honbun_files.append((filename, content))
                        logger.info(f"honbun 파일 발견: {filename}")
        
        except Exception as e:
            logger.error(f"ZIP 파일 처리 중 오류: {e}")
        
        return honbun_files
    
    def extract_header_file(self, zip_content: bytes) -> Optional[Tuple[str, str]]:
        """ZIP 파일에서 header iXBRL 파일 추출"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                for file_info in zip_file.filelist:
                    filename = file_info.filename
                    if "header" in filename and ("ixbrl" in filename or filename.endswith('.htm')):
                        content = zip_file.read(filename).decode('utf-8', errors='ignore')
                        logger.info(f"header 파일 발견: {filename}")
                        return (filename, content)
        
        except Exception as e:
            logger.error(f"header 파일 처리 중 오류: {e}")
        
        return None
    
    def parse_basic_info_from_header(self, header_content: str, filename: str) -> Dict[str, any]:
        """header 파일에서 기본 정보 추출"""
        basic_info = {
            "name_en": "",
            "headquarters": "",
            "founded_year": None,
            "sec_code": None,
        }
        
        # 1. 영문명 추출
        english_patterns = [
            r'CompanyNameInEnglishCoverPage">([^<]+)</ix:nonNumeric>',
            r'CompanyNameInEnglish[^>]*>([^<]+)</ix:',
            r'【英訳名】[^>]*>([^<]+)<',
        ]
        
        for pattern in english_patterns:
            match = re.search(pattern, header_content, re.IGNORECASE)
            if match:
                name_en = match.group(1).strip()
                if len(name_en) > 5:  # 유효한 영문명인 경우
                    basic_info["name_en"] = name_en
                    logger.info(f"header에서 영문명 추출: {name_en} (파일: {filename})")
                    break
        
        # 2. 본점 주소 추출
        address_patterns = [
            r'東京都[^<\n)]+\d+番?\d*号?',
            r'〒\d{3}-\d{4}[^<\n]+',
            r'本店[^>]*>([^<]+)',
            r'所在地[^>]*>([^<]+)',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, header_content, re.IGNORECASE)
            if matches:
                # 가장 완전해 보이는 주소 선택 (東京都로 시작하고 번지수가 있는 것) WHY? 도쿄 아니면 어쩔건데
                for address in matches:
                    clean_address = address.strip()
                    if (clean_address.startswith('東京都') and 
                        '番' in clean_address and 
                        len(clean_address) > 10):
                        basic_info["headquarters"] = clean_address
                        logger.info(f"header에서 본점 주소 추출: {clean_address} (파일: {filename})")
                        break
                if basic_info["headquarters"]:
                    break
        
        # 3. 설립일 추출 (실제 회사 설립일, 보고서 날짜가 아닌)　이건 본문에서 찾아야할듯. 헤더엔 안나와있음
        # 헤더에서 설립년도 추출 비활성화 - 사업년도 계산 우선 사용
        # founded_patterns = [
        #     r'設立[^>]*>([^<]*(\d{4})年(\d{1,2})月(\d{1,2})日[^<]*)',
        #     r'設立年月日[^>]*>([^<]*(\d{4})年(\d{1,2})月(\d{1,2})日[^<]*)',
        #     r'創立[^>]*>([^<]*(\d{4})年(\d{1,2})月(\d{1,2})日[^<]*)',
        # ]
        # 
        # for pattern in founded_patterns:
        #     matches = re.findall(pattern, header_content)
        #     if matches:
        #         for match in matches:
        #             year = int(match[1]) if len(match) > 1 else None
        #             # 1800년대~2000년대 초반의 합리적인 설립년도만 허용
        #             if year and 1800 <= year <= 2020:
        #                 basic_info["founded_year"] = year
        #                 logger.info(f"header에서 설립년도 추출: {year} (파일: {filename})")
        #                 break
        #         if basic_info["founded_year"]:
        #             break
        logger.info(f"헤더에서 설립년도 추출 건너뜀 - 사업년도 계산 우선 사용 (파일: {filename})")
        
        # 4. 상장번호 추출 (iXBRL 태그에서)
        sec_code_patterns = [
            r'SecurityCodeDEI">(\d{4})</ix:nonNumeric>',
            r'securityCode[^0-9]*(\d{4})',
            r'证券[コ]?[ー]?[ド][^0-9]*(\d{4})',
            r'[証券]?[コ]?[ー]?[ド]\s*[：:]\s*(\d{4})',
        ]
        
        for pattern in sec_code_patterns:
            match = re.search(pattern, header_content, re.IGNORECASE)
            if match:
                sec_code = match.group(1)
                if len(sec_code) == 4 and sec_code.isdigit():
                    basic_info["sec_code"] = sec_code
                    logger.info(f"header에서 상장번호 추출: {sec_code} (파일: {filename})")
                    break
        
        return basic_info
    
    def parse_employee_info(self, honbun_files: List[Tuple[str, str]]) -> Dict[str, any]:
        """honbun 파일들에서 인사 정보 추출"""
        employee_info = {
            "avgTenureYears": None,
            "avgAgeYears": None,
            "avgAnnualSalaryJPY": None,
            "employeeCount": None,
            "provenance": {}
        }
        
        # 찾을 키워드들
        keywords = {
            "avgTenureYears": ["平均勤続年数", "AverageLengthOfServiceYears"],
            "avgAgeYears": ["平均年齢", "AverageAgeYears"],
            "avgAnnualSalaryJPY": ["平均年間給与", "AverageAnnualSalary"],
            "employeeCount": ["従業員数", "NumberOfEmployees"]
        }
        
        # iXBRL 개념 이름들 (실제 name 속성값)
        ixbrl_concepts = {
            "avgTenureYears": [
                "jpcrp_cor:AverageLengthOfServiceYearsInformationAboutReportingCompanyInformationAboutEmployees",
                "jpcrp_cor:AverageLengthOfServiceYearsInformationOfReportingCompanyInformation",
                "jpcrp_cor:AverageLengthOfServiceYears"
            ],
            "avgAgeYears": [
                "jpcrp_cor:AverageAgeYearsInformationAboutReportingCompanyInformationAboutEmployees",
                "jpcrp_cor:AverageAgeYearsInformationOfReportingCompanyInformation", 
                "jpcrp_cor:AverageAgeYears",
                "jpcrp_cor:AverageAge"
            ],
            "avgAnnualSalaryJPY": [
                "jpcrp_cor:AverageAnnualSalaryInformationAboutReportingCompanyInformationAboutEmployees",
                "jpcrp_cor:AverageAnnualSalaryInformationOfReportingCompanyInformation",
                "jpcrp_cor:AverageAnnualSalary"
            ],
            "employeeCount": [
                "jpcrp_cor:NumberOfEmployees",
                "jpcrp_cor:AverageNumberOfEmployees"
            ]
        }
        
        for filename, content in honbun_files:
            logger.info(f"직원 정보 추출 시도: {filename}")
            
            # 각 지표별로 추출
            for field in keywords.keys():
                if employee_info[field] is not None:
                    continue  # 이미 찾았으면 스킵
                
                # 1. 우선 iXBRL 개념으로 검색 (가장 정확)
                if field in ixbrl_concepts:
                    for concept_name in ixbrl_concepts[field]:
                        results = self._extract_value_from_ixbrl_concept(content, concept_name, filename)
                        if results:
                            # contextRef로 최적 값 선택
                            best_result = self._select_best_value_by_context(results)
                            # scale/decimals/unitRef 속성 적용하여 최종 값 계산
                            final_value = self._apply_ixbrl_attributes(best_result, field)
                            employee_info[field] = final_value
                            employee_info["provenance"][field] = {
                                "file": filename,
                                "concept": concept_name,
                                "contextRef": best_result['contextRef'],
                                "unitRef": best_result['unitRef'],
                                "scale": best_result['scale'],
                                "method": "ixbrl_concept"
                            }
                            logger.info(f"{field}: iXBRL 개념으로 값 추출 성공 - {best_result['value']}")
                            break
                
                # 2. iXBRL로 못 찾으면 키워드 백업
                if employee_info[field] is None:
                    for keyword in keywords[field]:
                        if keyword in content:
                            value = self._extract_value_from_context(content, keyword, filename)
                            if value is not None:
                                employee_info[field] = value
                                employee_info["provenance"][field] = {
                                    "file": filename,
                                    "keyword": keyword,
                                    "method": "text_search_backup"
                                }
                                logger.info(f"{field}: 텍스트 백업으로 값 추출 - {value}")
                                break
        
        return employee_info
    
    def parse_basic_info(self, honbun_files: List[Tuple[str, str]], zip_content: bytes = None) -> Dict[str, any]:
        """honbun 파일들과 header 파일에서 기업 기본 정보 추출"""
        logger.info(f"🔍 parse_basic_info 호출됨 - {len(honbun_files)}개 honbun 파일 처리")
        basic_info = {
            "name": "",
            "name_en": "",
            "headquarters": "",
            "founded_year": None,
            "sec_code": None,
            "provenance": {}
        }
        
        # 헤더에서 추출한 설립년도 임시 저장 (연혁 우선, 없으면 사용)
        header_founded_year = None
        header_filename_for_founded = None
        
        # 먼저 header 파일에서 정보 추출 (우선순위)
        if zip_content:
            header_file = self.extract_header_file(zip_content)
            if header_file:
                filename, content = header_file
                header_info = self.parse_basic_info_from_header(content, filename)
                
                # header에서 추출한 정보로 우선 채움
                if header_info["name_en"]:
                    basic_info["name_en"] = header_info["name_en"]
                    basic_info["provenance"]["name_en"] = {"file": filename, "method": "header_ixbrl"}
                
                if header_info["headquarters"]:
                    basic_info["headquarters"] = header_info["headquarters"]
                    basic_info["provenance"]["headquarters"] = {"file": filename, "method": "header_ixbrl"}
                
                # 사업년도에서 설립년도 계산 (header에서 우선 시도)
                header_founded_year = self._extract_founded_year(content, filename, zip_content)
                if header_founded_year:
                    basic_info["founded_year"] = header_founded_year
                    basic_info["provenance"]["founded_year"] = {"file": filename, "method": "header_business_year"}
                    logger.info(f"헤더에서 설립년도 계산 완료: {header_founded_year}년 (파일: {filename})")
                
                if header_info["sec_code"]:
                    basic_info["sec_code"] = header_info["sec_code"]
                    basic_info["provenance"]["sec_code"] = {"file": filename, "method": "header_ixbrl"}
        
        for filename, content in honbun_files:
            # 1. 회사명 추출 (提出会社の状況)
            if not basic_info["name"]:
                name = self._extract_company_name(content, filename)
                if name:
                    basic_info["name"] = name
                    basic_info["provenance"]["name"] = {"file": filename, "method": "regex"}
            
            # 2. 영문명 추출 (英訳名)
            if not basic_info["name_en"]:
                name_en = self._extract_company_name_en(content, filename)
                if name_en:
                    basic_info["name_en"] = name_en
                    basic_info["provenance"]["name_en"] = {"file": filename, "method": "regex"}
            
            # 3. 본점 소재지 추출 (本店の所在の場所)
            if not basic_info["headquarters"]:
                headquarters = self._extract_headquarters(content, filename)
                if headquarters:
                    basic_info["headquarters"] = headquarters
                    basic_info["provenance"]["headquarters"] = {"file": filename, "method": "regex"}
            
            # 4. 설립년도는 이미 header에서 처리됨 (연혁 추출은 성능상 스킵)
            # 헤더의 사업년도 정보가 더 정확하므로 우선 사용
            
            # 5. 상장번호 추출 (증券コード)
            if not basic_info["sec_code"]:
                sec_code = self._extract_security_code(content, filename)
                if sec_code:
                    basic_info["sec_code"] = sec_code
                    basic_info["provenance"]["sec_code"] = {"file": filename, "method": "regex"}
        
        # 설립년도는 이미 header에서 처리됨
        
        return basic_info
    
    def _extract_company_name(self, content: str, filename: str) -> Optional[str]:
        """회사명 추출"""
        patterns = [
            r"会社名[^a-zA-Zａ-ｚ０-９]*?([^\s\n]+株式会社[^\s\n]*)",
            r"商号[^a-zA-Zａ-ｚ０-９]*?([^\s\n]+株式会社[^\s\n]*)",
            r"提出会社の状況[^a-zA-Z]*?会社名[^a-zA-Z]*?([^\s\n]+株式会社[^\s\n]*)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                name = match.group(1).strip()
                
                # HTML 태그 제거
                name = re.sub(r'<[^>]+>', '', name)
                # 특수문자 제거 (「」, (), 、, 。, 등)
                name = re.sub(r'[「」（）()、。]', '', name)
                name = re.sub(r'\s+', ' ', name).strip()
                name = name.rstrip('.,').strip()
                
                # 유효성 검사
                if name and len(name) > 3 and not re.search(r'[<>]', name):
                    logger.info(f"회사명 추출: {name} (파일: {filename})")
                    return name
        return None
    
    def _extract_company_name_en(self, content: str, filename: str) -> Optional[str]:
        """영문명 추출 (fallback) - 헤더에서 찾지 못한 경우의 백업"""
        
        # 일반적인 영문 회사명 패턴들 (본문에서 Inc, Corp, Company 등 찾기)
        
        patterns = [
            r'([A-Z][A-Za-z\s]*Holdings[A-Za-z\s]*Limited)',
            r'([A-Z][A-Za-z\s]*Corporation)',
            r'([A-Z][A-Za-z\s]*Holdings)',
            r'([A-Z][A-Za-z\s]*Group)',
            r'([A-Z][A-Za-z\s]*Inc\.?)',
            r'([A-Z][A-Za-z\s]*Corp\.?)', 
            r'([A-Z][A-Za-z\s]*Company)',
            r'([A-Z][A-Za-z\s]*Co\.?)',
            r'([A-Z][A-Za-z\s]*Ltd\.?)',
            r'([A-Z][A-Za-z\s]*Limited)',
        ]
        
        for i, pattern in enumerate(patterns):
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                name_en = match.group(1).strip()
                
                # HTML 태그 제거
                name_en = re.sub(r'<[^>]+>', '', name_en)
                # 불필요한 문자들 제거
                name_en = re.sub(r'[、。）\)]', '', name_en)
                name_en = re.sub(r'\s+', ' ', name_en).strip()
                name_en = name_en.rstrip('.,').strip()
                
                # 유효성 체크
                if (len(name_en) >= 8 and 
                    re.search(r'[A-Z]', name_en) and  # 대문자 포함
                    ('Holdings' in name_en or 'Limited' in name_en or 'Inc' in name_en or 'Corp' in name_en or 'Company' in name_en)):
                    logger.info(f"영문명 추출 성공 (패턴 {i+1}): {name_en} (파일: {filename})")
                    return name_en
                elif len(name_en) >= 5:
                    logger.debug(f"영문명 후보 (패턴 {i+1}): '{name_en}' - 조건 미달")
        
        logger.warning(f"영문명을 찾을 수 없음 (파일: {filename})")
        return None
    
    def _extract_headquarters(self, content: str, filename: str) -> Optional[str]:
        """본점 소재지 추출 (本店の所在の場所)"""
        patterns = [
            # 일본 주요 도도부현 주소 패턴들 (구체적인 주소)
            r"(東京都[^<\n)]+\d+番?\d*号?)",
            r"(大阪[府市][^<\n)]+\d+番?\d*号?)",
            r"(京都[府市][^<\n)]+\d+番?\d*号?)",
            r"(神奈川県[^<\n)]+\d+番?\d*号?)",
            r"(愛知県[^<\n)]+\d+番?\d*号?)",
            r"(福岡県[^<\n)]+\d+番?\d*号?)",
            r"(北海道[^<\n)]+\d+番?\d*号?)",
            r"([^<\n)]*[都道府県市区町村][^<\n)]+\d+番?\d*号?)",  # 일반적인 패턴
            
            # 우편번호가 있는 주소
            r"(〒\d{3}-\d{4}[^<\n]+)",
            
            # 전통적인 본점 패턴들
            r"本店の所在の場所[^>]*>([^<]+)",
            r"本店の所在の場所[^\n]*?([^\n<]+)",
            r"本店所在地[^>]*>([^<]+)",
            r"本店所在地[^\n]*?([^\n<]+)",
            r"本社所在地[^>]*>([^<]+)",
            r"本社所在地[^\n]*?([^\n<]+)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                # 그룹이 있는 패턴인지 확인 (괄호가 포함된 패턴)
                if match.groups():
                    headquarters = match.group(1).strip()
                else:
                    headquarters = match.group(0).strip()
                    
                # HTML 태그 제거
                headquarters = re.sub(r'<[^>]+>', '', headquarters)
                # 불필요한 문자 제거
                headquarters = re.sub(r'^[:\s・]*', '', headquarters)
                headquarters = re.sub(r'[>\]]*$', '', headquarters)  # 끝의 > 문자 제거
                headquarters = headquarters.strip()
                
                # 유효성 검사 (HTML 태그나 이상한 문자들 필터링)
                if (headquarters and len(headquarters) > 3 and 
                    not re.search(r'[<>]', headquarters) and
                    not headquarters.startswith('を') and
                    not headquarters.endswith('</b></p>')):
                    logger.info(f"본점 소재지 추출: {headquarters} (파일: {filename})")
                    return headquarters
        return None
    
    def _extract_submission_year(self, content: str, filename: str) -> Optional[int]:
        """제출일(提出日)에서 년도 추출"""
        try:
            # 제출일 패턴들 (다양한 형태 지원)
            submission_patterns = [
                r"提出日[^0-9]*?(\d{4})年(\d{1,2})月(\d{1,2})日",  # 提出日: 2025年6月23日
                r"提出日[^0-9]*?(\d{4})年",  # 提出日: 2025年
                r"提出年月日[^0-9]*?(\d{4})年(\d{1,2})月(\d{1,2})日",  # 提出年月日: 2025年6月23日
                r"提出年月日[^0-9]*?(\d{4})年",  # 提出年月日: 2025年
                r"SubmissionDate[^0-9]*?(\d{4})",  # SubmissionDate: 2025
                r"DocumentPeriodEndDate[^0-9]*?(\d{4})",  # DocumentPeriodEndDate: 2025
                # iXBRL 태그에서 추출
                r'<ix:nonNumeric[^>]*name="[^"]*SubmissionDate[^"]*"[^>]*>.*?(\d{4})年.*?</ix:nonNumeric>',
                r'<ix:nonNumeric[^>]*name="[^"]*DocumentPeriodEndDate[^"]*"[^>]*>.*?(\d{4})年.*?</ix:nonNumeric>',
                # 파일명에서 추출 (예: 2025-06-23)
                r'(\d{4})-\d{2}-\d{2}',
            ]
            
            for pattern in submission_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
                if matches:
                    # 첫 번째 매치에서 년도 추출
                    year_match = matches[0]
                    if isinstance(year_match, tuple):
                        year = int(year_match[0])  # 첫 번째 그룹이 년도
                    else:
                        year = int(year_match)
                    
                    # 합리적인 년도 범위 체크
                    if 2020 <= year <= 2030:
                        logger.info(f"제출일에서 년도 추출: {year}년 (패턴: {pattern}, 파일: {filename})")
                        return year
                    else:
                        logger.debug(f"제출일 년도 범위 벗어남: {year}년 (파일: {filename})")
            
            # 파일명에서 제출일 년도 추출 시도 (마지막 날짜가 제출일)
            # 예: 2025-03-31_01_2025-06-20 → 2025-06-20이 제출일
            filename_date_matches = re.findall(r'(\d{4})-(\d{2})-(\d{2})', filename)
            if filename_date_matches:
                # 마지막 날짜를 제출일로 간주
                submission_date = filename_date_matches[-1]
                year = int(submission_date[0])
                if 2020 <= year <= 2030:
                    logger.info(f"파일명에서 제출일 년도 추출: {year}년 (파일: {filename})")
                    return year
            
            logger.warning(f"제출일을 찾을 수 없음 (파일: {filename})")
            return None
            
        except Exception as e:
            logger.error(f"제출일 추출 중 오류: {e} (파일: {filename})")
            return None
    
    def _extract_founded_year(self, content: str, filename: str, zip_content: bytes = None) -> Optional[int]:
        """설립년도 추출 (事業年度 주기에서 계산)"""
        try:
            # 1. 먼저 제출일(提出日) 추출
            submission_year = self._extract_submission_year(content, filename)
            if not submission_year:
                logger.warning(f"제출일을 찾을 수 없어 설립년도 계산 불가 (파일: {filename})")
                return None
            
            logger.info(f"📅 제출일 기준년도: {submission_year}년")
            
            # 2. 事業年度(사업년도) 주기 정보에서 계산 - HTML 태그 고려
            business_year_patterns = [
                r"第(\d+)期[^<]*?事業年度",  # 第65期 (자 2024년...至 2025년...事業年度)
                r"第(\d+)期[^<]*?\(",  # 第65期 (자 2024년...
                r"第(\d+)期",  # 第65期 단독
                r"(\d+)期[^<]*?事業年度",  # 65期...事業年度
                r"事業年度[^<]*?第(\d+)期"  # 事業年度...第65期
            ]
            
            # HTML 태그 내부도 검색 (업데이트: 년도도 함께 추출)
            html_patterns = [
                r'<ix:nonNumeric[^>]*>.*?第(\d+)期.*?(\d{4})年.*?</ix:nonNumeric>',  # HTML 태그 내부에서 기수와 년도 함께
                r'<ix:nonNumeric[^>]*>.*?第(\d+)期.*?</ix:nonNumeric>',  # HTML 태그 내부
                r'>第(\d+)期[^<]*?事業年度<',  # 태그 사이
                r'>第(\d+)期[^<]*?\(<',  # 태그 사이
            ]
            
            # 디버깅: 실제 텍스트 일부 확인
            header_text = content[:3000]
            logger.info(f"🔍 설립년도 계산 디버깅 - 문서 헤더 (처음 1000자): {header_text[:1000]}")
            
            all_patterns = business_year_patterns + html_patterns
            
            for pattern in all_patterns:
                matches = re.findall(pattern, content[:15000], re.DOTALL)  # 더 넓은 범위에서 검색
                if matches:
                    logger.info(f"📊 사업년도 패턴 매칭: {pattern} → {matches}")
                    
                    # 매치 결과 분석
                    if isinstance(matches[0], tuple) and len(matches[0]) >= 2:
                        # 튜플인 경우: (기수, 년도) 형태
                        period = int(matches[0][0])
                        # 제출일 기준년도 사용
                        current_year = submission_year
                        logger.info(f"📊 패턴에서 기수 추출: 제{period}기, 제출일 기준년도: {current_year}년")
                    else:
                        # 단일 값인 경우: 기수만 추출
                        period = int(matches[0]) if isinstance(matches[0], str) else int(matches[0][0] if isinstance(matches[0], tuple) else matches[0])
                        # 제출일 기준년도 사용
                        current_year = submission_year
                        logger.info(f"📊 패턴에서 기수 추출: 제{period}기, 제출일 기준년도: {current_year}년")
                    
                    if current_year:
                        founded_year = current_year - period + 1  # +1은 창립년도 보정
                        logger.info(f"🧮 계산: {current_year}년 - {period}기 + 1 = {founded_year}년")
                        
                        if 1850 <= founded_year <= current_year:
                            logger.info(f"事業年度에서 설립년도 계산: 제{period}기 → {founded_year}년 (파일: {filename})")
                            return founded_year
                        else:
                            logger.warning(f"설립년도 범위 벗어남: {founded_year}년 (1850-{current_year} 범위)")
                    else:
                        logger.warning(f"기준년도를 찾을 수 없음 (패턴 매칭: {pattern})")
                else:
                    logger.debug(f"❌ 패턴 매칭 실패: {pattern}")
            
            # 3. 사업년도 방법이 실패하면 honbun 파일들에서 설립년도 직접 검색
            logger.info(f"honbun 파일들에서 설립년도 직접 검색 시도...")
            
            # honbun 파일들에서 설립년도 패턴 검색 (우선순위 순서)
            founded_patterns = [
                # 가장 정확한 패턴들 (1900년대 설립년도 우선)
                r"(19\d{2})年.*?設立",
                r"(19\d{2})年.*?創立", 
                r"(19\d{2})年.*?創業",
                # 일반적인 패턴들 (년도가 앞에 오는 경우)
                r"(\d{4})年.*?設立",
                r"(\d{4})年.*?創立", 
                r"(\d{4})年.*?創業",
                # 일반적인 패턴들 (설립이 앞에 오는 경우)
                r"設立.*?(\d{4})年",
                r"創立.*?(\d{4})年", 
                r"設立年.*?(\d{4})",
                r"Founded.*?(\d{4})",
                r"Established.*?(\d{4})",
                # 특수 패턴들
                r"会社設立.*?(\d{4})年",
                r"法人設立.*?(\d{4})年",
                r"設立登記.*?(\d{4})年",
                r"創業.*?(\d{4})年"
            ]
            
            # honbun 파일들에서 검색 (header 파일 + honbun 파일들)
            search_contents = [(filename, content)]
            
            # honbun 파일들도 추가
            honbun_files = self.extract_honbun_files(zip_content) if hasattr(self, 'extract_honbun_files') else []
            for honbun_filename, honbun_content in honbun_files[:5]:  # 처음 5개 파일만 검색
                search_contents.append((honbun_filename, honbun_content))
            
            for pattern in founded_patterns:
                for file_name, file_content in search_contents:
                    matches = re.findall(pattern, file_content, re.IGNORECASE)
                    if matches:
                        logger.info(f"설립년도 패턴 매치: {pattern} → {matches[:3]} (파일: {file_name})")
                        
                        # 가장 오래된 년도를 설립년도로 선택
                        valid_years = []
                        for match in matches:
                            year = int(match) if isinstance(match, str) else int(match[0])
                            if 1850 <= year <= submission_year:
                                valid_years.append(year)
                        
                        if valid_years:
                            founded_year = min(valid_years)  # 가장 오래된 년도
                            logger.info(f"honbun에서 설립년도 추출: {founded_year}년 (패턴: {pattern}, 파일: {file_name})")
                            return founded_year
            
            logger.info(f"설립년도 추출 실패 - 事業年度와 헤더 모두에서 발견 안됨 (파일: {filename})")
            return None
            
        except Exception as e:
            logger.warning(f"설립년도 추출 중 오류: {e} (파일: {filename})")
            return None
    
    def _extract_security_code(self, content: str, filename: str) -> Optional[str]:
        """상장번호 추출 (証券コード)"""
        patterns = [
            r"証券コード[^\d]*?(\d{4})",
            r"銘柄コード[^\d]*?(\d{4})",
            r"コード番号[^\d]*?(\d{4})"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                sec_code = match.group(1)
                logger.info(f"상장번호 추출: {sec_code} (파일: {filename})")
                return sec_code
        return None
    
    async def _translate_to_korean(self, japanese_text: str) -> str:
        """일본어 텍스트를 한글로 번역 (Google Translate API 사용)"""
        if not japanese_text:
            return ""
        
        # 먼저 하드코딩된 매핑 테이블에서 확인 (빠른 처리)
        translation_map = {
            # 회사 타입
            "株式会社": "",
            "ホールディングス": "홀딩스",
            "グループ": "그룹",
            "コーポレーション": "코퍼레이션",
            "インク": "",
            
            # 주요 회사명들
            "ソフトバンク": "소프트뱅크",
            "リクルート": "리쿠르트",
            "サイバーエージェント": "사이버에이전트",
            "メルカリ": "메르카리",
            "楽天": "라쿠텐",
            "ディー・エヌ・エー": "DeNA",
            "ソニー": "소니",
            "富士通": "후지쯔",
            "エヌ・ティ・ティ・データ": "NTT데이터",
            "ＬＩＮＥヤフー": "라인야후",
            
            # 기술/산업 용어들
            "テクノロジー": "테크놀로지",
            "システム": "시스템",
            "ソフトウェア": "소프트웨어",
            "デジタル": "디지털",
            "インターネット": "인터넷",
            "コンピューター": "컴퓨터",
            "ネットワーク": "네트워크",
            "サービス": "서비스",
            "ソリューション": "솔루션",
            "イノベーション": "이노베이션",
            
            # 일반적인 카타카나 단어들  
            "マーケティング": "마케팅",
            "コンサルティング": "컨설팅",
            "エンタテインメント": "엔터테인먼트",
            "プラットフォーム": "플랫폼",
            "メディア": "미디어",
            "ゲーム": "게임",
            "コンテンツ": "콘텐츠"
        }
        
        # 1. 매핑 테이블로 기본 번역 (빠른 처리)
        translated = japanese_text
        for jp, ko in sorted(translation_map.items(), key=lambda x: len(x[0]), reverse=True):
            translated = translated.replace(jp, ko)
        
        # 2. 매핑 테이블로만 충분히 번역된 경우 (일본어가 거의 남지 않음)
        # 원본과 동일하면 번역이 안된 것으로 간주하여 Google 번역 시도
        if translated != japanese_text and not re.search(r'[ひらがなカタカナ漢字]', translated):
            translated = re.sub(r'\s+', ' ', translated).strip()
            logger.info(f"매핑 테이블 번역: {japanese_text} → {translated}")
            return translated
        
        # 3. Google Translate API 사용 (일본어가 남아있는 경우)
        try:
            # Google Translate API 호출
            google_translated = await self._call_google_translate(japanese_text)
            if google_translated:
                translated = google_translated 
                logger.info(f"Google 번역: {japanese_text} → {translated}")
            else:
                # API 실패시 매핑 테이블 결과라도 사용
                translated = re.sub(r'\s+', ' ', translated).strip()
                logger.warning(f"Google 번역 실패, 매핑 테이블 사용: {japanese_text} → {translated}")
        except Exception as e:
            logger.error(f"번역 중 오류: {e}, 매핑 테이블 결과 사용")
            translated = re.sub(r'\s+', ' ', translated).strip()
        
        return translated.strip()
    
    async def _call_google_translate(self, text: str) -> Optional[str]:
        """Google Translate API 호출 (REST API 직접 사용)"""
        try:
            import aiohttp
            
            # API 키는 환경변수에서 가져오기
            google_api_key = os.getenv("GOOGLE_TRANSLATE_API_KEY")
            if not google_api_key:
                logger.warning("GOOGLE_TRANSLATE_API_KEY 환경변수가 설정되지 않음")
                return None
            
            # Google Translate REST API 호출
            url = "https://translation.googleapis.com/language/translate/v2"
            params = {
                'key': google_api_key,
                'q': text,
                'source': 'ja',
                'target': 'ko',
                'format': 'text'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=params) as response:
                    if response.status == 200:
                        result = await response.json()
                        if 'data' in result and 'translations' in result['data']:
                            translated_text = result['data']['translations'][0]['translatedText']
                            return translated_text
                    else:
                        logger.error(f"Google Translate API 오류: {response.status}")
                        return None
            
        except Exception as e:
            logger.error(f"Google Translate API 호출 실패: {e}")
            return None
    
    async def _get_market_cap(self, sec_code: str) -> Optional[int]:
        """Yahoo Finance에서 시가총액 가져오기"""
        if not sec_code or len(sec_code) != 4:
            return None
        
        try:
            import httpx
            
            url = f"https://finance.yahoo.co.jp/quote/{sec_code}.T"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                
                content = response.text
                logger.debug(f"Yahoo Finance 페이지 크기: {len(content)} 문자")
                
                # 시가총액 패턴 - 새로운 DOM 구조에 맞게 업데이트  
                # 구조: <span>時価総額</span>...<dd>...<span class="StyledNumber__value__3rXW">VALUE</span>...<span>百万円</span>
                pattern = r'時価総額.*?StyledNumber__value__[^>]*>([^<]+)</span>.*?百万円'
                
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    try:
                        value_str = match.group(1).replace(',', '').replace('.', '')
                        if value_str:
                            value = float(value_str) * 1_000_000  # 백만엔 → 엔
                            
                            # 합리적인 범위 체크 (1천억엔 ~ 1,000조엔)
                            if 100_000_000_000 <= value <= 1_000_000_000_000_000:
                                logger.info(f"시가총액 추출 성공: {sec_code} → {int(value):,}엔")
                                return int(value)
                                
                    except (ValueError, IndexError):
                        pass
                
                logger.warning(f"시가총액을 찾지 못함: {sec_code}")
                
        except Exception as e:
            logger.error(f"시가총액 추출 실패 ({sec_code}): {e}")
        
        return None    
    def _extract_value_from_context(self, content: str, keyword: str, filename: str) -> Optional[float]:
        """키워드 주변에서 수치 추출"""
        try:
            # 종업원수의 경우 단순히 값만 추출
            if "従業員数" in keyword or "NumberOfEmployees" in keyword:
                return self._extract_employee_count(content, filename)
            
            # 평균 연봉의 경우 정확한 원 단위 값 찾기  
            if "平均年間給与" in keyword or "AverageAnnualSalary" in keyword:
                return self._extract_annual_salary(content, filename)
            
            # 기타 키워드들 (근속연수, 나이)
            if "平均勤続年数" in keyword:
                pattern = rf"{keyword}[^0-9]*?([0-9]+\.?[0-9]*)\s*年"
            elif "平均年齢" in keyword:
                pattern = rf"{keyword}[^0-9]*?([0-9]+\.?[0-9]*)\s*歳"
            else:
                pattern = rf"{keyword}[^0-9]*?([0-9]+\.?[0-9]*)"
            
            match = re.search(pattern, content)
            if match:
                raw_value = match.group(1).replace(',', '')
                value = float(raw_value)
                logger.info(f"키워드 '{keyword}'에서 값 추출: {value} (파일: {filename})")
                return value
        except Exception as e:
            logger.error(f"값 추출 중 오류: {e}")
        return None
    
    def _select_best_value_by_context(self, results: List[dict]) -> dict:
        """contextRef 기준으로 최적의 값 선택"""
        if not results:
            return None
            
        if len(results) == 1:
            return results[0]
        
        logger.info(f"여러 값 중 최적 선택 - 총 {len(results)}개:")
        for i, result in enumerate(results):
            logger.info(f"  {i+1}: {result['value']} (contextRef: {result['contextRef']})")
        
        # 선택 기준:
        # 1. 전체/연결 기준 우선 (세그먼트 제외)
        # 2. 가장 최근 연도 (Current > Prior1 > Prior2...)
        # 3. 연결 기준 > 단독 기준
        
        # 컨텍스트별 우선순위 분류
        overall_results = []      # 전체/연결 (세그먼트 없음)
        segment_results = []      # 세그먼트별 
        non_consol_results = []   # 단독 기준
        
        for result in results:
            context_ref = result.get('contextRef') or ''
            
            # 세그먼트 멤버가 있는지 확인 (ReportableSegment, CorporateShared 등)
            if any(segment in context_ref for segment in ['ReportableSegment', 'Member']):
                if 'NonConsolidated' in context_ref:
                    non_consol_results.append(result)
                else:
                    segment_results.append(result)
            else:
                # 세그먼트가 없는 전체/연결 기준
                overall_results.append(result)
        
        # 우선순위: 전체/연결 > 세그먼트 > 단독
        candidates = overall_results if overall_results else (segment_results if segment_results else non_consol_results)
        
        if not candidates:
            candidates = results
        
        # 컨텍스트 우선순위에 따른 선택
        def get_context_priority(context_ref):
            """contextRef의 우선순위를 반환 (낮을수록 우선순위 높음)"""
            if 'CurrentYear' in context_ref:
                return 0  # 최고 우선순위
            elif 'Prior1Year' in context_ref:
                return 1
            elif 'Prior2Year' in context_ref:
                return 2
            elif 'Prior3Year' in context_ref:
                return 3
            elif 'Prior4Year' in context_ref:
                return 4
            else:
                return 999  # 알 수 없는 컨텍스트는 낮은 우선순위
        
        # 우선순위가 가장 높은 (숫자가 가장 낮은) 결과 선택
        best_result = min(candidates, key=lambda x: get_context_priority(x.get('contextRef', '')))
        
        logger.info(f"최종 선택: {best_result['value']} (contextRef: {best_result['contextRef']})")
        return best_result
    
    def _apply_ixbrl_attributes(self, result: dict, field: str) -> float:
        """scale/decimals/unitRef 속성을 적용하여 최종 값 계산"""
        try:
            value = float(str(result['value']).replace(',', ''))
        except (ValueError, AttributeError):
            logger.warning(f"값 변환 실패: {result['value']}")
            return 0.0
        
        scale = result.get('scale')
        decimals = result.get('decimals')
        unit_ref = result.get('unitRef')
        
        logger.info(f"속성 적용 전 - 값: {value}, scale: {scale}, decimals: {decimals}, unit: {unit_ref}")
        
        # scale 적용 (10^scale를 곱함)
        if scale is not None:
            try:
                scale_factor = int(scale)
                value *= (10 ** scale_factor)
                logger.info(f"scale {scale} 적용: {result['value']} → {value}")
            except ValueError:
                logger.warning(f"잘못된 scale 값: {scale}")
        
        # decimals 적용 (음수면 scale 효과, 양수면 소수점 자릿수)
        if decimals is not None:
            try:
                decimal_places = int(decimals)
                if decimal_places < 0:
                    # 음수 decimals는 10의 지수로 나눔 (scale과 비슷한 효과)
                    value /= (10 ** abs(decimal_places))
                    logger.info(f"decimals {decimals} 적용 (scale 효과): {result['value']} → {value}")
                elif decimal_places >= 0:
                    # 양수 decimals는 소수점 자릿수로 반올림
                    value = round(value, decimal_places)
                    logger.info(f"decimals {decimals} 적용 (반올림): 소수점 {decimal_places}자리")
            except ValueError:
                logger.warning(f"잘못된 decimals 값: {decimals}")
        
        # unitRef 검증 및 로깅
        if unit_ref:
            if field == 'avgAnnualSalaryJPY' and 'JPY' not in unit_ref.upper():
                logger.warning(f"예상과 다른 통화 단위: {unit_ref} (JPY 예상)")
            logger.info(f"단위 확인: {unit_ref}")
        
        # 필드별 타입 변환
        if field in ['avgAnnualSalaryJPY', 'employeeCount']:
            value = int(value)  # 정수 변환
        else:
            value = float(value)  # 소수점 유지
        
        logger.info(f"최종 적용된 값: {value}")
        return value
    
    def _extract_employee_count(self, content: str, filename: str) -> Optional[float]:
        """종업원수 추출 (단순 방식)"""
        try:
            logger.info(f"종업원수 추출 시작 - 파일: {filename}")
            
            # 단순 방식: 광범위한 패턴으로 모든 직원수를 찾고 가장 큰 값 선택
            patterns = [
                # 단순 숫자 + 人 패턴 (큰 숫자만)
                r"([0-9]{4,5}[,0-9]*)\s*人",
                # 테이블 형태 패턴  
                r"合計[^0-9]*?([0-9,]+)\s*人",
                r"計[^0-9]*?([0-9,]+)\s*人",
                # 일반적인 패턴들
                r"従業員数[^0-9]*?([0-9,]+)\s*人",
                r"従業員[^0-9]*?([0-9,]+)\s*人",
                r"NumberOfEmployees[^0-9]*?([0-9,]+)",
            ]
            
            all_matches = []
            
            for i, pattern in enumerate(patterns):
                matches = re.findall(pattern, content, re.DOTALL)
                if matches:
                    logger.info(f"패턴 {i+1}에서 매치 발견: {matches[:5]}")  # 처음 5개만 로그
                    all_matches.extend(matches)
            
            if all_matches:
                # 모든 매치에서 가장 큰 값 찾기 (합계값)
                max_value = 0
                for match in all_matches:
                    try:
                        # 문자열에서 숫자만 추출
                        clean_match = re.sub(r'[^0-9]', '', str(match))
                        if clean_match:
                            value = float(clean_match)
                            if value > max_value and value > 100:  # 100명 이상만
                                max_value = value
                    except:
                        continue
                
                if max_value > 0:
                    logger.info(f"최종 종업원수 추출: {max_value} (파일: {filename})")
                    return max_value
            
            logger.warning(f"종업원수를 찾을 수 없습니다 - 파일: {filename}")
                    
        except Exception as e:
            logger.error(f"종업원수 추출 중 오류: {e}")
        return None
    
    def _estimate_data_year(self, content: str, filename: str) -> int:
        """데이터 연도 추정"""
        # 1. 파일명에서 연도 추출 시도 (2025-03-31 같은 패턴)
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            year = int(year_match.group(1))
            if 2020 <= year <= 2030:  # 합리적인 범위
                logger.info(f"파일명에서 연도 추정: {year}")
                return year
        
        # 2. 내용에서 가장 최근 연도 찾기
        years = re.findall(r'(20[2-3][0-9])[年\s]', content)
        if years:
            recent_year = max([int(y) for y in years if 2020 <= int(y) <= 2030])
            logger.info(f"내용에서 연도 추정: {recent_year}")
            return recent_year
            
        # 3. 기본값: 현재 연도 - 1 (보고서는 보통 전년도 기준)
        default_year = datetime.now().year - 1
        logger.info(f"기본값으로 연도 추정: {default_year}")
        return default_year
    
    def _find_employee_data_year(self, content: str, employee_count: float) -> int:
        """실제 직원수와 연결된 연도 찾기"""
        try:
            # 직원수 값을 문자열로 변환 (콤마 포함/미포함 모두 고려)
            count_str = str(int(employee_count))
            count_with_comma = f"{int(employee_count):,}"
            
            logger.info(f"직원수 {employee_count}와 연결된 연도 찾기 시작")
            
            # 현재 시점에서 합리적한 연도 범위 (2020~2025)
            current_year = datetime.now().year
            valid_years = range(2020, current_year + 1)
            
            for year in sorted(valid_years, reverse=True):  # 최근 연도부터 찾기
                # 해당 연도와 직원수가 함께 나타나는 패턴들 찾기
                patterns = [
                    rf"{year}[年\s].*?{count_str}\s*人",
                    rf"{year}[年\s].*?{count_with_comma}\s*人",
                    rf"{year}.*?{count_str}\s*人",
                    rf"{year}.*?{count_with_comma}\s*人",
                    # 테이블 형태에서 년도와 직원수가 같은 행에 있는 경우
                    rf"{year}[^\n]*{count_str}[^\n]*人",
                    rf"{year}[^\n]*{count_with_comma}[^\n]*人"
                ]
                
                for pattern in patterns:
                    if re.search(pattern, content, re.DOTALL):
                        logger.info(f"직원수 {employee_count}가 {year}년과 연결됨")
                        return year
            
            # 직접적인 연결이 없으면 최신 연도 (2025년 또는 2024년)
            default_year = current_year  # 최신 연도로 설정
            logger.info(f"연결된 연도 없음, 최신 연도로 설정: {default_year}")
            return default_year
                    
        except Exception as e:
            logger.error(f"연도 찾기 중 오류: {e}")
            return datetime.now().year  # 최신 연도
    
    def _extract_annual_salary(self, content: str, filename: str) -> Optional[float]:
        """평균 연봉 추출 (엔 단위)"""
        try:
            # 11,453,407 같은 정확한 엔 단위 값 찾기
            patterns = [
                r"平均年間給与[^0-9]*?([0-9,]+)\s*円",  # 일본어 + 円
                r"AverageAnnualSalary[^0-9]*?([0-9,]+)",  # 영문
                r"平均年間給与[^0-9]*?([0-9,]+)\s*万円", # 만원 단위
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content)
                if match:
                    raw_value = match.group(1).replace(',', '')
                    value = float(raw_value)
                    
                    # 만엔 단위인 경우 엔으로 변환
                    if "万円" in pattern:
                        value *= 10000
                    
                    logger.info(f"연봉 추출: {value} (파일: {filename}, 패턴: {pattern})")
                    return value
                    
        except Exception as e:
            logger.error(f"연봉 추출 중 오류: {e}")
        return None
    
    def _extract_value_from_ixbrl_concept(self, content: str, concept_name: str, filename: str) -> Optional[List[dict]]:
        """iXBRL 개념(name 속성)에서 수치 추출"""
        try:
            # 전체 ix:nonFraction 태그 매칭 (속성과 값 모두 포함)
            full_pattern = rf'<ix:nonFraction([^>]*name="{re.escape(concept_name)}"[^>]*)>([^<]+)</ix:nonFraction>'
            full_matches = re.findall(full_pattern, content)
            
            if full_matches:
                logger.info(f"개념 '{concept_name}'에서 {len(full_matches)}개 값 발견: {[m[1] for m in full_matches[:3]]}...")
                
                results = []
                for attrs, value_text in full_matches:
                    try:
                        # 숫자 값 추출 (콤마 제거)
                        clean_value = re.sub(r'[,\s]', '', value_text)
                        if not re.match(r'^-?[\d.]+$', clean_value):
                            continue
                            
                        value = float(clean_value)
                        
                        # 속성들 파싱 (전체 속성 문자열에서 추출)
                        result = {
                            'value': value,
                            'raw_text': value_text,
                            'contextRef': self._extract_attribute(attrs, 'contextRef'),
                            'unitRef': self._extract_attribute(attrs, 'unitRef'),
                            'scale': self._extract_attribute(attrs, 'scale'),
                            'decimals': self._extract_attribute(attrs, 'decimals'),
                            'concept': concept_name,
                            'file': filename
                        }
                        results.append(result)
                        
                        # 디버깅용 로그
                        logger.debug(f"추출된 값: {value}, contextRef: {result['contextRef']}, unitRef: {result['unitRef']}")
                        
                    except ValueError as e:
                        logger.debug(f"값 변환 실패: {value_text} - {e}")
                        continue
                
                if results:
                    logger.info(f"개념 '{concept_name}'에서 {len(results)}개 유효한 값 추출")
                    return results
                    
        except Exception as e:
            logger.error(f"iXBRL 개념 추출 중 오류: {e}")
        return None
    
    def _extract_attribute(self, attrs_string: str, attr_name: str) -> Optional[str]:
        """속성 문자열에서 특정 속성값 추출"""
        try:
            pattern = rf'{attr_name}="([^"]*)"'
            match = re.search(pattern, attrs_string)
            return match.group(1) if match else None
        except:
            return None



class CompanyReportUpdater:
    """기업 리포트 최신화 관리자"""
    
    def __init__(self):
        # 타겟 기업들과 매칭 키워드 (모두 본사만 정확히 매칭)
        self.target_companies = {
            "rakuten": ["楽天グループ株式会社"],
            "mercari": ["株式会社メルカリ"],  
            "cyberagent": ["株式会社サイバーエージェント"],
            "lineyahoo": ["ＬＩＮＥヤフー株式会社"],
            "recruit": ["株式会社リクルートホールディングス"],
            "dena": ["株式会社ディー・エヌ・エー"],
            "sony": ["ソニーグループ株式会社"],
            "softbank": ["ソフトバンク株式会社"],
            "fujitsu": ["富士通株式会社"],  # 본사로 변경
            "nttdata": ["株式会社ＮＴＴデータグループ"]
        }
        
        # 상태 저장 파일 경로
        self.state_file = Path("data/last_check_dates.json")
        self.discovered_reports = {}  # company_key -> 최신 문서 정보
    
    def load_last_check_dates(self) -> Dict[str, str]:
        """마지막 체크 날짜 로드"""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"상태 파일 로드 실패: {e}")
        
        # 기본값: 18개월 전부터 시작
        default_date = (datetime.now() - timedelta(days=18*30)).strftime("%Y-%m-%d")
        return {company_key: default_date for company_key in self.target_companies.keys()}
    
    def save_last_check_dates(self, dates: Dict[str, str]):
        """마지막 체크 날짜 저장"""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(dates, f, indent=2)
        except Exception as e:
            logger.error(f"상태 파일 저장 실패: {e}")
    
    def match_company(self, company_name: str) -> Optional[str]:
        """회사명으로 타겟 기업 매칭"""
        for company_key, keywords in self.target_companies.items():
            for keyword in keywords:
                if keyword in company_name:
                    return company_key
        return None
    
    def date_range(self, start_date: datetime, end_date: datetime):
        """날짜 범위 생성 (역순)"""
        current_date = end_date
        while current_date >= start_date:
            yield current_date.strftime("%Y-%m-%d")
            current_date -= timedelta(days=1)
    
    async def scan_date_for_reports(self, date: str, api: EdinetAPI) -> List[CompanyDocument]:
        """특정 날짜에서 타겟 기업들의 유가증권보고서 검색"""
        documents = await api.get_document_list(date)
        found_reports = []
        
        for doc in documents:
            # 유가증권보고서만 (docTypeCode: 120)
            if doc.get("docTypeCode") != "120":
                continue
            
            company_name = doc.get("filerName", "")
            company_key = self.match_company(company_name)
            
            if company_key:
                report = CompanyDocument(
                    document_id=doc.get("docID"),
                    company_name=company_name,
                    submitted_date=date,
                    doc_type="120",
                    company_key=company_key,
                    sec_code=doc.get("secCode")[:4] if doc.get("secCode") else None  # EDINET API 응답에서 secCode 앞 4자리만 추가
                )
                found_reports.append(report)
                logger.info(f"유가증권보고서 발견: {company_name} ({date}) - {doc.get('docID')}")
        
        return found_reports
    
    async def find_latest_reports(self, months_back: int = 18) -> Dict[str, CompanyDocument]:
        """최신 유가증권보고서들 검색"""
        logger.info(f"최근 {months_back}개월간 유가증권보고서 검색 시작...")
        
        # 날짜 범위 설정
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months_back * 30)
        
        # 각 회사별 최신 리포트 저장
        latest_reports = {}
        dates_scanned = 0
        
        async with EdinetAPI() as api:
            # 날짜별 스캔 (최신 날짜부터)
            for date_str in self.date_range(start_date, end_date):
                dates_scanned += 1
                
                # 진행률 표시
                if dates_scanned % 30 == 0:
                    logger.info(f"진행률: {dates_scanned}일 스캔 완료 ({date_str})")
                
                # 해당 날짜의 리포트들 검색
                found_reports = await self.scan_date_for_reports(date_str, api)
                
                # 각 회사별로 가장 최근 리포트 업데이트
                for report in found_reports:
                    company_key = report.company_key
                    
                    # 더 최신 리포트이거나 처음 발견한 경우
                    if (company_key not in latest_reports or 
                        report.submitted_date > latest_reports[company_key].submitted_date):
                        
                        latest_reports[company_key] = report
                        logger.info(f"✨ {company_key} 최신 리포트 업데이트: {report.submitted_date}")
                
                # 모든 회사의 리포트를 찾았으면 조기 종료
                if len(latest_reports) == len(self.target_companies):
                    logger.info(f"🎉 모든 회사의 리포트 발견! {dates_scanned}일 스캔으로 완료")
                    break
                
                # API 부하 방지를 위한 딜레이
                await asyncio.sleep(0.1)
        
        # 결과 요약
        logger.info(f"\n📊 최신화 결과 요약:")
        logger.info(f"- 스캔 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"- 스캔 일수: {dates_scanned}일")
        logger.info(f"- 발견한 회사: {len(latest_reports)}/{len(self.target_companies)}개")
        
        for company_key, report in latest_reports.items():
            logger.info(f"  • {company_key}: {report.company_name} ({report.submitted_date})")
        
        # 못 찾은 회사들
        missing_companies = set(self.target_companies.keys()) - set(latest_reports.keys())
        if missing_companies:
            logger.warning(f"❌ 리포트를 찾지 못한 회사들: {missing_companies}")
        
        return latest_reports
    
    async def update_company_data(self, company_key: str, document: CompanyDocument) -> bool:
        """특정 회사의 데이터 업데이트"""
        logger.info(f"🔄 {company_key} 데이터 업데이트 시작...")
        
        try:
            async with EdinetAPI() as api:
                # 문서 다운로드
                zip_content = await api.get_document_package(document.document_id, doc_type="1")
                
                if zip_content:
                    # honbun 파일 추출 및 파싱
                    honbun_files = api.extract_honbun_files(zip_content)
                    
                    if honbun_files:
                        # 인사 정보 및 기본 정보 파싱
                        employee_info = api.parse_employee_info(honbun_files)
                        basic_info = api.parse_basic_info(honbun_files, zip_content)
                        
                        # EdinetData 객체 생성
                        edinet_data = EdinetData()
                        
                        # 기본 정보 설정
                        edinet_data.basic = EdinetBasic()
                        edinet_data.basic.name = basic_info.get("name") or document.company_name
                        edinet_data.basic.name_en = basic_info.get("name_en", "")
                        edinet_data.basic.name_ko = await api._translate_to_korean(edinet_data.basic.name) if edinet_data.basic.name else ""
                        edinet_data.basic.headquarters = basic_info.get("headquarters", "")  # 일본어 원본
                        # 본사 주소 한글 번역
                        edinet_data.basic.headquarters_ko = await api._translate_to_korean(edinet_data.basic.headquarters) if edinet_data.basic.headquarters else ""
                        # 본사 주소 영문 번역 (추후 구현 가능)
                        edinet_data.basic.headquarters_en = ""  # 현재는 빈 값
                        # founded_year 직접 설정
                        edinet_data.basic.founded_year = basic_info.get("founded_year")
                        # CompanyDocument에서 secCode가 있으면 우선 사용, 없으면 문서에서 추출한 것 사용
                        edinet_data.basic.sec_code = document.sec_code or basic_info.get("sec_code")
                        
                        # 시가총액 가져오기 (상장번호가 있는 경우)
                        if edinet_data.basic.sec_code:
                            market_cap = await api._get_market_cap(edinet_data.basic.sec_code)
                            edinet_data.basic.market_cap = market_cap
                        edinet_data.basic.employee_count = employee_info.get("employeeCount")
                        
                        # HR 정보 설정
                        edinet_data.hr.avgTenureYears = employee_info.get("avgTenureYears")
                        edinet_data.hr.avgAgeYears = employee_info.get("avgAgeYears")
                        edinet_data.hr.avgAnnualSalaryJPY = employee_info.get("avgAnnualSalaryJPY")
                        
                        # 재무 정보 설정
                        edinet_data.financials = EdinetFinancials()
                        edinet_data.financials.fiscalYear = datetime.now().year
                        
                        # 출처 정보 설정
                        edinet_data.provenance = {
                            "source": "EDINET API v2",
                            "document_id": document.document_id,
                            "submitted_date": document.submitted_date,
                            "fetched_at": datetime.now().isoformat(),
                            "company_key": company_key,
                            "employee_info_provenance": employee_info.get("provenance", {})
                        }
                        
                        # 데이터 저장
                        await self.save_company_data(company_key, edinet_data)
                        
                        logger.info(f"✅ {company_key} 업데이트 완료!")
                        return True
                    else:
                        logger.error(f"❌ {company_key} honbun 파일 추출 실패")
                else:
                    logger.error(f"❌ {company_key} 문서 다운로드 실패")
                    
        except Exception as e:
            logger.error(f"❌ {company_key} 업데이트 중 오류: {e}")
        
        return False
    
    async def save_company_data(self, company_key: str, edinet_data: EdinetData):
        """회사 데이터 저장"""
        output_path = Path(f"data/edinet_reports/{company_key}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # EdinetData를 dict로 변환하여 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(edinet_data.model_dump(), f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"💾 {company_key} 데이터 저장: {output_path}")
    
    async def run_full_update(self) -> Dict[str, bool]:
        """전체 최신화 프로세스 실행"""
        logger.info("🚀 EDINET 유가증권보고서 최신화 시작...")
        
        # 1. 최신 리포트들 검색
        latest_reports = await self.find_latest_reports()
        
        if not latest_reports:
            logger.warning("검색된 리포트가 없습니다.")
            return {}
        
        # 2. 각 회사별 데이터 업데이트
        update_results = {}
        
        for company_key, document in latest_reports.items():
            logger.info(f"\n📋 {company_key} 처리 중...")
            success = await self.update_company_data(company_key, document)
            update_results[company_key] = success
            
            # API 부하 방지
            await asyncio.sleep(2.0)
        
        # 3. 결과 요약
        successful_updates = sum(update_results.values())
        logger.info(f"\n🎊 최신화 완료!")
        logger.info(f"성공: {successful_updates}/{len(update_results)}개 회사")
        
        # 성공한 회사들
        for company_key, success in update_results.items():
            status = "✅" if success else "❌"
            logger.info(f"  {status} {company_key}")
        
        return update_results


async def fetch_edinet_data(company_code: str) -> EdinetData:
    """EDINET에서 기업 데이터 종합 수집"""
    edinet_data = EdinetData()
    
    async with EdinetAPI() as api:
        # 1. 유가증권보고서 패키지 다운로드 (기존 호환성을 위해 document_id로 처리)
        zip_content = await api.get_document_package(company_code)
        if zip_content:
            # 2. honbun 파일들 추출
            honbun_files = api.extract_honbun_files(zip_content)
            
            if honbun_files:
                # 3. 기본 정보 파싱 (설립년도, 회사명, 본사 주소 등)
                basic_info = api.parse_basic_info(honbun_files, zip_content)
                
                # 4. 인사 정보 파싱
                employee_info = api.parse_employee_info(honbun_files)
                
                # 5. EdinetBasic 객체에 저장
                edinet_data.basic.name = basic_info.get("name", "")
                edinet_data.basic.name_en = basic_info.get("name_en", "")
                edinet_data.basic.headquarters = basic_info.get("headquarters", "")
                edinet_data.basic.founded_year = basic_info.get("founded_year")
                edinet_data.basic.sec_code = basic_info.get("sec_code")
                edinet_data.basic.employee_count = employee_info["employeeCount"]
                
                # 6. EdinetHR 객체에 저장
                edinet_data.hr.avgTenureYears = employee_info["avgTenureYears"]
                edinet_data.hr.avgAgeYears = employee_info["avgAgeYears"]
                edinet_data.hr.avgAnnualSalaryJPY = employee_info["avgAnnualSalaryJPY"]
                
                # 7. 출처 정보 저장
                edinet_data.provenance = {
                    "source": "EDINET API v2",
                    "company_code": company_code,
                    "fetched_at": datetime.now().isoformat(),
                    "basic_info_provenance": basic_info.get("provenance", {}),
                    "employee_info_provenance": employee_info.get("provenance", {})
                }
            else:
                logger.warning("honbun 파일을 찾을 수 없습니다.")
        else:
            logger.warning("유가증권보고서 패키지를 다운로드할 수 없습니다.")
    
    # 재무 정보 설정
    edinet_data.financials = EdinetFinancials()
    edinet_data.financials.fiscalYear = datetime.now().year
    
    return edinet_data

# 테스트용 함수
async def test_edinet_api():
    """EDINET API 테스트"""
    company_code = "S100VZG5"  # 리쿠르트 홀딩스
    
    print("EDINET API 테스트 시작...")
    data = await fetch_edinet_data(company_code)
    
    print(f"기업명: {data.basic.name}")
    print(f"본점: {data.basic.headquarters}")
    print(f"직원 수: {data.basic.employee_count}")
    print(f"평균 근속연수: {data.hr.avgTenureYears}")
    print(f"평균 연령: {data.hr.avgAgeYears}")
    print(f"평균 연봉: {data.hr.avgAnnualSalaryJPY}")
    print(f"출처: {data.provenance}")
    
    return data

# 최신화 실행 함수
async def run_edinet_update():
    """EDINET 최신화 실행"""
    updater = CompanyReportUpdater()
    results = await updater.run_full_update()
    return results

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "update":
        # python -m src.jap_syu.utils.edinet update
        asyncio.run(run_edinet_update())
    else:
        # 기존 테스트 실행
        asyncio.run(test_edinet_api())
