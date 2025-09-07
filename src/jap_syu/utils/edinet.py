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
EDINET_API_KEY = os.getenv("EDINET_API_KEY", "your-api-key-here")

@dataclass
class CompanyDocument:
    """발견된 기업 문서 정보"""
    document_id: str
    company_name: str 
    submitted_date: str
    doc_type: str
    company_key: str  # 내부 식별용

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
            # 従業員の状況 섹션이 있는지 확인
            if "従業員の状況" not in content:
                continue
            
            logger.info(f"従業員の状況 섹션 발견: {filename}")
            
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
        
        # 가장 최적 값 선택 (여러 기준 적용)
        best_result = candidates[0]
        
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
                            if value > max_value and value > 1000:  # 1000명 이상만
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
        """평균 연봉 추출 (원 단위)"""
        try:
            # 11,453,407 같은 정확한 원 단위 값 찾기
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
                    
                    # 만원 단위인 경우 원으로 변환
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

def parse_edinet_basic_info(company_code: str) -> EdinetBasic:
    """EDINET 기업 기본 정보 파싱 (현재는 코드만 저장)"""
    basic = EdinetBasic()
    basic.name = "リクルートホールディングス"  # 기본값
    basic.address = "東京都千代田区丸の内1-9-1"  # 기본값
    return basic

def parse_edinet_financials(company_code: str) -> EdinetFinancials:
    """EDINET 재무 정보 파싱 (현재는 기본값)"""
    financials = EdinetFinancials()
    financials.fiscalYear = datetime.now().year
    return financials

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
            "softbank": ["ソフトバンクグループ株式会社"],
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
                    company_key=company_key
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
                        employee_info = api.parse_employee_info(honbun_files)
                        
                        # EdinetData 객체 생성
                        edinet_data = EdinetData()
                        
                        # 기본 정보 설정
                        edinet_data.basic = EdinetBasic()
                        edinet_data.basic.name = document.company_name
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
                # 3. 인사 정보 파싱
                employee_info = api.parse_employee_info(honbun_files)
                
                # 4. EdinetHR 객체에 저장
                edinet_data.hr.avgTenureYears = employee_info["avgTenureYears"]
                edinet_data.hr.avgAgeYears = employee_info["avgAgeYears"]
                edinet_data.hr.avgAnnualSalaryJPY = employee_info["avgAnnualSalaryJPY"]
                
                # 5. 출처 정보 저장
                edinet_data.provenance = {
                    "source": "EDINET API v2",
                    "company_code": company_code,
                    "fetched_at": datetime.now().isoformat(),
                    "employee_info_provenance": employee_info.get("provenance", {})
                }
                
                # 6. 기본 정보 설정 (먼저 기본값 설정 후 직원수 덮어쓰기)
                edinet_data.basic = parse_edinet_basic_info(company_code)
                edinet_data.basic.employee_count = employee_info["employeeCount"]
            else:
                logger.warning("honbun 파일을 찾을 수 없습니다.")
                edinet_data.basic = parse_edinet_basic_info(company_code)
        else:
            logger.warning("유가증권보고서 패키지를 다운로드할 수 없습니다.")
            edinet_data.basic = parse_edinet_basic_info(company_code)
    
    # 재무 정보 설정
    edinet_data.financials = parse_edinet_financials(company_code)
    
    return edinet_data

# 테스트용 함수
async def test_edinet_api():
    """EDINET API 테스트"""
    company_code = "S100VZG5"  # 리쿠르트 홀딩스
    
    print("EDINET API 테스트 시작...")
    data = await fetch_edinet_data(company_code)
    
    print(f"기업명: {data.basic.name}")
    print(f"주소: {data.basic.address}")
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
