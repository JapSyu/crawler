#!/usr/bin/env python3
"""사업년도 패턴 테스트"""

import asyncio
import sys
import os
import re
from pathlib import Path

# 프로젝트 루트를 Python path에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.jap_syu.utils.edinet import EdinetAPI

# 테스트할 회사들 (일부만)
TEST_COMPANIES = {
    "recruit": "S100VZG5",
    "softbank": "S100W3P2",
    "cyberagent": "S100VZI4"
}

async def test_business_year_patterns():
    """사업년도 패턴 테스트"""
    print("🧪 사업년도 패턴 테스트 시작")
    
    async with EdinetAPI() as api:
        for company_name, doc_id in TEST_COMPANIES.items():
            print(f"\n📋 {company_name} ({doc_id}) 테스트 중...")
            
            try:
                # 1. 최신 문서 ID 가져오기
                latest_docs = await api.get_latest_document_list([company_name])
                if company_name in latest_docs:
                    actual_doc_id = latest_docs[company_name]["docID"]
                    print(f"📄 최신 문서 ID: {actual_doc_id}")
                else:
                    actual_doc_id = doc_id
                    print(f"📄 기본 문서 ID 사용: {actual_doc_id}")
                
                # 2. 문서 다운로드
                zip_content = await api.get_document_package(actual_doc_id)
                if not zip_content:
                    print(f"❌ {company_name}: 문서 다운로드 실패")
                    continue
                
                # 3. honbun 파일 추출
                honbun_files = api.extract_honbun_files(zip_content)
                if not honbun_files:
                    print(f"❌ {company_name}: honbun 파일 없음")
                    continue
                
                # 4. 첫 번째 honbun 파일에서 사업년도 패턴 테스트
                filename, content = honbun_files[0]
                print(f"📄 분석 파일: {filename}")
                print(f"📊 문서 크기: {len(content):,}자")
                
                # 헤더 부분 출력 (처음 1000자)
                header = content[:1000]
                print(f"📋 문서 헤더 (처음 1000자):")
                print("-" * 50)
                print(header)
                print("-" * 50)
                
                # 사업년도 패턴 테스트
                business_year_patterns = [
                    r"第(\d+)期.*?事業年度",
                    r"第(\d+)期",
                    r"(\d+)期.*?事業年度",
                    r"事業年度.*?第(\d+)期"
                ]
                
                print("\n🔍 사업년도 패턴 검색:")
                found_period = None
                for pattern in business_year_patterns:
                    matches = re.findall(pattern, content[:10000])
                    if matches:
                        print(f"  ✅ 패턴 '{pattern}': {matches}")
                        if not found_period:
                            found_period = int(matches[0])
                    else:
                        print(f"  ❌ 패턴 '{pattern}': 매치 없음")
                
                # 년도 패턴 검색
                print("\n📅 년도 패턴 검색:")
                year_patterns = [r"(\d{4})年.*?月.*?日", r"(\d{4})年"]
                found_year = None
                for pattern in year_patterns:
                    matches = re.findall(pattern, content[:5000])
                    if matches:
                        print(f"  ✅ 패턴 '{pattern}': {matches[:5]}")  # 처음 5개만
                        if not found_year:
                            # 2020년 이후의 년도 찾기
                            for year in matches:
                                year_int = int(year)
                                if 2020 <= year_int <= 2025:
                                    found_year = year_int
                                    break
                    else:
                        print(f"  ❌ 패턴 '{pattern}': 매치 없음")
                
                # 설립년도 계산
                print("\n🧮 설립년도 계산:")
                if found_period and found_year:
                    founded_year = found_year - found_period + 1
                    print(f"  📋 사업년도: 제{found_period}기")
                    print(f"  📅 기준년도: {found_year}년")
                    print(f"  🎯 계산된 설립년도: {founded_year}년")
                    
                    if 1850 <= founded_year <= 2010:
                        print(f"  ✅ 유효한 설립년도입니다!")
                    else:
                        print(f"  ❌ 범위를 벗어난 설립년도입니다 (1850-2010)")
                else:
                    print(f"  ❌ 계산에 필요한 정보를 찾을 수 없음")
                    print(f"     사업년도: {found_period}, 기준년도: {found_year}")
                
            except Exception as e:
                print(f"❌ {company_name} 처리 중 오류: {e}")
                continue

if __name__ == "__main__":
    asyncio.run(test_business_year_patterns())