#!/usr/bin/env python3
"""설립년도 추출 테스트"""

import asyncio
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python path에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.jap_syu.utils.edinet import EdinetAPI

async def test_founded_year_extraction():
    """설립년도 추출 테스트"""
    print("🧪 설립년도 추출 로직 테스트")
    
    edinet = EdinetAPI()
    
    # Recruit Holdings 문서 다운로드
    doc_id = "S100VZG5"  # Recruit Holdings 최신 문서
    
    print(f"📥 문서 다운로드 중: {doc_id}")
    zip_data = await edinet.get_document_package(doc_id)
    
    if not zip_data:
        print("❌ 문서 다운로드 실패")
        return
    
    # ZIP 압축 해제
    import zipfile
    import io
    
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        # honbun 파일 찾기
        honbun_files = [f for f in zf.namelist() if 'honbun' in f and f.endswith('.htm')]
        
        if not honbun_files:
            print("❌ honbun 파일을 찾을 수 없음")
            return
        
        # 첫 번째 honbun 파일 읽기
        filename = honbun_files[0]
        print(f"📄 분석 파일: {filename}")
        
        with zf.open(filename) as f:
            content = f.read().decode('utf-8')
        
        # 사업년도 패턴 검색
        import re
        
        print(f"📊 문서 크기: {len(content):,}자")
        print(f"📊 헤더 내용 (처음 2000자):")
        print("-" * 50)
        header = content[:2000]
        print(header)
        print("-" * 50)
        
        # 사업년도 패턴 테스트
        business_year_patterns = [
            r"第(\d+)期.*?事業年度",
            r"第(\d+)期",
            r"(\d+)期.*?事業年度",
            r"事業年度.*?第(\d+)期"
        ]
        
        print("🔍 사업년도 패턴 검색:")
        for pattern in business_year_patterns:
            matches = re.findall(pattern, content[:10000])
            if matches:
                print(f"  ✅ 패턴 '{pattern}': {matches}")
            else:
                print(f"  ❌ 패턴 '{pattern}': 매치 없음")
        
        # 년도 패턴 검색
        print("\n📅 년도 패턴 검색:")
        year_patterns = [
            r"(\d{4})年.*?月.*?日",
            r"(\d{4})年",
            r"(\d{4})"
        ]
        
        for pattern in year_patterns[:1]:  # 첫 번째만
            matches = re.findall(pattern, content[:5000])
            if matches:
                print(f"  ✅ 패턴 '{pattern}': {matches[:10]}")  # 처음 10개만
        
        # 설립년도 추출 테스트
        print("\n🧮 설립년도 계산 시뮬레이션:")
        period_match = re.search(r"第(\d+)期", content[:10000])
        year_match = re.search(r"(\d{4})年", content[:5000])
        
        if period_match and year_match:
            period = int(period_match.group(1))
            current_year = int(year_match.group(1))
            founded_year = current_year - period + 1
            
            print(f"  📋 사업년도: 제{period}기")
            print(f"  📅 기준년도: {current_year}년")
            print(f"  🎯 계산된 설립년도: {founded_year}년")
            
            if 1850 <= founded_year <= 2010:
                print(f"  ✅ 유효한 설립년도입니다!")
            else:
                print(f"  ❌ 범위를 벗어난 설립년도입니다 (1850-2010)")
        else:
            print("  ❌ 계산에 필요한 정보를 찾을 수 없음")

if __name__ == "__main__":
    asyncio.run(test_founded_year_extraction())