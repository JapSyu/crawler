#!/usr/bin/env python3
"""
전체 회사 정보 업데이트 스크립트
EDINET 데이터 + 로고 정보를 한 번에 수집하여 RDS에 저장합니다.
"""

import asyncio
import sys
import os
from pathlib import Path
from loguru import logger

# 프로젝트 루트를 Python path에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.jap_syu.utils.edinet import CompanyReportUpdater
from src.jap_syu.utils.logo_fetcher import CompanyLogoUpdater
from src.jap_syu.utils.database import DatabaseManager

async def update_all_company_data():
    """전체 회사 데이터 업데이트 (EDINET + 로고)"""
    logger.info("🚀 전체 회사 정보 업데이트 시작...")

    # 1. 먼저 데이터베이스 테이블 생성/업데이트 (로고 컬럼 포함)
    try:
        async with DatabaseManager() as db:
            await db.create_tables()
            logger.info("✅ 데이터베이스 테이블 준비 완료")
    except Exception as e:
        logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
        return False

    # 2. EDINET 데이터 최신화
    logger.info("📊 EDINET 데이터 업데이트 시작...")
    edinet_updater = CompanyReportUpdater()
    edinet_results = await edinet_updater.run_full_update()

    # 3. 로고 정보 업데이트
    logger.info("🎨 로고 정보 업데이트 시작...")
    logo_updater = CompanyLogoUpdater()
    logo_results = await logo_updater.update_all_company_logos()

    # 4. 결과 요약
    logger.info("\n" + "="*50)
    logger.info("📊 전체 업데이트 결과 요약")
    logger.info("="*50)

    all_companies = set(edinet_results.keys()) | set(logo_results.keys())

    for company_key in sorted(all_companies):
        edinet_status = "✅" if edinet_results.get(company_key, False) else "❌"
        logo_status = "✅" if logo_results.get(company_key, False) else "❌"

        logger.info(f"{company_key:12} | EDINET: {edinet_status} | 로고: {logo_status}")

    edinet_success = sum(edinet_results.values())
    logo_success = sum(logo_results.values())
    total_companies = len(all_companies)

    logger.info("-" * 50)
    logger.info(f"EDINET 성공률: {edinet_success}/{total_companies} ({edinet_success/total_companies*100:.1f}%)")
    logger.info(f"로고 성공률: {logo_success}/{total_companies} ({logo_success/total_companies*100:.1f}%)")
    logger.info("="*50)

    return True

async def update_logos_only():
    """로고 정보만 업데이트"""
    logger.info("🎨 로고 정보만 업데이트 시작...")

    # 데이터베이스 테이블 준비
    try:
        async with DatabaseManager() as db:
            await db.create_tables()
    except Exception as e:
        logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
        return False

    # 로고 정보 업데이트
    logo_updater = CompanyLogoUpdater()
    results = await logo_updater.update_all_company_logos()

    return results

async def update_edinet_only():
    """EDINET 정보만 업데이트"""
    logger.info("📊 EDINET 정보만 업데이트 시작...")

    # 데이터베이스 테이블 준비
    try:
        async with DatabaseManager() as db:
            await db.create_tables()
    except Exception as e:
        logger.error(f"❌ 데이터베이스 초기화 실패: {e}")
        return False

    # EDINET 정보 업데이트
    edinet_updater = CompanyReportUpdater()
    results = await edinet_updater.run_full_update()

    return results

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "all":
            # 전체 업데이트
            asyncio.run(update_all_company_data())
        elif command == "edinet":
            # EDINET만 업데이트
            asyncio.run(update_edinet_only())
        elif command == "logo":
            # 로고만 업데이트
            asyncio.run(update_logos_only())
        else:
            print("사용법:")
            print("  python update_all_companies.py all      # 전체 업데이트")
            print("  python update_all_companies.py edinet   # EDINET만 업데이트")
            print("  python update_all_companies.py logo     # 로고만 업데이트")
    else:
        # 기본: 전체 업데이트
        asyncio.run(update_all_company_data())