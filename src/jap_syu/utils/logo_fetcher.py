"""
Logo.dev API 유틸리티
logo.dev 서비스를 사용하여 기업 로고 정보를 가져오고 데이터베이스에 저장하는 기능을 제공합니다.
"""

import os
import asyncio
import httpx
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime
from loguru import logger
from pathlib import Path
import json
from urllib.parse import urlparse

# .env 파일 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class LogoFetcher:
    """Logo.dev API를 사용한 로고 수집기"""

    def __init__(self, api_key: Optional[str] = None):
        """
        Args:
            api_key: Logo.dev API 키 (없으면 rate limit 적용)
        """
        self.api_key = api_key or os.getenv("LOGO_DEV_API_KEY")
        self.search_url = "https://api.logo.dev/search"
        self.session = None

    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        self.session = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self.session:
            await self.session.aclose()
        
    async def search_logo_by_query(self, query: str) -> Optional[Dict[str, Any]]:
        """
        검색어로 로고 검색

        Args:
            query: 검색어 (회사명 또는 도메인)

        Returns:
            로고 정보 딕셔너리 또는 None
        """
        try:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            params = {"q": query}

            logger.info(f"🔍 로고 검색 시작: '{query}'")

            response = await self.session.get(
                self.search_url,
                params=params,
                headers=headers,
                timeout=10
            )

            logger.info(f"📡 검색 응답 상태: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                logger.info(f"📄 응답 데이터 구조: {type(data)}")
                logger.info(f"📄 응답 데이터 (처음 500자): {str(data)[:500]}")

                # 응답이 리스트인지 딕셔너리인지 확인
                if isinstance(data, list):
                    # 응답이 리스트인 경우
                    results = data
                elif isinstance(data, dict):
                    # 응답이 딕셔너리인 경우
                    results = data.get("results", data.get("data", []))
                else:
                    logger.error(f"예상하지 못한 응답 타입: {type(data)}")
                    return None

                logger.info(f"📄 검색 결과: {len(results)}개 발견")

                if results and len(results) > 0:
                    # 첫 번째 결과 사용
                    result = results[0]
                    logger.info(f"📄 첫 번째 결과: {result}")

                    logo_info = {
                        "logo_url": result.get("logo_url") if isinstance(result, dict) else result,
                        "company_name": result.get("name", result.get("company_name", "")) if isinstance(result, dict) else "",
                        "domain": result.get("domain", result.get("website", "")) if isinstance(result, dict) else "",
                        "confidence_score": result.get("score", result.get("confidence", 0.0)) if isinstance(result, dict) else 0.0,
                        "source": "logo.dev",
                        "fetched_at": datetime.now().isoformat(),
                        "search_query": query,
                        "api_key_used": bool(self.api_key)
                    }

                    logger.info(f"✅ 로고 발견: {logo_info.get('company_name')} - {logo_info.get('logo_url')}")
                    return logo_info
                else:
                    logger.warning(f"검색 결과 없음: {query}")
                    return None

            elif response.status_code == 401:
                logger.error(f"❌ API 인증 실패: 401 - API 키를 확인하세요")
                return None
            else:
                logger.error(f"❌ API 검색 실패: {response.status_code}")
                try:
                    error_data = response.json()
                    logger.error(f"📄 에러 내용: {error_data}")
                except:
                    logger.error(f"📄 에러 내용: {response.text[:200]}")
                return None

        except Exception as e:
            logger.error(f"로고 검색 중 오류: {e}")
            return None

    async def get_logo_url(
        self,
        domain: str,
        size: int = 128,
        format: str = "png"
    ) -> Optional[Dict[str, Any]]:
        """
        회사 도메인으로 로고 정보 수집

        Args:
            domain: 회사 도메인 (예: recruit.co.jp)
            size: 로고 크기 (기본값: 128) - logo.dev에서는 사용되지 않음
            format: 이미지 포맷 (기본값: png) - logo.dev에서는 사용되지 않음

        Returns:
            로고 정보 딕셔너리 또는 None
        """
        # 도메인에서 프로토콜 제거
        if domain.startswith(('http://', 'https://')):
            domain = urlparse(domain).netloc

        # 도메인으로 검색
        return await self.search_logo_by_query(domain)
    
    async def get_logo_by_company_name(
        self,
        company_name: str,
        size: int = 128,
        format: str = "png"
    ) -> Optional[Dict[str, Any]]:
        """
        회사명으로 로고 정보 수집

        Args:
            company_name: 회사명 (예: "Recruit Holdings")
            size: 로고 크기 (사용되지 않음)
            format: 이미지 포맷 (사용되지 않음)

        Returns:
            로고 정보 딕셔너리 또는 None
        """
        # 회사명으로 직접 검색
        logo_info = await self.search_logo_by_query(company_name)
        if logo_info:
            logo_info["search_method"] = "company_name"
            logo_info["original_search"] = company_name
        return logo_info

    async def get_company_logo_info(self, company_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        회사 데이터에서 로고 정보 추출

        Args:
            company_data: 회사 정보 (name, name_en, 도메인 등)

        Returns:
            Dict: 로고 정보
        """
        company_name = company_data.get("name", "")
        company_name_en = company_data.get("name_en", "")
        domain = company_data.get("domain")

        # 1. 도메인이 있으면 도메인으로 우선 검색
        if domain:
            logo_info = await self.get_logo_url(domain)
            if logo_info:
                logo_info["search_method"] = "domain"
                return logo_info

        # 2. 영문명으로 검색
        if company_name_en:
            logo_info = await self.get_logo_by_company_name(company_name_en)
            if logo_info:
                return logo_info

        # 3. 일본어명으로 검색 (마지막 시도)
        if company_name:
            logo_info = await self.get_logo_by_company_name(company_name)
            if logo_info:
                return logo_info

        return None
    
    def get_attribution_html(self) -> str:
        """
        무료 사용시 필요한 attribution HTML 반환
        
        Returns:
            Attribution HTML 문자열
        """
        if self.api_key:
            return ""  # API 키가 있으면 attribution 불필요
        else:
            return '<a href="https://logo.dev" alt="Logo API">Logos provided by Logo.dev</a>'

class CompanyLogoUpdater:
    """기업 로고 정보 업데이트 관리자"""

    def __init__(self):
        self.target_companies = {
            "rakuten": {
                "name": "楽天グループ株式会社",
                "name_en": "Rakuten Group Inc",
                "domain": "rakuten.co.jp"
            },
            "mercari": {
                "name": "株式会社メルカリ",
                "name_en": "Mercari Inc",
                "domain": "mercari.com"
            },
            "cyberagent": {
                "name": "株式会社サイバーエージェント",
                "name_en": "CyberAgent Inc",
                "domain": "cyberagent.co.jp"
            },
            "lineyahoo": {
                "name": "ＬＩＮＥヤフー株式会社",
                "name_en": "LY Corporation",
                "domain": "lycorp.co.jp"
            },
            "recruit": {
                "name": "株式会社リクルートホールディングス",
                "name_en": "Recruit Holdings Co Ltd",
                "domain": "recruit-holdings.com"
            },
            "dena": {
                "name": "株式会社ディー・エヌ・エー",
                "name_en": "DeNA Co Ltd",
                "domain": "dena.com"
            },
            "sony": {
                "name": "ソニーグループ株式会社",
                "name_en": "Sony Group Corporation",
                "domain": "sony.com"
            },
            "softbank": {
                "name": "ソフトバンク株式会社",
                "name_en": "SoftBank Corp",
                "domain": "softbank.jp"
            },
            "fujitsu": {
                "name": "富士通株式会社",
                "name_en": "Fujitsu Limited",
                "domain": "fujitsu.com"
            },
            "nttdata": {
                "name": "株式会社ＮＴＴデータグループ",
                "name_en": "NTT DATA Group Corporation",
                "domain": "nttdata.com"
            },
            "zozo": {
                "name": "株式会社ZOZO",
                "name_en": "ZOZO Inc",
                "domain": "zozo.jp"
            }
        }

    async def update_all_company_logos(self) -> Dict[str, bool]:
        """모든 타겟 회사의 로고 정보 업데이트"""
        logger.info("🎨 기업 로고 정보 업데이트 시작...")

        results = {}

        async with LogoFetcher() as fetcher:
            for company_key, company_info in self.target_companies.items():
                logger.info(f"🔍 {company_key} 로고 검색 중...")

                try:
                    logo_info = await fetcher.get_company_logo_info(company_info)

                    if logo_info:
                        # 로고 정보 저장
                        await self._save_logo_info(company_key, logo_info)

                        # 데이터베이스 업데이트
                        db_success = await self._update_logo_in_database(company_key, logo_info)

                        results[company_key] = True
                        logger.info(f"✅ {company_key} 로고 정보 업데이트 완료")
                    else:
                        results[company_key] = False
                        logger.warning(f"❌ {company_key} 로고를 찾을 수 없음")

                except Exception as e:
                    results[company_key] = False
                    logger.error(f"❌ {company_key} 로고 업데이트 실패: {e}")

                # API 부하 방지
                await asyncio.sleep(1.0)

        # 결과 요약
        successful = sum(results.values())
        total = len(results)
        logger.info(f"🎊 로고 업데이트 완료: {successful}/{total}개 회사")

        return results

    async def _save_logo_info(self, company_key: str, logo_info: Dict[str, Any]):
        """로고 정보를 파일로 저장"""
        try:
            output_path = Path(f"data/logo_info/{company_key}_logo.json")
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(logo_info, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"💾 {company_key} 로고 정보 저장: {output_path}")

        except Exception as e:
            logger.error(f"로고 정보 저장 실패: {e}")

    async def _update_logo_in_database(self, company_key: str, logo_info: Dict[str, Any]) -> bool:
        """데이터베이스에 로고 정보 업데이트"""
        try:
            from .database import DatabaseManager

            async with DatabaseManager() as db:
                async with db.pool.acquire() as conn:
                    # 로고 정보 업데이트
                    query = """
                        UPDATE companies
                        SET
                            logo_url = $1,
                            logo_source = $2,
                            logo_fetched_at = $3,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE company_key = $4
                    """

                    # datetime 문자열을 datetime 객체로 변환
                    fetched_at_str = logo_info.get("fetched_at")
                    if fetched_at_str:
                        from datetime import datetime
                        fetched_at = datetime.fromisoformat(fetched_at_str.replace('Z', '+00:00'))
                    else:
                        fetched_at = datetime.now()

                    result = await conn.execute(
                        query,
                        logo_info.get("logo_url"),
                        logo_info.get("source"),
                        fetched_at,
                        company_key
                    )

                    if result == "UPDATE 1":
                        logger.info(f"💾 {company_key} 데이터베이스 로고 정보 업데이트 완료")
                        return True
                    else:
                        logger.warning(f"⚠️ {company_key} 데이터베이스 업데이트 실패 (회사 정보 없음?)")
                        return False

        except Exception as e:
            logger.error(f"데이터베이스 로고 업데이트 실패: {e}")
            return False


async def update_company_logos():
    """기업 로고 정보 업데이트 실행"""
    updater = CompanyLogoUpdater()
    results = await updater.update_all_company_logos()
    return results


# 편의 함수들 (기존 호환성)
async def get_company_logo(
    domain: str,
    api_key: Optional[str] = None,
    size: int = 128
) -> Optional[Dict[str, Any]]:
    """
    간단한 로고 추출 함수

    Args:
        domain: 회사 도메인
        api_key: Logo.dev API 키 (선택사항)
        size: 로고 크기

    Returns:
        로고 정보 딕셔너리 또는 None
    """
    async with LogoFetcher(api_key) as fetcher:
        return await fetcher.get_logo_url(domain, size)

async def get_logos_batch(
    domains: list[str],
    api_key: Optional[str] = None,
    size: int = 128
) -> dict[str, Optional[Dict[str, Any]]]:
    """
    여러 회사의 로고를 배치로 가져오기

    Args:
        domains: 도메인 리스트
        api_key: Logo.dev API 키
        size: 로고 크기

    Returns:
        {domain: logo_info} 딕셔너리
    """
    results = {}

    async with LogoFetcher(api_key) as fetcher:
        # 동시 실행 (너무 많으면 rate limit)
        tasks = []
        for domain in domains:
            task = fetcher.get_logo_url(domain, size)
            tasks.append(task)

        logo_infos = await asyncio.gather(*tasks, return_exceptions=True)

        for domain, logo_info in zip(domains, logo_infos):
            if isinstance(logo_info, Exception):
                results[domain] = None
                logger.error(f"로고 추출 실패 for {domain}: {logo_info}")
            else:
                results[domain] = logo_info

    return results

# 개별 회사 로고 검색 테스트 함수
async def test_logo_search(company_name: str, domain: str = None):
    """개별 회사 로고 검색 테스트"""
    logger.info(f"🧪 로고 검색 테스트: {company_name}")

    async with LogoFetcher() as fetcher:
        if domain:
            logo_info = await fetcher.get_logo_url(domain)
        else:
            logo_info = await fetcher.get_logo_by_company_name(company_name)

        if logo_info:
            logger.info(f"✅ 로고 발견!")
            logger.info(f"  - URL: {logo_info.get('logo_url')}")
            logger.info(f"  - 도메인: {logo_info.get('domain')}")
            logger.info(f"  - 크기: {logo_info.get('size')}")
            logger.info(f"  - 포맷: {logo_info.get('format')}")
            logger.info(f"  - 소스: {logo_info.get('source')}")
        else:
            logger.warning(f"❌ 로고를 찾을 수 없음")

        return logo_info


async def test_logo_fetcher():
    """Logo fetcher 테스트"""
    logger.info("=== Logo Fetcher 테스트 ===")

    # 일본 회사들 테스트
    test_companies = [
        ("Recruit Holdings", "recruit-holdings.com"),
        ("Sony Corporation", "sony.com"),
        ("SoftBank Corp", "softbank.jp"),
        ("Toyota Motor Corporation", "toyota.com")
    ]

    async with LogoFetcher() as fetcher:
        for company_name, domain in test_companies:
            logger.info(f"🔍 {company_name} ({domain}) 로고 검색...")
            logo_info = await fetcher.get_logo_url(domain)
            if logo_info:
                logger.info(f"  ✅ 발견: {logo_info.get('logo_url')}")
            else:
                logger.info(f"  ❌ 없음")

        # Attribution 정보
        logger.info(f"Attribution: {fetcher.get_attribution_html()}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "update":
            # python -m src.jap_syu.utils.logo_fetcher update
            asyncio.run(update_company_logos())
        elif sys.argv[1] == "test" and len(sys.argv) > 2:
            # python -m src.jap_syu.utils.logo_fetcher test "Sony Corporation" [도메인]
            company_name = sys.argv[2]
            domain = sys.argv[3] if len(sys.argv) > 3 else None
            asyncio.run(test_logo_search(company_name, domain))
        else:
            print("사용법:")
            print("  python -m src.jap_syu.utils.logo_fetcher update")
            print("  python -m src.jap_syu.utils.logo_fetcher test '회사명' [도메인]")
    else:
        # 기본 테스트
        asyncio.run(test_logo_fetcher())