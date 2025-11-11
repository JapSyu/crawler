"""
기업 채용 사이트 분석 결과를 MongoDB에 저장하는 일반적인 스크립트
Playwright MCP로 수집한 JSON 데이터를 그대로 MongoDB에 저장

Usage:
    python save_company_to_mongodb.py <json_file_path>
"""
import asyncio
import sys
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()


async def save_to_mongodb(data: dict) -> bool:
    """
    JSON 데이터를 MongoDB에 저장

    Args:
        data: Playwright MCP로 수집한 기업 데이터 (company_key 필수)

    Returns:
        bool: 저장 성공 시 True
    """
    # company_key 필수 확인
    if "company_key" not in data:
        logger.error("❌ company_key 필드가 필요합니다")
        return False

    company_key = data["company_key"]

    # MongoDB 연결
    # .env 파일에서 MongoDB 연결 정보 가져오기 (필수)
    mongodb_url = os.getenv("MONGODB_URL")
    db_name = os.getenv("MONGODB_DB_NAME")
    collection_name = os.getenv("MONGODB_COLLECTION_NAME")
    
    # 환경변수가 없으면 에러 발생
    if not mongodb_url:
        logger.error("❌ MONGODB_URL 환경변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return False
    if not db_name:
        logger.error("❌ MONGODB_DB_NAME 환경변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return False
    if not collection_name:
        logger.error("❌ MONGODB_COLLECTION_NAME 환경변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return False
    
    client = AsyncIOMotorClient(mongodb_url)
    db = client[db_name]
    collection = db[collection_name]

    # 메타데이터 추가
    # collected_at: 최초 수집 시간 (한 번만 설정)
    if "collected_at" not in data:
        data["collected_at"] = datetime.now(timezone.utc).isoformat()
    
    # updated_at: 최종 업데이트 시간 (항상 갱신)
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    if "source" not in data:
        data["source"] = "Playwright MCP"

    try:
        # replace_one: 기존 데이터가 있으면 교체, 없으면 삽입
        # upsert=True: 문서가 없으면 삽입, 있으면 교체
        result = await collection.replace_one(
            {"company_key": company_key},
            data,
            upsert=True
        )
        
        if result.upserted_id:
            logger.info(f"✅ {company_key} 데이터를 새로 삽입했습니다")
        elif result.modified_count > 0:
            logger.info(f"✅ {company_key} 데이터를 업데이트했습니다")

        if result.upserted_id or result.modified_count > 0:
            logger.info("========================================")
            logger.info(f"✅ {company_key} 데이터 저장 완료")
            logger.info("========================================")
            logger.info(f"   - company_key: {company_key}")
            logger.info(f"   - url: {data.get('url', 'N/A')}")
            logger.info(f"   - source: {data.get('source')}")
            logger.info(f"   - collected_at: {data.get('collected_at')}")
            logger.info(f"   - updated_at: {data.get('updated_at')}")

            # 통계 정보
            if "job_postings" in data and "positions" in data["job_postings"]:
                logger.info(f"   - 모집 코스 수: {len(data['job_postings']['positions'])}")
            if "interview_links" in data:
                logger.info(f"   - 사원 인터뷰 수: {len(data['interview_links'])}")
            if "selection_flow" in data:
                flow_count = len(data["selection_flow"]) if isinstance(data["selection_flow"], dict) else 0
                logger.info(f"   - 전형 흐름: {flow_count}종류")

            return True
        else:
            # upsert=True이므로 이 경우는 거의 발생하지 않음
            logger.warning(f"{company_key} 데이터가 변경되지 않았습니다 (내용 동일)")
            return True

    except Exception as e:
        logger.error(f"❌ 에러 발생: {e}")
        return False
    finally:
        client.close()


async def main():
    """메인 처리"""

    # 커맨드라인 인수 확인    
    if len(sys.argv) < 2:
        logger.error("Usage: python save_company_to_mongodb.py <json_file_path>")
        sys.exit(1)

    # JSON 파일 읽기
    json_file = Path(sys.argv[1])
    if not json_file.exists():
        logger.error(f"❌ 파일을 찾을 수 없습니다: {json_file}")
        sys.exit(1)

    logger.info(f"📄 {json_file} 을 읽습니다...")
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON 파싱 에러: {e}")
        sys.exit(1)

    # MongoDB 저장
    success = await save_to_mongodb(data)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())