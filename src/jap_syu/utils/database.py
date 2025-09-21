"""
데이터베이스 저장 유틸리티
RDS(PostgreSQL)에 기업 데이터를 저장하는 기능을 제공합니다.
"""

import os
import asyncio
import asyncpg
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger
from ..models import EdinetData, EdinetBasic, EdinetHR, EdinetFinancials

# .env 파일 로드 (python-dotenv가 설치되어 있으면)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class DatabaseManager:
    """데이터베이스 관리자"""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        데이터베이스 연결 설정
        
        Args:
            connection_string: PostgreSQL 연결 문자열
                              예: postgresql://user:password@host:port/database
        """
        self.connection_string = connection_string or os.getenv("DATABASE_URL")
        if not self.connection_string:
            raise ValueError(
                "DATABASE_URL 환경 변수가 설정되지 않았습니다. "
                ".env 파일에 DATABASE_URL을 설정하거나 환경 변수로 전달해주세요."
            )
        self.pool = None
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        await self.close()
    
    async def connect(self):
        """데이터베이스 연결 풀 생성"""
        try:
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            logger.info("데이터베이스 연결 풀 생성 완료")
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            raise
    
    async def close(self):
        """데이터베이스 연결 풀 종료"""
        if self.pool:
            await self.pool.close()
            logger.info("데이터베이스 연결 풀 종료")
    
    async def create_tables(self):
        """테이블 생성"""
        async with self.pool.acquire() as conn:
            try:
                # 회사 기본 정보 테이블
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS companies (
                        id SERIAL PRIMARY KEY,
                        company_key VARCHAR(50) UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        name_en VARCHAR(255),
                        name_ko VARCHAR(255),
                        headquarters TEXT,
                        headquarters_en TEXT,
                        headquarters_ko TEXT,
                        founded_year INTEGER,
                        industry VARCHAR(100),
                        market_cap BIGINT,
                        sec_code VARCHAR(10),
                        employee_count INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 인사 정보 테이블
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS company_hr (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                        avg_tenure_years DECIMAL(5,2),
                        avg_age_years DECIMAL(5,2),
                        avg_annual_salary_jpy INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 재무 정보 테이블
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS company_financials (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                        revenue BIGINT,
                        operating_income BIGINT,
                        net_income BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 출처 정보 테이블
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS company_provenance (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                        document_id VARCHAR(100),
                        submission_date DATE,
                        extraction_method VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                logger.info("테이블 생성 완료")
                
            except Exception as e:
                logger.error(f"테이블 생성 실패: {e}")
                raise
    
    async def save_company_data(self, company_key: str, edinet_data: EdinetData) -> bool:
        """
        회사 데이터를 데이터베이스에 저장
        
        Args:
            company_key: 회사 식별자
            edinet_data: EDINET 데이터 객체
            
        Returns:
            bool: 저장 성공 여부
        """
        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    # 1. 회사 기본 정보 저장/업데이트
                    company_id = await self._upsert_company(conn, company_key, edinet_data.basic)
                    
                    # 2. 인사 정보 저장/업데이트
                    await self._upsert_hr(conn, company_id, edinet_data.hr)
                    
                    # 3. 재무 정보 저장/업데이트
                    await self._upsert_financials(conn, company_id, edinet_data.financials)
                    
                    # 4. 출처 정보 저장
                    await self._insert_provenance(conn, company_id, edinet_data.provenance)
                    
                    logger.info(f"✅ {company_key} 데이터베이스 저장 완료")
                    return True
                    
            except Exception as e:
                logger.error(f"❌ {company_key} 데이터베이스 저장 실패: {e}")
                return False
    
    async def _upsert_company(self, conn, company_key: str, basic: EdinetBasic) -> int:
        """회사 기본 정보 저장/업데이트"""
        query = """
            INSERT INTO companies (
                company_key, name, name_en, name_ko, headquarters, 
                headquarters_en, headquarters_ko, founded_year, industry,
                market_cap, sec_code, employee_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (company_key) 
            DO UPDATE SET
                name = EXCLUDED.name,
                name_en = EXCLUDED.name_en,
                name_ko = EXCLUDED.name_ko,
                headquarters = EXCLUDED.headquarters,
                headquarters_en = EXCLUDED.headquarters_en,
                headquarters_ko = EXCLUDED.headquarters_ko,
                founded_year = EXCLUDED.founded_year,
                industry = EXCLUDED.industry,
                market_cap = EXCLUDED.market_cap,
                sec_code = EXCLUDED.sec_code,
                employee_count = EXCLUDED.employee_count,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """
        
        result = await conn.fetchrow(
            query,
            company_key,
            basic.name,
            basic.name_en,
            basic.name_ko,
            basic.headquarters,
            basic.headquarters_en,
            basic.headquarters_ko,
            basic.founded_year,
            basic.industry,
            basic.market_cap,
            basic.sec_code,
            basic.employee_count
        )
        
        return result['id']
    
    async def _upsert_hr(self, conn, company_id: int, hr: EdinetHR):
        """인사 정보 저장/업데이트"""
        # 기존 데이터 삭제 후 새로 삽입
        await conn.execute("DELETE FROM company_hr WHERE company_id = $1", company_id)
        
        query = """
            INSERT INTO company_hr (
                company_id, avg_tenure_years, avg_age_years, avg_annual_salary_jpy
            ) VALUES ($1, $2, $3, $4)
        """
        
        await conn.execute(
            query,
            company_id,
            hr.avgTenureYears,
            hr.avgAgeYears,
            hr.avgAnnualSalaryJPY
        )
    
    async def _upsert_financials(self, conn, company_id: int, financials: EdinetFinancials):
        """재무 정보 저장/업데이트"""
        # 기존 데이터 삭제 후 새로 삽입
        await conn.execute("DELETE FROM company_financials WHERE company_id = $1", company_id)
        
        query = """
            INSERT INTO company_financials (
                company_id, revenue, operating_income, net_income
            ) VALUES ($1, $2, $3, $4)
        """
        
        await conn.execute(
            query,
            company_id,
            financials.revenueJPY,  # revenueJPY 필드 사용
            None,  # operating_income은 현재 없음
            None   # net_income은 현재 없음
        )
    
    async def _insert_provenance(self, conn, company_id: int, provenance: Dict[str, Any]):
        """출처 정보 저장"""
        query = """
            INSERT INTO company_provenance (
                company_id, document_id, submission_date, extraction_method
            ) VALUES ($1, $2, $3, $4)
        """
        
        await conn.execute(
            query,
            company_id,
            provenance.get('document_id'),
            provenance.get('submission_date'),
            provenance.get('extraction_method')
        )
    
    async def get_company_data(self, company_key: str) -> Optional[Dict[str, Any]]:
        """회사 데이터 조회"""
        async with self.pool.acquire() as conn:
            query = """
                SELECT 
                    c.*,
                    h.avg_tenure_years,
                    h.avg_age_years,
                    h.avg_annual_salary_jpy,
                    f.revenue,
                    f.operating_income,
                    f.net_income
                FROM companies c
                LEFT JOIN company_hr h ON c.id = h.company_id
                LEFT JOIN company_financials f ON c.id = f.company_id
                WHERE c.company_key = $1
            """
            
            result = await conn.fetchrow(query, company_key)
            return dict(result) if result else None
    
    async def list_companies(self) -> list[Dict[str, Any]]:
        """모든 회사 목록 조회"""
        async with self.pool.acquire() as conn:
            query = """
                SELECT 
                    company_key, name, name_ko, founded_year, 
                    market_cap, employee_count, updated_at
                FROM companies
                ORDER BY updated_at DESC
            """
            
            results = await conn.fetch(query)
            return [dict(row) for row in results]


async def save_to_database(company_key: str, edinet_data: EdinetData) -> bool:
    """
    EDINET 데이터를 데이터베이스에 저장하는 편의 함수
    
    Args:
        company_key: 회사 식별자
        edinet_data: EDINET 데이터 객체
        
    Returns:
        bool: 저장 성공 여부
    """
    async with DatabaseManager() as db:
        return await db.save_company_data(company_key, edinet_data)


async def test_database_connection():
    """데이터베이스 연결 테스트"""
    try:
        async with DatabaseManager() as db:
            await db.create_tables()
            logger.info("✅ 데이터베이스 연결 및 테이블 생성 성공")
            return True
    except Exception as e:
        logger.error(f"❌ 데이터베이스 연결 실패: {e}")
        return False


if __name__ == "__main__":
    # 테스트 실행
    asyncio.run(test_database_connection())
