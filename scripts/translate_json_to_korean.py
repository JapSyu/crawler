"""
일본어 JSON을 한국어로 번역
DeepL API를 사용해서 JSON의 일본어 텍스트를 한국어로 번역하고, 영어는 그대로 유지

Usage:
    python scripts/translate_json_to_korean.py <input_json> [output_json] [--save]
    예: python scripts/translate_json_to_korean.py data/recruit_holdings.json data/recruit_holdings_ko.json --save
"""
import asyncio
import sys
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Union, Optional
import deepl
from loguru import logger
from dotenv import load_dotenv


# .env 파일 로드
load_dotenv()

# 번역 제외 필드 (메타데이터, URL 등)
SKIP_FIELDS = {
    "company_name",
    "url",
    "source",
    "collected_at",
    "interview_links"
}


def has_japanese(text: str) -> bool:
    """
    문자열에 일본어가 포함되어 있는지 확인

    Args:
        text: 확인할 문자열

    Returns:
        bool: 일본어 포함 여부
    """
    # 히라가나, 가타카나, 한자(CJK) 범위 체크 (구두점 포함)
    japanese_pattern = re.compile(r'[\u3000-\u303F\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]')
    return bool(japanese_pattern.search(text))


def has_english(text: str) -> bool:
    """
    문자열에 영어가 포함되어 있는지 확인

    Args:
        text: 확인할 문자열

    Returns:
        bool: 영어 포함 여부
    """
    # 알파벳 패턴 체크
    english_pattern = re.compile(r'[a-zA-Z]')
    return bool(english_pattern.search(text))


def translate_text(translator: deepl.Translator, text: str) -> str:
    """
    텍스트를 일본어에서 한국어로 번역 (영어 구문은 유지)

    영어 구문(연속된 영어 단어들)을 <notranslate> 태그로 감싸 DeepL에서 번역 제외

    Args:
        translator: DeepL Translator 인스턴스
        text: 번역할 텍스트

    Returns:
        str: 번역된 텍스트 (영어 구문은 원본 유지)
    """
    if not text or not isinstance(text, str):
        return text

    # 일본어가 없으면 원본 반환
    if not has_japanese(text):
        return text

    try:
        # 영어 구문 찾기 (연속된 영어 단어들, 공백/하이픈 포함)
        # 예: "Wow the World", "Indeed", "UI/UX" 등
        english_phrases_pattern = re.compile(r'[a-zA-Z]+(?:[\s\-/&]+[a-zA-Z]+)*')

        # 영어 구문을 <notranslate> 태그로 감싸 번역에서 제외
        tagged_text = text
        for match in english_phrases_pattern.finditer(text):
            phrase = match.group()
            # 최소 2글자 이상의 영어만 보호 (단일 문자는 제외)
            if len(phrase.replace(' ', '').replace('-', '').replace('/', '')) >= 2:
                tagged_text = tagged_text.replace(phrase, f"<notranslate>{phrase}</notranslate>", 1)

        # 번역 실행 (DeepL에 notranslate 태그 무시 지시)
        result = translator.translate_text(
            tagged_text,
            source_lang="JA",
            target_lang="KO",
            tag_handling="xml",
            tag_handling_version="v2",
            ignore_tags=["notranslate"],
            preserve_formatting=True,
            model_type="prefer_quality_optimized",
        )

        translated_text = result.text

        logger.debug(f"번역: {text[:50]}... → {translated_text[:50]}...")
        return translated_text

    except Exception as e:
        logger.error(f"번역 실패: {text[:50]}... - {e}")
        return text


def translate_json_recursive(
    data: Union[Any],
    translator: deepl.Translator,
    current_path: str = ""
) -> Union[Any]:
    """
    JSON 데이터를 재귀적으로 순회하면서 일본어를 한국어로 번역

    Args:
        data: 번역할 JSON 데이터
        translator: DeepL Translator 인스턴스
        current_path: 현재 필드 경로 (디버깅용)

    Returns:
        번역된 JSON 데이터
    """
    # 딕셔너리 처리
    if isinstance(data, dict):
        translated = {}
        for key, value in data.items():
            # 현재 경로 업데이트
            new_path = f"{current_path}.{key}" if current_path else key

            # 번역 제외 필드 체크
            if key in SKIP_FIELDS:
                translated[key] = value
                logger.debug(f"스킵: {new_path}")
            else:
                # KEY도 번역 (일본어가 있는 경우)
                translated_key = translate_text(translator, key) if isinstance(key, str) else key
                translated[translated_key] = translate_json_recursive(value, translator, new_path)
        return translated

    # 리스트 처리
    elif isinstance(data, list):
        return [
            translate_json_recursive(item, translator, f"{current_path}[{i}]")
            for i, item in enumerate(data)
        ]

    # 문자열 처리 (번역 대상)
    elif isinstance(data, str):
        return translate_text(translator, data)

    # 기타 타입 (숫자, bool 등)
    else:
        return data


def main():
    """메인 처리"""
    if len(sys.argv) < 2:
        logger.error("Usage: python scripts/translate_json_to_korean.py <input_json> [output_json] [--save]")
        sys.exit(1)

    # 인자 파싱 (간단): --로 시작하는 옵션은 제외하고 포지셔널만 추출
    args = sys.argv[1:]
    auto_save = "--save" in args
    positionals = [a for a in args if not a.startswith("--")]

    if len(positionals) < 1:
        logger.error("Usage: python scripts/translate_json_to_korean.py <input_json> [output_json] [--save]")
        sys.exit(1)

    # 입력/출력 파일 경로
    input_file = Path(positionals[0])
    if len(positionals) >= 2:
        output_file = Path(positionals[1])
    else:
        # 기본 출력: 입력 파일명에 _ko 추가
        output_file = input_file.parent / f"{input_file.stem}_ko{input_file.suffix}"

    if not input_file.exists():
        logger.error(f"❌ 파일을 찾을 수 없습니다: {input_file}")
        sys.exit(1)

    # DeepL API 키 확인
    api_key = os.getenv("DEEPL_API_KEY")
    if not api_key:
        logger.error("❌ DEEPL_API_KEY가 .env 파일에 설정되어있지 않습니다.")
        sys.exit(1)

    logger.info("========================================")
    logger.info("JSON 일본어 → 한국어 번역 시작")
    logger.info("========================================")
    logger.info(f"입력: {input_file}")
    logger.info(f"출력: {output_file}")

    # JSON 읽기
    logger.info("\n📄 JSON 파일 읽기 중...")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # DeepL Translator 초기화
    logger.info("🔄 DeepL API 초기화 중...")
    translator = deepl.Translator(api_key)

    # 번역 실행
    logger.info("\n🌐 번역 실행 중... (시간이 걸릴 수 있습니다)")
    translated_data = translate_json_recursive(data, translator)

    # 번역된 JSON 저장
    logger.info(f"\n💾 번역된 JSON 저장 중: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(translated_data, f, ensure_ascii=False, indent=2)

    logger.info("\n========================================")
    logger.info("✅ 번역 완료")
    logger.info("========================================")
    logger.info(f"📁 번역된 파일: {output_file}")

    # 옵션: 자동으로 MongoDB 저장 스크립트 실행
    if auto_save:
        cmd = [sys.executable, str(Path(__file__).parent / "save_company_to_mongodb.py"), str(output_file)]
        logger.info("\n🪄 MongoDB 저장 스크립트 실행 중...")
        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
            )
            logger.info("✅ MongoDB 저장 스크립트 성공")
        except subprocess.CalledProcessError as e:
            logger.error("❌ MongoDB 저장 스크립트 실패")
            if e.stdout:
                logger.error(e.stdout)
            if e.stderr:
                logger.error(e.stderr)
        except Exception as e:
            logger.error(f"❌ MongoDB 저장 스크립트 실행 오류: {e}")
    else:
        logger.info("\n다음 명령으로 MongoDB에 저장:")
        logger.info(f"  {sys.executable} scripts/save_company_to_mongodb.py {output_file}")


if __name__ == "__main__":
    main()
