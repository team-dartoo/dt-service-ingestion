import os
import re
import unicodedata
import logging
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from typing import Tuple

# 상수 정의
PDF_SIG = b'%PDF-'                                                  # PDF 파일의 시작값
ZIP_SIG = b'PK\x03\x04'                                             # ZIP 파일의 시작값
READ_HEAD_N = 4096                                                  # 파일 유형을 감지하기 위해 파일의 앞부분에서 읽는 바이트 수
PICK_ORDER = ('.pdf', '.html', '.htm', '.xml')

_CHARSET_RE = re.compile(rb'(?i)charset\s*=\s*([a-zA-Z0-9_-]+)')    # 'charset' 탐색 정규식

_CODEC_MAP = {
    "euc-kr": "cp949", "ks_c_5601-1987": "cp949", "ksc5601": "cp949"
}

# 유틸리티 함수
def _normalize_name(name: str) -> str:
    '''
    파일명 정리
    - 경로 제거 & 파일명 사용
    - 유니코드 정규화(NFC)
    '''
    base = os.path.basename(name).strip()
    return unicodedata.normalize('NFC', base)

def _get_base_name(filename: str) -> str:                           # 파일명에서 확장자를 제거한 순수 이름 반환
    return os.path.splitext(_normalize_name(filename))[0]

def _detect_charset(head_bytes: bytes) -> str | None:               # 문서 앞부분에서 문자 인코딩 감지
    match = _CHARSET_RE.search(head_bytes)
    if match:
        charset = match.group(1).decode('ascii', errors='ignore').lower()   # 정규식으로 찾은 인코딩을 ascii코드로 디코딩
        return _CODEC_MAP.get(charset, charset)                             # _CODEC_MAP을 참조하여 표준 코덱 이름으로 변환
    return None

def sniff_type(blob: bytes) -> str:
    head = blob[:READ_HEAD_N]
    if head.startswith(PDF_SIG): return 'pdf'
    if head.startswith(ZIP_SIG): return 'zip'
    
    text_head = head.lstrip()                                       # 텍스트 기반 파일 감지
    if text_head.startswith(b'\xef\xbb\xbf'):                       # UTF-8 BOM
        text_head = text_head[3:]
    
    if re.match(b'\\s*<!doctype html|<html', text_head, re.IGNORECASE): return 'html'   # html 판단
    if re.match(b'\\s*<\\?xml', text_head, re.IGNORECASE): return 'xml'                 # xml 판단
    
    return 'bin'                                                    # 위 조건 미충족시, binary file로 판단

def _transcode_to_utf8(raw_bytes: bytes) -> bytes:                  # 원본 바이트를 UTF-8로 트랜스코딩
    detected = _detect_charset(raw_bytes[:READ_HEAD_N])             # 1. 문서에 명시된 인코딩 감지
    encodings_to_try = ['euc-kr' ,'utf-8', detected, 'cp949']       # 2. 디코딩 순서 정의
    
    decoded_text = None
    for enc in filter(None, encodings_to_try):                      # 3. 유효한 인코딩만 시도
        try:
            decoded_text = raw_bytes.decode(enc, errors='strict')   # 4. Strict 모드로 디코딩 시도
            break
        except (UnicodeDecodeError, LookupError):                   # 5. 해당 인코딩으로 디코딩 실패 시 다음 후보로 전환
            continue
    
    if decoded_text is None:                                        # 6. 모든 시도 실패할 경우, 대체문자를 사용하여 강제 변환
        decoded_text = raw_bytes.decode('utf-8', errors='replace') 

    cleaned_text = re.sub(                                          # 7. UTF-8 선언으로 교체
        r'(?i)<meta\s+http-equiv\s*=\s*["\']Content-Type["\'][^>]*>',
        '<meta charset="utf-8">',
        decoded_text
    )
    
    if cleaned_text == decoded_text:                                # 8. 위 패턴이 없을 시, 다른 형태의 charset 선언을 찾아 교체
        cleaned_text = re.sub(
            r'(?i)<meta\s+charset\s*=\s*["\'][^"\']+["\']\s*\/?>',
            '<meta charset="utf-8">',
            cleaned_text
        )

    return cleaned_text.encode('utf-8')

def _pick_and_normalize_from_zip(zip_bytes: bytes, base_name: str) -> Tuple[str, bytes, str]:
    '''
    ZIP 파일 압축 해제하고, 내부에 있는 파일을 추출하여 정규화
    '''
    try:
        with ZipFile(BytesIO(zip_bytes)) as zf:
            infos = [info for info in zf.infolist() if not info.is_dir()]
            
            for ext in PICK_ORDER:                                                              # 우선순위에 따라 파일 탐색
                candidates = [info for info in infos if info.filename.lower().endswith(ext)]
                if candidates:
                    best_file = max(candidates, key=lambda f: f.file_size)                      # 후보가 여러 개일 경우, 가장 용량이 큰 파일 전택
                    return normalize_payload(best_file.filename, zf.read(best_file.filename))   # [재귀호출] 추출한 파일도 재정규화
            
            if infos:
                largest = max(infos, key=lambda f: f.file_size)
                return 'application/octet-stream', zf.read(largest.filename), _normalize_name(largest.filename)
            raise ValueError("No processable files found in ZIP.")
            
    except (BadZipFile, ValueError) as e:                                                       # ZIP 파일 손상되었거나 처리 중 오류 발생 시, 바이너리로 변환
        logging.warning(f"Failed to process ZIP for {base_name}: {e}")
        return 'application/octet-stream', zip_bytes, f"{base_name}.zip"

def normalize_payload(object_key: str, body: bytes) -> Tuple[str, bytes, str]:
    '''
    파일 유형을 판별하고, 각 유형에 맞는 처리 함수로 작업 분기
    
    Args:
        object_key (str): 문서의 고유 식별자 (예: 접수번호 '20250924000123')
        body (bytes): 문서의 원본 데이터 바이트
    '''
    file_type = sniff_type(body)                # 1. 파일 유형 확인
    base_name = _get_base_name(object_key)      # 2. 파일명 정규화

    if file_type == 'pdf':                      # 3. 파일 유형에 따라 적절한 처리 로직 실행
        return 'application/pdf', body, f"{base_name}.pdf"
    if file_type == 'zip':
        return _pick_and_normalize_from_zip(body, base_name)
    if file_type == 'html':
        return 'text/html; charset=UTF-8', _transcode_to_utf8(body), f"{base_name}.html"
    if file_type == 'xml':
        return 'application/xml; charset=UTF-8', _transcode_to_utf8(body), f"{base_name}.xml"
    
    return 'application/octet-stream', body, f"{base_name}.bin"