"""
Content Normalizer v2.0 - 라이브러리 기반 개선 버전

DART API에서 다운로드한 공시 원문(ZIP)을 정규화하는 모듈.

[개선 사항 v2.0]
1. chardet: 인코딩 자동 감지 (선언 없는 문서 처리)
2. BeautifulSoup: HTML charset 추출 정확도 향상  
3. lxml: XML 파싱 안정성 강화
4. 상세 에러 로깅 및 graceful degradation

[의존성]
- chardet>=5.0.0
- beautifulsoup4>=4.12.0
- lxml>=5.0.0 (선택적)
"""

import os
import re
import unicodedata
import logging
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from typing import Tuple, Optional, List, Dict, Any

# ==================== 외부 라이브러리 (graceful degradation) ====================

try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False
    logging.warning("chardet not installed. Encoding detection may be less accurate.")

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    logging.warning("beautifulsoup4 not installed. HTML parsing may be less accurate.")

try:
    import lxml.etree as LxmlET
    HAS_LXML = True
except ImportError:
    HAS_LXML = False
    logging.warning("lxml not installed. Using standard xml parser.")


# ==================== 상수 정의 ====================

READ_HEAD_N = 65536                     # 파일 종류 판별용 읽기 바이트 수
ZIP_SIG = b'PK\x03\x04'                 # ZIP 파일 시그니처

KIND_PRIORITY = ['html', 'xml', 'bin']  # ZIP 내 콘텐츠 우선순위
MAX_FILES = 200                         # ZIP 내 최대 파일 수 (보안)
MAX_TOTAL_UNCOMPRESSED = 200 * 1024 * 1024  # 최대 압축 해제 용량 (200MB)

# 인코딩 별칭 (한국어 문서 특화)
_ENCODING_ALIASES = {
    'ks_c_5601-1987': 'cp949',
    'ks_c_5601': 'cp949',
    'x-windows-949': 'cp949',
    'windows-949': 'cp949',
    'euckr': 'euc-kr',
    'euc_kr': 'euc-kr',
    'utf8': 'utf-8',
    'utf-8-sig': 'utf-8-sig',
}

# 한국어 문서에서 우선 시도할 인코딩 목록
_KOREAN_ENCODING_PRIORITY = [
    'utf-8-sig',
    'utf-8',
    'cp949',      # Windows 한글
    'euc-kr',     # Unix 한글
    'iso-8859-1', # Latin-1 fallback
]

# ==================== 정규식 정의 (fallback용) ====================

_HTML_TAG_RE = re.compile(rb'(?is)<html[^>]*>')
_HTML_DOCTYPE_RE = re.compile(rb'(?is)<!doctype\s+html')
_HEAD_RE = re.compile(rb'(?is)<head[^>]*>')
_META_ANY_RE = re.compile(
    rb'(?is)<meta[^>]+(?:charset\s*=|http-equiv\s*=\s*["\']content-type["\'][^>]*content\s*=\s*["\']text/html;\s*charset=)'
)
_DOCTYPE_RE = re.compile(rb'(?is)\A\s*<!doctype[^>]*>\s*')
_XML_DECL_RE = re.compile(rb'(?is)<\?xml[^>]+encoding\s*=\s*["\']?([a-zA-Z0-9._-]+)')
_XML_DECL_REPL = re.compile(rb'^<\?xml[^>]*\?>')
_HTML_META_TAG_RE = re.compile(rb'(?is)<meta[^>]+charset\s*=\s*["\']?([a-zA-Z0-9._-]+)')
_HTML_HTTP_EQUIV_RE = re.compile(
    rb'(?is)<meta[^>]+http-equiv\s*=\s*["\']content-type["\'][^>]*content\s*=\s*["\']text/html;\s*charset=([a-zA-Z0-9._-]+)[^"\']*["\']'
)
_SAFE_NAME_RE = re.compile(r'[^A-Za-z0-9._-]+')


# ==================== 유틸리티 함수 ====================

def _normalize_encoding_name(enc: Optional[str]) -> Optional[str]:
    """인코딩 이름을 표준 형식으로 정규화"""
    if not enc:
        return None
    enc_lower = enc.strip().lower().replace('_', '-')
    return _ENCODING_ALIASES.get(enc_lower, enc_lower)


def _normalize_filename(name: str) -> str:
    """파일명 유니코드 정규화 및 경로 제거"""
    return unicodedata.normalize('NFC', os.path.basename(name).strip())


def _safe_ascii_name(name: str) -> str:
    """안전한 ASCII 파일명 생성"""
    n = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    n = _SAFE_NAME_RE.sub('_', n).strip('._-')
    return n or 'document'


def _get_base_name(filename: str) -> str:
    """확장자 제거한 파일명 반환"""
    return os.path.splitext(_normalize_filename(filename))[0]


# ==================== 콘텐츠 감지 ====================

def sniff_kind(data: bytes) -> str:
    """
    바이트 데이터의 콘텐츠 종류를 감지.
    
    Returns:
        'html', 'xml', 또는 'bin'
    """
    head = data[:READ_HEAD_N]
    
    # BOM 제거 후 분석
    if head.startswith(b'\xef\xbb\xbf'):  # UTF-8 BOM
        head = head[3:]
    elif head.startswith(b'\xff\xfe') or head.startswith(b'\xfe\xff'):  # UTF-16 BOM
        head = head[2:]
    
    # 앞 공백/주석 무시하고 실제 콘텐츠 시작점 찾기
    stripped = head.lstrip()
    
    # HTML 감지 (여러 패턴)
    if (_HTML_TAG_RE.search(head) or 
        _HTML_DOCTYPE_RE.search(head) or 
        _META_ANY_RE.search(head) or 
        _HEAD_RE.search(head)):
        return 'html'
    
    # XML 선언 확인
    if stripped.startswith(b'<?xml'):
        return 'xml'
    
    # 선언 없는 XML 감지 (lxml 사용)
    if HAS_LXML and stripped.startswith(b'<') and not stripped.startswith(b'<!'):
        try:
            LxmlET.fromstring(data[:READ_HEAD_N * 2])
            return 'xml'
        except:
            pass
    
    # 기타 XML 패턴 (휴리스틱)
    if stripped.startswith(b'<') and b'</' in head:
        # 닫는 태그가 있고, HTML이 아니면 XML로 추정
        if not any(tag in head.lower() for tag in [b'<html', b'<body', b'<head', b'<div', b'<span']):
            return 'xml'
    
    return 'bin'


# ==================== 인코딩 감지 ====================

def _detect_encoding_from_declaration(data: bytes, kind: str) -> Optional[str]:
    """콘텐츠 선언부에서 인코딩 감지"""
    
    # BeautifulSoup으로 HTML charset 감지
    if HAS_BS4 and kind == 'html':
        try:
            soup = BeautifulSoup(data[:READ_HEAD_N], 'html.parser')
            
            # <meta charset="...">
            meta = soup.find('meta', charset=True)
            if meta:
                return _normalize_encoding_name(meta.get('charset'))
            
            # <meta http-equiv="Content-Type" content="...; charset=...">
            meta = soup.find('meta', attrs={
                'http-equiv': lambda x: x and x.lower() == 'content-type'
            })
            if meta:
                content = meta.get('content', '')
                match = re.search(r'charset=([^\s;]+)', content, re.I)
                if match:
                    return _normalize_encoding_name(match.group(1))
        except Exception as e:
            logging.debug(f"BS4 encoding detection failed: {e}")
    
    # 정규식 fallback
    head = data[:READ_HEAD_N]
    
    # XML 선언
    m = _XML_DECL_RE.search(head)
    if m:
        return _normalize_encoding_name(m.group(1).decode('ascii', 'ignore'))
    
    # HTML meta charset
    m = _HTML_META_TAG_RE.search(head)
    if m:
        return _normalize_encoding_name(m.group(1).decode('ascii', 'ignore'))
    
    # HTML http-equiv
    m = _HTML_HTTP_EQUIV_RE.search(head)
    if m:
        return _normalize_encoding_name(m.group(1).decode('ascii', 'ignore'))
    
    return None


def _detect_encoding_auto(data: bytes) -> Tuple[Optional[str], float]:
    """chardet으로 인코딩 자동 감지"""
    if not HAS_CHARDET:
        return (None, 0.0)
    
    try:
        # 더 많은 데이터로 감지 (정확도 향상)
        result = chardet.detect(data[:READ_HEAD_N * 2])
        enc = result.get('encoding')
        conf = result.get('confidence', 0.0) or 0.0
        return (_normalize_encoding_name(enc), conf)
    except Exception as e:
        logging.debug(f"chardet detection failed: {e}")
        return (None, 0.0)


# ==================== 인코딩 변환 ====================

def _build_encoding_candidates(declared: Optional[str], auto: Optional[str], auto_conf: float) -> List[str]:
    """시도할 인코딩 목록 구성"""
    candidates = []
    
    # 1. 선언된 인코딩 우선
    if declared:
        candidates.append(declared)
    
    # 2. 자동 감지 인코딩 (신뢰도 높으면)
    if auto and auto_conf > 0.7 and auto not in candidates:
        candidates.append(auto)
    
    # 3. 한국어 인코딩 우선순위
    for enc in _KOREAN_ENCODING_PRIORITY:
        if enc not in candidates:
            candidates.append(enc)
    
    # 4. 자동 감지 인코딩 (신뢰도 낮아도)
    if auto and auto not in candidates:
        candidates.append(auto)
    
    return candidates


def _to_utf8_with_rewrite(data: bytes, kind: str) -> Tuple[bytes, str]:
    """
    바이트 데이터를 UTF-8로 변환하고 선언부 재작성.
    
    Returns:
        (UTF-8 바이트, 사용된 인코딩)
    """
    # 인코딩 감지
    declared_enc = _detect_encoding_from_declaration(data, kind)
    auto_enc, auto_conf = _detect_encoding_auto(data)
    
    candidates = _build_encoding_candidates(declared_enc, auto_enc, auto_conf)
    
    # 순차적으로 시도
    for enc in candidates:
        try:
            # 엄격한 디코딩/인코딩
            txt = data.decode(enc, errors='strict')
            utf8_bytes = txt.encode('utf-8', errors='strict')
            
            # 선언부 재작성
            utf8_bytes = _rewrite_encoding_declaration(utf8_bytes, kind)
            
            # 검증
            utf8_bytes.decode('utf-8', errors='strict')
            
            return (utf8_bytes, enc)
            
        except (UnicodeDecodeError, UnicodeEncodeError):
            continue
    
    # 최종 fallback (손실 허용)
    logging.warning(
        f"All encoding attempts failed for {kind}. "
        f"Using cp949 with replacement. Tried: {candidates}"
    )
    
    try:
        txt = data.decode('cp949', errors='replace')
    except Exception:
        txt = data.decode('utf-8', errors='replace')
    
    utf8_bytes = _rewrite_encoding_declaration(txt.encode('utf-8'), kind)
    return (utf8_bytes, 'cp949-fallback')


# ==================== 선언부 재작성 ====================

def _rewrite_encoding_declaration(data: bytes, kind: str) -> bytes:
    """HTML/XML 인코딩 선언부를 UTF-8로 수정"""
    
    if kind == 'html':
        return _rewrite_html_encoding(data)
    elif kind == 'xml':
        return _rewrite_xml_encoding(data)
    return data


def _rewrite_html_encoding(data: bytes) -> bytes:
    """HTML charset 선언을 UTF-8로 수정"""
    
    had_charset = False
    
    # charset 속성 치환
    if _HTML_META_TAG_RE.search(data):
        had_charset = True
        data = _HTML_META_TAG_RE.sub(b'<meta charset="UTF-8">', data, count=0)
    
    # http-equiv 방식 치환
    if _HTML_HTTP_EQUIV_RE.search(data):
        had_charset = True
        data = _HTML_HTTP_EQUIV_RE.sub(b'<meta charset="UTF-8">', data, count=0)
    
    # charset 선언이 없으면 추가
    if not had_charset:
        m = _HEAD_RE.search(data)
        if m:
            data = data[:m.end()] + b'\n<meta charset="UTF-8">' + data[m.end():]
        else:
            m2 = _DOCTYPE_RE.search(data)
            if m2:
                data = data[:m2.end()] + b'\n<meta charset="UTF-8">' + data[m2.end():]
            else:
                data = b'<meta charset="UTF-8">\n' + data
    
    # 중복 meta charset 제거
    data = re.sub(rb'(?is)(<meta\s+charset="UTF-8">\s*){2,}', b'<meta charset="UTF-8">', data)
    
    return data


def _rewrite_xml_encoding(data: bytes) -> bytes:
    """XML 선언의 encoding을 UTF-8로 수정"""
    
    if _XML_DECL_REPL.search(data):
        return _XML_DECL_REPL.sub(
            b'<?xml version="1.0" encoding="UTF-8"?>',
            data,
            count=1
        )
    else:
        # XML 선언이 없으면 추가
        return b'<?xml version="1.0" encoding="UTF-8"?>\n' + data


# ==================== ZIP 처리 ====================

def _safe_zip_members(zf: ZipFile) -> List:
    """ZIP 멤버 보안 검사"""
    infos = [i for i in zf.infolist() if not i.is_dir()]
    
    if len(infos) > MAX_FILES:
        raise ValueError(f"Too many files in ZIP: {len(infos)} (max: {MAX_FILES})")
    
    total_size = sum(i.file_size for i in infos)
    if total_size > MAX_TOTAL_UNCOMPRESSED:
        raise ValueError(f"ZIP too large: {total_size} bytes (max: {MAX_TOTAL_UNCOMPRESSED})")
    
    return infos


def _fix_zip_filename_encoding(info) -> str:
    """ZIP 내 파일명 인코딩 교정 (한글 깨짐 방지)"""
    name = info.filename
    
    # UTF-8 플래그가 없으면 CP437 → CP949 변환 시도
    if not (info.flag_bits & 0x800):
        try:
            name = name.encode('cp437').decode('cp949', errors='ignore')
        except Exception:
            pass
    
    return _normalize_filename(name)


def _classify_member(zf: ZipFile, info) -> Tuple[str, bytes, str]:
    """ZIP 멤버 종류 판별 및 데이터 반환"""
    name = _fix_zip_filename_encoding(info)
    data = zf.read(info)
    kind = sniff_kind(data)
    return (kind, data, name)


def _pick_best(members: List[Tuple[str, bytes, str]]) -> Tuple[str, bytes, str]:
    """ZIP 멤버 중 최적 파일 선택 (종류 우선순위 > 크기)"""
    def sort_key(m):
        kind, data, name = m
        kind_priority = KIND_PRIORITY.index(kind) if kind in KIND_PRIORITY else len(KIND_PRIORITY)
        return (kind_priority, -len(data))
    
    members.sort(key=sort_key)
    return members[0]


def _normalize_zip(base_name: str, zip_bytes: bytes) -> Tuple[str, bytes, str, str]:
    """ZIP 파일 처리 및 정규화"""
    try:
        with ZipFile(BytesIO(zip_bytes)) as zf:
            infos = _safe_zip_members(zf)
            
            if not infos:
                raise ValueError("Empty ZIP file")
            
            # 모든 멤버 분류
            classified = [_classify_member(zf, i) for i in infos]
            
            # 최적 멤버 선택
            kind, data, picked_name = _pick_best(classified)
            
            summary = f"ZIP({len(infos)} files) -> '{picked_name}' ({kind})"
            
            # UTF-8 변환
            if kind == 'html':
                utf8_data, used_enc = _to_utf8_with_rewrite(data, 'html')
                summary += f" [enc: {used_enc}]"
                return ('text/html; charset=UTF-8', utf8_data, f'{base_name}.html', summary)
            
            if kind == 'xml':
                utf8_data, used_enc = _to_utf8_with_rewrite(data, 'xml')
                summary += f" [enc: {used_enc}]"
                return ('application/xml; charset=UTF-8', utf8_data, f'{base_name}.xml', summary)
            
            # 바이너리는 그대로
            return ('application/octet-stream', data, base_name, summary)
            
    except (BadZipFile, ValueError) as e:
        logging.warning(f"ZIP processing failed: {e}")
        return ('application/octet-stream', zip_bytes, f'{base_name}.zip', f"ZIP(failed: {e})")


# ==================== 메인 함수 ====================

def normalize_payload(
    object_key: str,
    body: bytes,
    log_context: Optional[Dict[str, Any]] = None
) -> Tuple[str, bytes, str]:
    """
    다운로드한 원본 데이터를 정규화.
    
    Args:
        object_key: 객체 식별자 (파일명으로 사용)
        body: 원본 바이트 데이터
        log_context: 로깅용 컨텍스트 정보
        
    Returns:
        (content_type, normalized_bytes, filename)
    """
    base_name = _safe_ascii_name(_get_base_name(object_key))
    kind = sniff_kind(body)
    final_summary = f"Detected as '{kind}'"
    
    # ZIP 시그니처가 있으면 종류를 'zip'으로 확정
    if body.startswith(ZIP_SIG):
        kind = 'zip'
    
    # 파일 종류에 따라 처리
    if kind == 'zip':
        content_type, norm_bytes, final_name, summary = _normalize_zip(base_name, body)
        final_summary = summary
        
    elif kind == 'html':
        norm_bytes, used_enc = _to_utf8_with_rewrite(body, 'html')
        content_type = 'text/html; charset=UTF-8'
        final_name = f'{base_name}.html'
        final_summary = f"HTML [enc: {used_enc}]"
        
    elif kind == 'xml':
        norm_bytes, used_enc = _to_utf8_with_rewrite(body, 'xml')
        content_type = 'application/xml; charset=UTF-8'
        final_name = f'{base_name}.xml'
        final_summary = f"XML [enc: {used_enc}]"
        
    else:  # bin
        content_type = 'application/octet-stream'
        norm_bytes = body
        final_name = base_name
    
    # 로깅
    if log_context:
        polling_date = log_context.get('polling_date', '-')
        rcept_date = log_context.get('rcept_dt', '-')
        date_info = rcept_date
        if polling_date != rcept_date:
            date_info += f" (Polled on {polling_date})"
        
        report_name = log_context.get('report_nm', '-')
        corp_name = log_context.get('corp_name', '-')
        
        log_msg = (
            f"Processed | {date_info:<30} | [{corp_name:<15}] | "
            f"{report_name:<50} | Saved as '{final_name}' ({final_summary})"
        )
        logging.info(log_msg)
    
    return content_type, norm_bytes, final_name


# ==================== 유틸리티 ====================

def get_library_status() -> Dict[str, bool]:
    """사용 가능한 라이브러리 상태 반환"""
    return {
        'chardet': HAS_CHARDET,
        'beautifulsoup4': HAS_BS4,
        'lxml': HAS_LXML,
    }


def validate_utf8(data: bytes) -> bool:
    """UTF-8 유효성 검증"""
    try:
        data.decode('utf-8', errors='strict')
        return True
    except UnicodeDecodeError:
        return False