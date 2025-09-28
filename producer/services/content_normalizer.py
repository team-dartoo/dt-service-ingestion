import os, re, unicodedata, logging
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from typing import Tuple, Optional, List, Dict, Any

READ_HEAD_N = 65536                                 # 파일 종류를 판별하기 위해 읽을 바이트 수
ZIP_SIG = b'PK\x03\x04'                             # ZIP 파일 시그니처

KIND_PRIORITY = ['html', 'xml', 'bin']              # ZIP 파일 내에서 처리할 콘텐츠 종류 우선순위
MAX_FILES = 200                                     # ZIP 파일 내 최대 파일 개수 (보안)
MAX_TOTAL_UNCOMPRESSED = 200 * 1024 * 1024          # ZIP 압축 해제 시 최대 용량 (보안)
_ALIAS = {                                          # 다양한 인코딩 표현을 표준 이름으로 변환하기 위한 별칭
    'ks_c_5601-1987': 'cp949', 'ks_c_5601': 'cp949',
    'x-windows-949': 'cp949', 'windows-949': 'cp949',
    'euckr': 'euc-kr', 'euc_kr': 'euc-kr',
    'utf8': 'utf-8', 'utf-8-sig': 'utf-8-sig',
}

# -------------------- 정규식 정의 --------------------
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
_SAFE_NAME_RE = re.compile(r'[^A-Za-z0-9._-]+')                                         # 안전한 파일명을 위한 정규식

#-------------------- 유틸리티 함수 --------------------
def _normalize_name(name: str) -> str:
    return unicodedata.normalize('NFC', os.path.basename(name).strip())                 # 유니코드 정규화 및 경로 제거

def _safe_ascii_name(name: str) -> str:
    n = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')   # 한글 등을 ASCII로 변환
    n = _SAFE_NAME_RE.sub('_', n).strip('._-')                                          # 안전하지 않은 문자 '_'로 대체
    return n or 'document'

def _get_base_name(filename: str) -> str:
    return os.path.splitext(_normalize_name(filename))[0]                               # 파일명에서 확장자 제거

def _canon(enc: Optional[str]) -> Optional[str]:
    if not enc: return None
    return _ALIAS.get(enc.strip().lower(), enc.strip().lower())                         # 인코딩 이름을 표준 별칭으로 변환

# -------------------- 콘텐츠 분석 및 변환 함수 --------------------
def sniff_kind(buf: bytes) -> str:
    head = buf[:READ_HEAD_N]                                                            # 파일의 실제 내용을 기반으로 종류(html, xml, bin)를 판별
    if _HTML_TAG_RE.search(head) or _HTML_DOCTYPE_RE.search(head) or _META_ANY_RE.search(head) or _HEAD_RE.search(head):
        return 'html'
    if head.lstrip().startswith(b'<?xml'): return 'xml'
    return 'bin'

def _detect_encoding(buf: bytes) -> Optional[str]:
    head = buf[:READ_HEAD_N]                                                            # HTML/XML 문서에 선언된 인코딩을 감지
    m = _XML_DECL_RE.search(head)
    if m: return _canon(m.group(1).decode('ascii', 'ignore'))
    m = _HTML_META_TAG_RE.search(head)
    if m: return _canon(m.group(1).decode('ascii', 'ignore'))
    m = _HTML_HTTP_EQUIV_RE.search(head)
    if m: return _canon(m.group(1).decode('ascii', 'ignore'))
    return None

def _rewrite_decl_to_utf8(kind: str, b: bytes) -> bytes:
    if kind == 'html':                                                                  # HTML/XML 문서 내의 인코딩 선언부를 UTF-8로 강제 수정
        had = False
        if _HTML_META_TAG_RE.search(b): had = True; b = _HTML_META_TAG_RE.sub(b'<meta charset="UTF-8">', b, count=0)
        if _HTML_HTTP_EQUIV_RE.search(b): had = True; b = _HTML_HTTP_EQUIV_RE.sub(b'<meta charset="UTF-8">', b, count=0)
        if not had:                                                                     # 인코딩 선언이 없는 경우 새로 삽입
            m = _HEAD_RE.search(b)
            if m: b = b[:m.end()] + b'\n<meta charset="UTF-8">' + b[m.end():]
            else:
                m2 = _DOCTYPE_RE.search(b)
                if m2: b = b[:m2.end()] + b'\n<meta charset="UTF-8">' + b[m2.end():]
                else: b = b'<meta charset="UTF-8">\n' + b
        return re.sub(rb'(?is)(<meta\s+charset="UTF-8">\s*){2,}', b'<meta charset="UTF-8">', b)
    if kind == 'xml':
        if _XML_DECL_REPL.search(b): return _XML_DECL_REPL.sub(b'<?xml version="1.0" encoding="UTF-8"?>', b, count=1)
        return b'<?xml version="1.0" encoding="UTF-8"?>\n' + b
    return b

def _decode_candidates(detected: Optional[str]) -> List[str]:
    base = [_canon(detected), 'utf-8-sig', 'utf-8', 'cp949', 'euc-kr', 'windows-1252', 'iso-8859-1']
    return [enc for i, enc in enumerate(base) if enc and enc not in base[:i]]           # 중복 제거

def _to_utf8_with_rewrite(buf: bytes, kind: str) -> bytes:
    det = _detect_encoding(buf)                                                         # 다양한 인코딩으로 디코딩을 시도하여 최종적으로 UTF-8로 변환하는 핵심 함수                   
    for enc in _decode_candidates(det):
        try:                                                                            # 엄격한(strict) 디코딩/인코딩 시도
            txt = buf.decode(enc, 'strict'); out = txt.encode('utf-8', 'strict')
            out = _rewrite_decl_to_utf8(kind, out); out.decode('utf-8', 'strict')
            return out
        except Exception: continue
        
    logging.warning(f"strict decode failed for {kind}; fallback with replacements")
    try: txt = buf.decode('cp949', 'replace')                                           # 모든 시도 실패 시, 손실을 감수하고 변환 (Fallback)
    except Exception: txt = buf.decode('utf-8', 'replace')
    return _rewrite_decl_to_utf8(kind, txt.encode('utf-8'))

def _safe_zip_members(zf: ZipFile):
    infos = [i for i in zf.infolist() if not i.is_dir()]                                # 보안 검사: ZIP 파일 내 파일 개수 및 총 용량 제한
    if len(infos) > MAX_FILES: raise ValueError(f"too many files in zip: {len(infos)}")
    total = sum(i.file_size for i in infos)
    if total > MAX_TOTAL_UNCOMPRESSED: raise ValueError(f"zip too large: {total} bytes")
    return infos

def _classify_member(zf: ZipFile, info) -> Tuple[str, bytes, str]:
    name = info.filename                                                                # ZIP 파일 내 개별 멤버의 종류를 판별하고 데이터를 반환
    if not (info.flag_bits & 0x800):                                                    # 깨진 한글 파일명 인코딩 교정 시도
        try: name = name.encode('cp437').decode('cp949', 'ignore')
        except Exception: pass
    data = zf.read(info)
    kind = sniff_kind(data)
    return kind, data, _normalize_name(name)

def _pick_best(members: List[Tuple[str, bytes, str]]) -> Tuple[str, bytes, str]:
    members.sort(key=lambda t: (KIND_PRIORITY.index(t[0]), -len(t[1])))                 # 분류된 멤버들 중 우선순위(종류 > 크기)가 가장 높은 것을 선택
    return members[0]

def _normalize_zip(base_name: str, zip_bytes: bytes) -> Tuple[str, bytes, str, str]:
    try:                                                                                # ZIP 파일의 압축을 해제하고, 가장 유의미한 파일을 선택하여 정규화
        with ZipFile(BytesIO(zip_bytes)) as zf:
            infos = _safe_zip_members(zf)
            if not infos: raise ValueError("empty zip")
            classified = [_classify_member(zf, i) for i in infos]                       # 모든 멤버 종류 판별
            kind, data, picked_name = _pick_best(classified)                            # 최적의 멤버 선택
            
            summary = f"ZIP({len(infos)} files) -> '{picked_name}' ({kind})"            # 로그용 요약 정보 생성
            
            if kind == 'html': return 'text/html; charset=UTF-8', _to_utf8_with_rewrite(data, 'html'), f'{base_name}.html', summary
            if kind == 'xml': return 'application/xml; charset=UTF-8', _to_utf8_with_rewrite(data, 'xml'), f'{base_name}.xml', summary
            return 'application/octet-stream', data, base_name, summary                 # html/xml 외에는 바이너리로 처리
    except (BadZipFile, ValueError) as e:
        logging.warning(f"zip normalize fail: {e}")
        return 'application/octet-stream', zip_bytes, f'{base_name}.zip', "ZIP(failed)"

# -------------------- 메인 함수 --------------------
def normalize_payload(
    object_key: str,
    body: bytes,
    log_context: Optional[Dict[str, Any]] = None
) -> Tuple[str, bytes, str]:
    
    base_name = _safe_ascii_name(_get_base_name(object_key))                            # 다운로드한 원본 데이터를 받아 종류를 판별하고, 각 유형에 맞게 정규화
    kind = sniff_kind(body) 
    final_summary = f"Detected as '{kind}'"

    if body.startswith(ZIP_SIG): kind = 'zip'                                           # ZIP 시그니처가 있으면 종류를 'zip'으로 확정
        
    if kind == 'zip':                                                                   # 파일 종류에 따라 적절한 처리 로직 분기
        content_type, norm_bytes, final_name, summary = _normalize_zip(base_name, body)
        final_summary = summary
    elif kind == 'html':
        content_type, norm_bytes, final_name = 'text/html; charset=UTF-8', _to_utf8_with_rewrite(body, 'html'), f'{base_name}.html'
    elif kind == 'xml':
        content_type, norm_bytes, final_name = 'application/xml; charset=UTF-8', _to_utf8_with_rewrite(body, 'xml'), f'{base_name}.xml'
    else: # bin
        content_type, norm_bytes, final_name = 'application/octet-stream', body, base_name

    if log_context:
        polling_date = log_context.get('polling_date', '-')
        rcept_date = log_context.get('rcept_dt', '-')
        date_info = rcept_date
        if polling_date != rcept_date: date_info += f" (Polled on {polling_date})"

        report_name = log_context.get('report_nm', '-')
        corp_name = log_context.get('corp_name', '-')
        
        log_msg = (
            f"Processed | {date_info:<30} | [{corp_name:<15}] | "
            f"{report_name:<50} | Saved as '{final_name}' ({final_summary})"
        )
        logging.info(log_msg)

    return content_type, norm_bytes, final_name
