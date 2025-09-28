import os, re, unicodedata, logging
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from typing import Tuple, Optional, List, Dict, Any

READ_HEAD_N = 65536
PDF_SIG = b'%PDF-'
ZIP_SIG = b'PK\x03\x04'

KIND_PRIORITY = ['html', 'xml', 'bin']
MAX_FILES = 200
MAX_TOTAL_UNCOMPRESSED = 200 * 1024 * 1024
_ALIAS = {
    'ks_c_5601-1987': 'cp949', 'ks_c_5601': 'cp949',
    'x-windows-949': 'cp949', 'windows-949': 'cp949',
    'euckr': 'euc-kr', 'euc_kr': 'euc-kr',
    'utf8': 'utf-8', 'utf-8-sig': 'utf-8-sig',
}
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

def _normalize_name(name: str) -> str:
    return unicodedata.normalize('NFC', os.path.basename(name).strip())

def _safe_ascii_name(name: str) -> str:
    n = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    n = _SAFE_NAME_RE.sub('_', n).strip('._-')
    return n or 'document'

def _get_base_name(filename: str) -> str:
    return os.path.splitext(_normalize_name(filename))[0]

def _canon(enc: Optional[str]) -> Optional[str]:
    if not enc: return None
    return _ALIAS.get(enc.strip().lower(), enc.strip().lower())

def sniff_kind(buf: bytes) -> str:
    head = buf[:READ_HEAD_N]
    if _HTML_TAG_RE.search(head) or _HTML_DOCTYPE_RE.search(head) or _META_ANY_RE.search(head) or _HEAD_RE.search(head):
        return 'html'
    if head.lstrip().startswith(b'<?xml'): return 'xml'
    return 'bin'

def _detect_encoding(buf: bytes) -> Optional[str]:
    head = buf[:READ_HEAD_N]
    m = _XML_DECL_RE.search(head)
    if m: return _canon(m.group(1).decode('ascii', 'ignore'))
    m = _HTML_META_TAG_RE.search(head)
    if m: return _canon(m.group(1).decode('ascii', 'ignore'))
    m = _HTML_HTTP_EQUIV_RE.search(head)
    if m: return _canon(m.group(1).decode('ascii', 'ignore'))
    return None

def _rewrite_decl_to_utf8(kind: str, b: bytes) -> bytes:
    if kind == 'html':
        had = False
        if _HTML_META_TAG_RE.search(b):
            had = True
            b = _HTML_META_TAG_RE.sub(b'<meta charset="UTF-8">', b, count=0)
        if _HTML_HTTP_EQUIV_RE.search(b):
            had = True
            b = _HTML_HTTP_EQUIV_RE.sub(b'<meta charset="UTF-8">', b, count=0)
        if not had:
            m = _HEAD_RE.search(b)
            if m:
                b = b[:m.end()] + b'\n<meta charset="UTF-8">' + b[m.end():]
            else:
                m2 = _DOCTYPE_RE.search(b)
                if m2:
                    b = b[:m2.end()] + b'\n<meta charset="UTF-8">' + b[m2.end():]
                else:
                    b = b'<meta charset="UTF-8">\n' + b
        b = re.sub(rb'(?is)(<meta\s+charset="UTF-8">\s*){2,}', b'<meta charset="UTF-8">', b)
        return b
    if kind == 'xml':
        if _XML_DECL_REPL.search(b):
            return _XML_DECL_REPL.sub(b'<?xml version="1.0" encoding="UTF-8"?>', b, count=1)
        return b'<?xml version="1.0" encoding="UTF-8"?>\n' + b
    return b

def _decode_candidates(detected: Optional[str]) -> List[str]:
    base = [_canon(detected), 'utf-8-sig', 'utf-8', 'cp949', 'euc-kr', 'windows-1252', 'iso-8859-1']
    out: List[str] = []
    for enc in base:
        if enc and enc not in out: out.append(enc)
    return out

def _to_utf8_with_rewrite(buf: bytes, kind: str) -> bytes:
    det = _detect_encoding(buf)
    for enc in _decode_candidates(det):
        try:
            txt = buf.decode(enc, 'strict')
            out = txt.encode('utf-8', 'strict')
            out = _rewrite_decl_to_utf8(kind, out)
            out.decode('utf-8', 'strict')
            return out
        except Exception:
            continue
    logging.warning(f"strict decode failed for {kind}; fallback with replacements")
    try:
        txt = buf.decode('cp949', 'replace'); used = 'cp949'
    except Exception:
        txt = buf.decode('utf-8', 'replace'); used = 'utf-8'
    out = txt.encode('utf-8')
    out = _rewrite_decl_to_utf8(kind, out)
    logging.warning(f"{kind} decoded with replacements as {used}")
    return out

def _safe_zip_members(zf: ZipFile):
    infos = [i for i in zf.infolist() if not i.is_dir()]
    if len(infos) > MAX_FILES: raise ValueError(f"too many files in zip: {len(infos)}")
    total = sum(i.file_size for i in infos)
    if total > MAX_TOTAL_UNCOMPRESSED: raise ValueError(f"zip too large: {total} bytes")
    return infos

def _classify_member(zf: ZipFile, info) -> Tuple[str, bytes, str]:
    name = info.filename
    if not (info.flag_bits & 0x800):
        try: name = name.encode('cp437').decode('cp949', 'ignore')
        except Exception: pass
    data = zf.read(info)
    kind = sniff_kind(data)
    return kind, data, _normalize_name(name)

def _pick_best(members: List[Tuple[str, bytes, str]]) -> Tuple[str, bytes, str]:
    members.sort(key=lambda t: (KIND_PRIORITY.index(t[0]) if t[0] in KIND_PRIORITY else len(KIND_PRIORITY), -len(t[1])))
    return members[0]

def _normalize_zip(base_name: str, zip_bytes: bytes) -> Tuple[str, bytes, str, str]:
    try:
        with ZipFile(BytesIO(zip_bytes)) as zf:
            infos = _safe_zip_members(zf)
            classified: List[Tuple[str, bytes, str]] = []
            for i in infos:
                kind, data, name = _classify_member(zf, i)
                classified.append((kind, data, name))
            if not classified: raise ValueError("empty zip")
            kind, data, picked_name = _pick_best(classified)
            
            summary = f"ZIP({len(infos)} files) -> '{picked_name}' ({kind})"
            
            if kind == 'html':
                return 'text/html; charset=UTF-8', _to_utf8_with_rewrite(data, 'html'), f'{base_name}.html', summary
            if kind == 'xml':
                return 'application/xml; charset=UTF-8', _to_utf8_with_rewrite(data, 'xml'), f'{base_name}.xml', summary
            # bin
            return 'application/octet-stream', data, base_name, summary
    except (BadZipFile, ValueError) as e:
        logging.warning(f"zip normalize fail: {e}")
        return 'application/octet-stream', zip_bytes, f'{base_name}.zip', "ZIP(failed)"

def normalize_payload(
    object_key: str,
    body: bytes,
    log_context: Optional[Dict[str, Any]] = None
) -> Tuple[str, bytes, str]:

    base_name = _safe_ascii_name(_get_base_name(object_key))
    kind = sniff_kind(body) 
    final_summary = f"Detected as '{kind}'"

    if body.startswith(ZIP_SIG):
        kind = 'zip'
        
    if kind == 'zip':
        content_type, norm_bytes, final_name, summary = _normalize_zip(base_name, body)
        final_summary = summary
    elif kind == 'html':
        content_type, norm_bytes, final_name = 'text/html; charset=UTF-8', _to_utf8_with_rewrite(body, 'html'), f'{base_name}.html'
    elif kind == 'xml':
        content_type, norm_bytes, final_name = 'application/xml; charset=UTF-8', _to_utf8_with_rewrite(body, 'xml'), f'{base_name}.xml'
    else: 
        content_type, norm_bytes, final_name = 'application/octet-stream', body, base_name

    if log_context:
        polling_date = log_context.get('polling_date', '-')
        rcept_date = log_context.get('rcept_dt', '-')
        
        date_info = rcept_date
        if polling_date != rcept_date:
            date_info += f" (Polled on {polling_date})"

        report_name = log_context.get('report_nm', '-')
        corp_name = log_context.get('corp_name', '-')
        
        log_msg = (
            f"Processed | "
            f"{date_info:<30} | "
            f"[{corp_name:<15}] | "
            f"{report_name:<50} | "
            f"Saved as '{final_name}' ({final_summary})"
        )
        logging.info(log_msg)

    return content_type, norm_bytes, final_name

