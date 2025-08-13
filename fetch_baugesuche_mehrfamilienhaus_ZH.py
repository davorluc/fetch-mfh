#!/usr/bin/env python3
"""
ZH + ZG Baugesuche (BP-ZH, BP-ZG) → MFH-only (Mehrfamilienhaus-like) with Bauherrschaft.

Flow:
- List via /publications/xml with cantons=ZH,ZG and rubrics=BP-ZH,BP-ZG
- Follow each 'ref' to detail XML
- Extract Bauherrschaft (ZH: precise; ZG: best-known paths + heuristic)
- Filter to MFH-like projects (MFH / Mehrfamilienhaus / Wohnblock / Reihenhaus / Wohnüberbauung / etc.)
- Write CSV: canton, publicationNumber, date, title, bauherrschaft, match_term, ref
"""

import re
import csv
import time
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_LIST = "https://www.amtsblattportal.ch/api/v1/publications/xml"
CANTONS = ["ZH", "ZG"]
RUBRICS = ["BP-ZH", "BP-ZG"]
PAGE_SIZE = 2000
OUTFILE = "baugesuche_ZH_ZG_MFH.csv"
REQUEST_TIMEOUT = 30
MAX_WORKERS = 16

HEADERS = {"User-Agent": "baugesuche-zh-zg-mfh/1.0"}

# --- MFH keyword set (expand as needed) ---
MFH_PATTERNS = [
    r"\bMFH\b",
    r"\bMehrfamilienhaus\b", r"\bMehrfamilienhäuser\b",
    r"\bMehrfamilienwohnhaus\b", r"\bMehrparteienhaus\b", r"\bMehrparteienhäuser\b",
    r"\bWohnblock\b", r"\bWohnanlage\b", r"\bWohnüberbauung\b", r"\bÜberbauung\b",
    r"\bReihenhaus\b", r"\bReihenhäuser\b",
    r"\bReihenfamilienhaus\b", r"\bReiheneinfamilienhaus\b", r"\bReiheneinfamilienhäuser\b",
    r"\bWohnbau\b", r"\bWohnbebauung\b",
    r"\bMehrfamiliengebäude\b", r"\bMehrfamilienwohngebäude\b",
]
MFH_REGEX = re.compile("|".join(MFH_PATTERNS), flags=re.IGNORECASE | re.UNICODE)


def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s


def fetch_list_page(session: requests.Session, page: int) -> Optional[str]:
    params = [
        ("publicationStates", "PUBLISHED"),
        *[( "cantons", c ) for c in CANTONS],
        *[( "rubrics", r ) for r in RUBRICS],
        ("pageRequest.page", page),      # 0-based
        ("pageRequest.size", PAGE_SIZE),
    ]
    r = session.get(BASE_LIST, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        print(f"[LIST] HTTP {r.status_code} on page {page}")
        return None
    return r.text


def parse_list(xml_text: str) -> List[Dict[str, str]]:
    """
    Return items: {ref, publicationNumber, date, title, canton}
    """
    out: List[Dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return out

    pubs = root.findall(".//publication") or root.findall(".//{*}entry")

    def get_text(elem, *paths) -> str:
        for p in paths:
            v = elem.findtext(p)
            if v:
                return v.strip()
        return ""

    for pub in pubs:
        ref = (pub.attrib.get("ref") or "").strip()
        meta = pub.find("meta") or pub.find("{*}meta")
        title_text = ""
        pub_no = ""
        date_text = ""
        canton_code = ""

        if meta is not None:
            title_el = meta.find("title") or meta.find("{*}title")
            if title_el is not None:
                title_text = (
                    (title_el.findtext("de") or title_el.findtext("{*}de")
                     or title_el.findtext("en") or title_el.findtext("{*}en")
                     or title_el.text or "")
                ).strip()
            pub_no = get_text(meta, "publicationNumber", "{*}publicationNumber")
            date_text = get_text(meta, "publicationDate", "{*}publicationDate")
            canton_code = get_text(meta, "cantons", "{*}cantons")
        else:
            title_text = (
                get_text(pub, "title/de", "{*}title/{*}de", "title", "{*}title") or ""
            )
            date_text = get_text(pub, "publicationDate", "{*}publicationDate")
            canton_code = get_text(pub, "cantons", "{*}cantons")

        if ref:
            out.append({
                "ref": ref,
                "publicationNumber": pub_no,
                "date": date_text,
                "title": title_text,
                "canton": (canton_code or "").strip(),
            })
    return out


def fetch_detail_xml(session: requests.Session, ref_url: str) -> Optional[str]:
    r = session.get(ref_url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return None
    return r.text


def text_of(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(text_of(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(p.strip() for p in parts if p and p.strip())


# ---------- Bauherrschaft extractors ----------

def extract_bauherrschaft_precise_zh(root: ET.Element) -> str:
    """
    ZH (BP-ZH01) places Bauherrschaft under <content><buildingContractor>.
    Extract persons/companies cleanly if present.
    """
    bc = root.find(".//{*}content/{*}buildingContractor")
    if bc is None:
        return ""

    pieces: List[str] = []

    # Persons
    for person in bc.findall(".//{*}persons/{*}person"):
        prename = (person.findtext(".//{*}prename") or "").strip()
        name = (person.findtext(".//{*}name") or "").strip()
        full = " ".join(x for x in [prename, name] if x)
        if full:
            # Optional: add address if available
            addr = person.find(".//{*}addressSwitzerland")
            if addr is not None:
                addr_txt = " ".join(filter(None, [
                    addr.findtext(".//{*}street"),
                    addr.findtext(".//{*}houseNumber"),
                    addr.findtext(".//{*}swissZipCode"),
                    addr.findtext(".//{*}town"),
                ]))
                if addr_txt:
                    full = f"{full} ({addr_txt})"
            pieces.append(full)

    # Companies
    for company in bc.findall(".//{*}companies/{*}company"):
        cname = (company.findtext(".//{*}name") or "").strip()
        if cname:
            custom_addr = (company.findtext(".//{*}customAddress") or "").strip().replace("\n", ", ")
            pieces.append(f"{cname} ({custom_addr})" if custom_addr else cname)

    # Fallback: whole block
    if not pieces:
        txt = " ".join(text_of(el) for el in bc)
        return " ".join(txt.split())

    # Dedupe keep order
    seen, uniq = set(), []
    for p in pieces:
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)
    return " | ".join(uniq)


def extract_bauherrschaft_precise_zg(root: ET.Element) -> str:
    """
    Best-effort for ZG (BP-ZG):
    Try under <content> for known containers, else party structures.
    If schema differs, the heuristic will still catch most cases.
    """
    content = root.find(".//{*}content")
    if content is None:
        return ""

    pieces: List[str] = []

    # Common container names we see across schemas
    for path in [
        ".//{*}buildingContractor",
        ".//{*}bauherrschaft",
        ".//{*}gesuchsteller",
        ".//{*}applicant",
        ".//{*}applicants/{*}applicant",
    ]:
        for node in content.findall(path):
            # Prefer structured person/company data if present
            for person in node.findall(".//{*}persons/{*}person"):
                prename = (person.findtext(".//{*}prename") or "").strip()
                name = (person.findtext(".//{*}name") or "").strip()
                full = " ".join(x for x in [prename, name] if x)
                if full:
                    pieces.append(full)
            for company in node.findall(".//{*}companies/{*}company"):
                cname = (company.findtext(".//{*}name") or "").strip()
                if cname:
                    custom_addr = (company.findtext(".//{*}customAddress") or "").strip().replace("\n", ", ")
                    pieces.append(f"{cname} ({custom_addr})" if custom_addr else cname)

            # If nothing structured, take the node’s text
            if not pieces:
                txt = text_of(node).strip()
                if txt:
                    pieces.append(" ".join(txt.split()))

    # If still empty, try generic party structures anywhere in content
    if not pieces:
        for path in [".//{*}party", ".//{*}person", ".//{*}company"]:
            for node in content.findall(path):
                txt = text_of(node).strip()
                if txt:
                    pieces.append(" ".join(txt.split()))

    if not pieces:
        return ""

    # Dedupe keep order
    seen, uniq = set(), []
    for p in pieces:
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)
    return " | ".join(uniq)


def extract_bauherrschaft(detail_xml: str, canton: str) -> str:
    try:
        root = ET.fromstring(detail_xml)
    except ET.ParseError:
        return ""

    # Canton-specific first
    if canton == "ZH":
        val = extract_bauherrschaft_precise_zh(root)
        if val:
            return val
    elif canton == "ZG":
        val = extract_bauherrschaft_precise_zg(root)
        if val:
            return val

    # Heuristic fallback for unknown layouts
    acc = []
    for node in root.iter():
        tag = node.tag.split("}", 1)[-1].lower()
        if "bauherr" in tag or "gesuchstell" in tag:
            txt = text_of(node)
            if txt:
                acc.append(" ".join(txt.split()))
    if acc:
        seen, out = set(), []
        for t in acc:
            if t and t not in seen:
                seen.add(t)
                out.append(t)
        return " | ".join(out)
    return ""


# ---------- MFH classifier ----------

def is_mfh_like(detail_xml: str, title: str) -> Optional[str]:
    """
    Return the matching term if the publication looks like MFH/Wohnblock/Reihenhaus/etc., else None.
    We scan title + projectDescription + full <content> text.
    """
    try:
        root = ET.fromstring(detail_xml)
    except ET.ParseError:
        root = None

    haystack_parts = [title or ""]
    if root is not None:
        pd = root.find(".//{*}content/{*}projectDescription")
        if pd is not None:
            haystack_parts.append(text_of(pd))
        content = root.find(".//{*}content")
        if content is not None:
            haystack_parts.append(text_of(content))

    haystack = " ".join(haystack_parts)
    m = MFH_REGEX.search(haystack)
    return m.group(0) if m else None


def main():
    session = make_session()

    # 1) Paginated list (ZH + ZG)
    page = 0
    items: List[Dict[str, str]] = []
    while True:
        xml_text = fetch_list_page(session, page)
        if not xml_text:
            break
        batch = parse_list(xml_text)
        if not batch:
            break
        items.extend(batch)
        print(f"[LIST] page {page}: {len(batch)} publications")
        if len(batch) < PAGE_SIZE:
            break
        page += 1

    print(f"[LIST] total: {len(items)} publications (ZH+ZG)")

    # 2) Fetch details concurrently, extract Bauherrschaft, filter MFH-like
    results = []
    started = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(fetch_detail_xml, session, it["ref"]): it for it in items}
        for fut in as_completed(futs):
            meta = futs[fut]
            detail = fut.result()
            if not detail:
                continue

            match_term = is_mfh_like(detail, meta.get("title", ""))
            if not match_term:
                continue  # skip non-MFH projects

            canton = (meta.get("canton") or "").strip() or "ZH"
            bauherr = extract_bauherrschaft(detail, canton)
            results.append({
                "canton": canton,
                "publicationNumber": meta.get("publicationNumber", ""),
                "date": meta.get("date", ""),
                "title": meta.get("title", ""),
                "bauherrschaft": bauherr,
                "match_term": match_term,
                "ref": meta["ref"],
            })

    # 3) Write CSV
    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["canton", "publicationNumber", "date", "title", "bauherrschaft", "match_term", "ref"]
        )
        writer.writeheader()
        # sort by canton then date desc
        results.sort(key=lambda r: (r["canton"], r["date"], r["publicationNumber"]), reverse=True)
        writer.writerows(results)

    print(f"[OK] MFH-only: {len(results)} rows → {OUTFILE} in {time.time()-started:.2f}s")


if __name__ == "__main__":
    main()
