
import re
import time
import argparse
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/126.0.0.0 Safari/537.36"
}

BARNET_BASE = "https://publicaccess.barnet.gov.uk/online-applications"

POSSIBLE_CONTACT_TABS = ["contacts", "people", "neighbourComments"]

def normalise_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def absolutise(base: str, href: str) -> str:
    if href.startswith("http"):
        return href
    if not href.startswith("/"):
        href = "/" + href
    return base.rstrip("/") + href

def pick_first_appdetails_link(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    a = soup.find("a", href=re.compile(r"applicationDetails\.do"))
    if a and a.get("href"):
        return a["href"]
    return None

def try_direct_reference(ref: str, session: requests.Session) -> Optional[str]:
    url = f"{BARNET_BASE}/applicationDetails.do?reference={ref}"
    r = session.get(url, headers=HEADERS, allow_redirects=True, timeout=30)
    # Some instances may redirect to summary with keyVal; if so, r.url will contain keyVal
    if r.status_code == 200 and "applicationDetails" in r.url:
        return r.url
    # Fallback: simple content check for ref text
    if r.status_code == 200 and ref.replace(" ", "").lower() in re.sub(r"\s+", "", r.text).lower():
        return r.url
    return None

def try_search_get(ref: str, session: requests.Session) -> Optional[str]:
    search_url = f"{BARNET_BASE}/search.do?action=search&searchType=Application&reference={ref}"
    r = session.get(search_url, headers=HEADERS, allow_redirects=True, timeout=30)
    if r.status_code != 200:
        return None
    link = pick_first_appdetails_link(r.text)
    if link:
        return absolutise(BARNET_BASE, link)
    return None

def try_search_post(ref: str, session: requests.Session) -> Optional[str]:
    adv_url = f"{BARNET_BASE}/search.do?action=advanced"
    session.get(adv_url, headers=HEADERS, timeout=30)
    data = {
        "searchType": "Application",
        "searchCriteria.reference": ref,
        "date(applicationValidatedStart)":"",
        "date(applicationValidatedEnd)":"",
        "caseAddressType":"Application",
    }
    r = session.post(f"{BARNET_BASE}/doSearch.do", headers=HEADERS, data=data, allow_redirects=True, timeout=30)
    if r.status_code != 200:
        return None
    link = pick_first_appdetails_link(r.text)
    if link:
        return absolutise(BARNET_BASE, link)
    return None

def extract_keyval_from_url(url: str) -> Optional[str]:
    m = re.search(r"[?&]keyVal=([A-Za-z0-9]+)", url)
    return m.group(1) if m else None

def ensure_summary_url(url: str) -> str:
    # Force activeTab=summary for stability
    if "activeTab=" in url:
        url = re.sub(r"activeTab=[^&]+", "activeTab=summary", url)
    elif "?" in url:
        url = url + "&activeTab=summary"
    else:
        url = url + "?activeTab=summary"
    return url

def resolve_url_from_reference(ref: str, delay: float = 1.0, session: Optional[requests.Session] = None) -> Tuple[Optional[str], str]:
    own = session or requests.Session()
    # Strategy A
    url = try_direct_reference(ref, own)
    if url:
        return ensure_summary_url(url), "direct_reference"
    time.sleep(delay)
    # Strategy B
    url = try_search_get(ref, own)
    if url:
        return ensure_summary_url(url), "search_get"
    time.sleep(delay)
    # Strategy C
    url = try_search_post(ref, own)
    if url:
        return ensure_summary_url(url), "search_post"
    return None, "not_found"

def build_contacts_url(url: str) -> List[str]:
    candidates = []
    if "activeTab=" in url:
        for tab in POSSIBLE_CONTACT_TABS:
            candidates.append(re.sub(r"activeTab=[^&]+", f"activeTab={tab}", url))
    else:
        sep = "&" if "?" in url else "?"
        for tab in POSSIBLE_CONTACT_TABS:
            candidates.append(f"{url}{sep}activeTab={tab}")
    return list(dict.fromkeys(candidates))

def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> Optional[str]:
    try:
        r = session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code == 200 and r.text:
            return r.text
        return None
    except requests.RequestException:
        return None

def parse_contacts_html(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    section_labels = {"applicant": ["applicant"], "agent": ["agent"]}
    for header_tag in soup.select("h1,h2,h3,h4,strong"):
        label = normalise_whitespace(header_tag.get_text(" ")).lower()
        for key, needles in section_labels.items():
            if any(n in label for n in needles):
                container = None
                for sib in header_tag.find_all_next():
                    if sib.name in ["h1", "h2", "h3", "h4", "strong"]:
                        break
                    if sib.name in ["ul", "ol", "dl", "table", "div"] and sib.get_text(strip=True):
                        container = sib
                        break
                if container:
                    text = normalise_whitespace(container.get_text(" "))
                    data[f"{key}_block"] = text
    if not data:
        cards = soup.select(".contact, .contactDetails, .simpleList")
        for c in cards:
            t = normalise_whitespace(c.get_text(" "))
            if "applicant" in t.lower():
                data["applicant_block"] = t
            if "agent" in t.lower():
                data["agent_block"] = t

    def extract_fields(block: str) -> Dict[str, str]:
        out = {}
        pairs = re.findall(r"([A-Za-z ]{3,30}):\s*([^:]+?)(?=(?:[A-Za-z ]{3,30}:)|$)", block)
        for k, v in pairs:
            key = normalise_whitespace(k).lower().replace(" ", "_")
            out[key] = normalise_whitespace(v)
        if "name" not in out:
            m = re.search(r"(?:name|contact)\s*:\s*([^:]+)", block, flags=re.I)
            if m: out["name"] = normalise_whitespace(m.group(1))
        if "company" not in out:
            m = re.search(r"(?:company|organisation)\s*:\s*([^:]+)", block, flags=re.I)
            if m: out["company"] = normalise_whitespace(m.group(1))
        if "address" not in out:
            m = re.search(r"(address)\s*:\s*([^:]+)", block, flags=re.I)
            if m: out["address"] = normalise_whitespace(m.group(2))
        if "telephone" not in out:
            m = re.search(r"(?:tel(?:ephone)?|phone)\s*:\s*([\d +()-]{7,})", block, flags=re.I)
            if m: out["telephone"] = normalise_whitespace(m.group(1))
        if "email" not in out:
            m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", block, flags=re.I)
            if m: out["email"] = normalise_whitespace(m.group(1))
        return out

    result = {}
    if "applicant_block" in data:
        result.update({f"applicant_{k}": v for k, v in extract_fields(data["applicant_block"]).items()})
    if "agent_block" in data:
        result.update({f"agent_{k}": v for k, v in extract_fields(data["agent_block"]).items()})
    return result

def resolve_and_scrape(input_path: Path, out_xlsx: Optional[Path], out_csv: Optional[Path],
                       sheet_name: str="Export", url_col: str="URL", ref_col: str="Reference",
                       delay: float=1.2) -> pd.DataFrame:
    df = pd.read_excel(input_path, sheet_name=sheet_name)
    session = requests.Session()
    rows: List[Dict] = []
    for _, row in df.iterrows():
        r = row.to_dict()
        url = str(r.get(url_col, "") or "").strip()
        ref = str(r.get(ref_col, "") or "").strip()
        status_chain: List[str] = []

        # Resolve URL if missing or looks incomplete
        if not url or "applicationDetails.do" not in url:
            if ref:
                resolved, how = resolve_url_from_reference(ref, delay=delay, session=session)
                r["Resolved URL"] = resolved or ""
                status_chain.append(how)
                url = resolved or ""
            else:
                r["Resolved URL"] = ""
                status_chain.append("no_url_no_ref")
        else:
            r["Resolved URL"] = url
            status_chain.append("url_provided")

        # Try scrape contacts
        if url:
            ok = False
            for candidate in build_contacts_url(url):
                html = fetch_html(session, candidate)
                if html:
                    parsed = parse_contacts_html(html)
                    if parsed:
                        r.update(parsed)
                        r["Scrape Source URL"] = candidate
                        r["Scrape Status"] = "ok"
                        ok = True
                        break
            if not ok:
                r["Scrape Status"] = "no_contacts_or_parse_failed"
        else:
            r["Scrape Status"] = "no_url"

        r["URL Resolve Status"] = " > ".join(status_chain)
        rows.append(r)

    out_df = pd.DataFrame(rows)
    if out_csv:
        out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    if out_xlsx:
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
            out_df.to_excel(writer, index=False, sheet_name="Enriched")
    return out_df

def main():
    ap = argparse.ArgumentParser(description="Barnet Idox: resolve keyVal URL from Reference, then scrape Applicant/Agent.")
    ap.add_argument("input", help="Path to Excel (expects sheet 'Export' with 'Reference' and 'URL' columns).")
    ap.add_argument("--sheet", default="Export", help="Sheet name (default: Export).")
    ap.add_argument("--url-col", default="URL", help="URL column name (default: URL).")
    ap.add_argument("--ref-col", default="Reference", help="Reference column name (default: Reference).")
    ap.add_argument("--out-xlsx", default="barnet_enriched.xlsx", help="Output Excel path.")
    ap.add_argument("--out-csv", default="barnet_enriched.csv", help="Output CSV path.")
    ap.add_argument("--delay", type=float, default=1.2, help="Delay (s) between web requests.")
    args = ap.parse_args()

    df = resolve_and_scrape(Path(args.input), Path(args.out_xlsx), Path(args.out_csv),
                            sheet_name=args.sheet, url_col=args.url_col, ref_col=args.ref_col, delay=args.delay)
    print(f"Saved: {args.out_xlsx}")
    print(f"Saved: {args.out_csv}")
    print(df[['Reference', 'Resolved URL', 'URL Resolve Status', 'Scrape Status']].head())

if __name__ == "__main__":
    main()
