"""
APT Literature Checker
======================
Searches CrossRef for all "Atom Probe Tomography" literature,
checks whether each paper already exists in your local PDF folder,
and writes missing DOIs to a text file for download.

Requirements (install once):
    pip install requests PyPDF2 rapidfuzz tqdm

Usage:
    1. Set PDF_FOLDER to the path of your local APT PDF collection.
    2. Run the script. It will produce:
         - missing_dois.txt      → DOIs not found in your folder
         - apt_scan_report.txt   → full log of every paper checked
         - apt_rejected.txt      → papers filtered out as non-APT (for review)
"""

import os
import re
import json
import time
import unicodedata
import requests
import logging
from pathlib import Path
from rapidfuzz import fuzz
from tqdm import tqdm

try:
    import PyPDF2
except ImportError:
    PyPDF2 = None

# ─────────────────────────────────────────────
#  USER CONFIGURATION  ← edit these two paths
# ─────────────────────────────────────────────
PDF_FOLDER   = r"C:\Users\bavle\OneDrive - McMaster University\Materials Science and Engineering\Thesis\APT Literature Database\A(P)TLAS\apt_output\pdfs"   # ← your local collection folder
OUTPUT_DIR   = r"C:\Users\bavle\OneDrive - McMaster University\Materials Science and Engineering\Thesis\APT Literature Database\A(P)TLAS\apt_output"      # ← where results are written
# ─────────────────────────────────────────────

QUERY_TERM          = "Atom Probe Tomography"
CROSSREF_API        = "https://api.crossref.org/works"
ROWS_PER_PAGE       = 100          # CrossRef max per request
MAX_RESULTS         = 10000       # safety cap – raise if you want more
TITLE_MATCH_THRESH  = 88           # fuzzy-match score (0-100); lower = more lenient
MAILTO              = "guerguib@mcmaster.com"  # polite pool – speeds up CrossRef responses

# Output files
missing_dois_file = Path(OUTPUT_DIR) / "missing_dois.txt"
report_file       = Path(OUTPUT_DIR) / "apt_scan_report.txt"
cache_file        = Path(OUTPUT_DIR) / "crossref_cache.json"
rejected_file     = Path(OUTPUT_DIR) / "apt_rejected.txt"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
#  APT RELEVANCE FILTER
# ══════════════════════════════════════════════

# Terms that MUST appear (at least one) for a paper to be considered APT-related.
# Checked against title + abstract (if available).
APT_REQUIRED_TERMS = [
    "atom probe tomography",
    "atom probe",
    "field ion microscopy",
    "field evaporation",
    "atom probe crystallography",
    "local electrode atom probe",
    "leap",          # LEAP instrument – only meaningful with APT context (see below)
]

# If a paper only matches a weak/ambiguous term (e.g. "leap"), it must ALSO contain
# one of these supporting terms to be accepted.
APT_SUPPORTING_TERMS = [
    "atom probe",
    "field evaporation",
    "field ion",
    "reconstruction",
    "mass spectrum",
    "proximity histogram",
    "proxigram",
    "isoconcentration surface",
    "solute clustering",
    "needle specimen",
    "pulsed laser",
    "voltage pulse",
    "evaporation rate",
]

# Terms that — if found in the title alone — immediately disqualify the paper,
# because they indicate a completely different field using "tomography".
APT_REJECT_TITLE_TERMS = [
    "computed tomography",
    "ct scan",
    "x-ray tomography",
    "optical coherence tomography",
    "electron tomography",
    "neutron tomography",
    "muon tomography",
    "seismic tomography",
    "electrical resistance tomography",
    "positron emission tomography",
    "pet scan",
    "photoacoustic tomography",
    "ultrasound tomography",
    "impedance tomography",
    "diffuse optical tomography",
    "fluorescence tomography",
    "microwave tomography",
    "magnetic resonance",
    " mri ",
    "apt-1",          # unrelated product codes
]


def is_apt_relevant(work: dict) -> tuple[bool, str]:
    """
    Returns (relevant: bool, reason: str).
    Checks title (and abstract if present) against inclusion/exclusion term lists.
    """
    titles   = work.get("title") or []
    abstract = work.get("abstract", "") or ""
    title    = titles[0] if titles else ""

    title_norm    = normalise(title)
    abstract_norm = normalise(abstract)
    full_text     = title_norm + " " + abstract_norm

    # ── Hard reject: disqualifying terms in the title
    for bad in APT_REJECT_TITLE_TERMS:
        if bad in title_norm:
            return False, f"rejected: title contains '{bad}'"

    # ── Must contain at least one required APT term in title+abstract
    matched_required = [t for t in APT_REQUIRED_TERMS if t in full_text]
    if not matched_required:
        return False, "rejected: no APT term found"

    # ── If only a weak/ambiguous term matched, require a supporting term too
    strong_terms = [t for t in matched_required if t != "leap"]
    if not strong_terms:
        # Only "leap" matched — check for supporting context
        matched_support = [t for t in APT_SUPPORTING_TERMS if t in full_text]
        if not matched_support:
            return False, "rejected: 'leap' matched but no APT supporting context"

    return True, f"accepted: {matched_required[0]}"



# ══════════════════════════════════════════════
#  STEP 1 – Index your local PDF collection
# ══════════════════════════════════════════════

def normalise(text: str) -> str:
    """Lowercase, remove punctuation/accents, collapse whitespace."""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def extract_doi_from_pdf(path: Path) -> str | None:
    """Try to read the DOI embedded in the PDF metadata or first page text."""
    if PyPDF2 is None:
        return None
    doi_pattern = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+", re.IGNORECASE)
    try:
        with open(path, "rb") as f:
            reader = PyPDF2.PdfReader(f, strict=False)
            # Check XMP / document info
            info = reader.metadata or {}
            for v in info.values():
                m = doi_pattern.search(str(v))
                if m:
                    return m.group(0).rstrip(".")
            # Check first two pages
            for page in list(reader.pages)[:2]:
                text = page.extract_text() or ""
                m = doi_pattern.search(text)
                if m:
                    return m.group(0).rstrip(".")
    except Exception:
        pass
    return None


def build_local_index(folder: str) -> dict:
    """
    Returns a dict with two look-up structures:
      index['dois']   = set of normalised DOIs found in filenames / PDF content
      index['titles'] = list of normalised title strings derived from filenames
    """
    pdf_dir = Path(folder)
    if not pdf_dir.exists():
        raise FileNotFoundError(f"PDF folder not found: {folder}")

    pdf_files = list(pdf_dir.rglob("*.pdf"))
    log.info(f"Indexing {len(pdf_files)} PDFs in {folder} …")

    doi_set     = set()
    title_list  = []   # (normalised_title, original_filename)
    doi_pattern = re.compile(r"10\.\d{4,9}[/_][^\s\"'<>]+")

    for pdf in tqdm(pdf_files, desc="Indexing PDFs", unit="file"):
        stem = pdf.stem

        # ── DOI from filename (common pattern: "10.1007_s11837-21-04567-8.pdf")
        m = doi_pattern.search(stem)
        if m:
            doi_raw = m.group(0).replace("_", "/").rstrip(".")
            doi_set.add(doi_raw.lower())

        # ── DOI from PDF content
        embedded = extract_doi_from_pdf(pdf)
        if embedded:
            doi_set.add(embedded.lower())

        # ── Title proxy from filename
        #    Strip leading DOI-like prefix, replace separators with spaces
        clean = re.sub(r"^10\.\d{4,9}[/_][^\s_]+[_\-]", "", stem)
        clean = re.sub(r"[_\-]+", " ", clean)
        title_list.append((normalise(clean), stem))

    log.info(f"  → {len(doi_set)} DOIs indexed | {len(title_list)} filenames indexed")
    return {"dois": doi_set, "titles": title_list}


# ══════════════════════════════════════════════
#  STEP 2 – Fetch all results from CrossRef
# ══════════════════════════════════════════════

def fetch_crossref_results(query: str, max_results: int, use_cache: bool = True) -> list:
    """Page through CrossRef and return a list of work dicts."""
    cache_path = Path(cache_file)
    if use_cache and cache_path.exists():
        log.info("Loading CrossRef results from cache …")
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)

    all_works = []
    cursor    = "*"
    params    = {
        "query":  query,
        "rows":   ROWS_PER_PAGE,
        "select": "DOI,title,author,published,container-title,abstract",
        "mailto": MAILTO,
    }

    log.info(f"Fetching CrossRef results for: '{query}' …")
    pbar = tqdm(total=max_results, desc="CrossRef pages", unit="rec")

    while len(all_works) < max_results:
        params["cursor"] = cursor
        try:
            r = requests.get(CROSSREF_API, params=params, timeout=30)
            r.raise_for_status()
            data    = r.json()["message"]
            items   = data.get("items", [])
            if not items:
                break
            all_works.extend(items)
            pbar.update(len(items))
            cursor = data.get("next-cursor")
            if not cursor:
                break
            time.sleep(0.12)   # be polite to CrossRef
        except requests.RequestException as e:
            log.warning(f"CrossRef request failed: {e}. Retrying in 5 s …")
            time.sleep(5)

    pbar.close()
    log.info(f"  → {len(all_works)} records retrieved from CrossRef")

    # Cache to disk so you don't re-download on every run
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(all_works, f)
    log.info(f"  → Cached to {cache_path}")

    return all_works


# ══════════════════════════════════════════════
#  STEP 3 – Match each CrossRef record locally
# ══════════════════════════════════════════════

def paper_exists_locally(work: dict, index: dict) -> tuple[bool, str]:
    """
    Returns (found: bool, method: str).
    Uses three cascading strategies:
      1. Exact DOI match
      2. Fuzzy DOI match (handles encoding quirks)
      3. Fuzzy title match against filename index
    """
    raw_doi = work.get("DOI", "").lower().strip()
    titles  = work.get("title", [])
    cr_title = normalise(titles[0]) if titles else ""

    # ── Strategy 1: exact DOI
    if raw_doi and raw_doi in index["dois"]:
        return True, "doi-exact"

    # ── Strategy 2: fuzzy DOI (handles URL-encoded variants, trailing chars)
    if raw_doi:
        for local_doi in index["dois"]:
            if fuzz.ratio(raw_doi, local_doi) >= 97:
                return True, "doi-fuzzy"

    # ── Strategy 3: fuzzy title against filenames
    if cr_title:
        for (local_title, _fname) in index["titles"]:
            score = fuzz.token_sort_ratio(cr_title, local_title)
            if score >= TITLE_MATCH_THRESH:
                return True, f"title-fuzzy({score})"

    return False, "not-found"


# ══════════════════════════════════════════════
#  STEP 4 – Write outputs
# ══════════════════════════════════════════════

def run():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Build local index
    local_index = build_local_index(PDF_FOLDER)

    # Fetch CrossRef literature
    works = fetch_crossref_results(QUERY_TERM, MAX_RESULTS)

    # ── Filter to APT-relevant papers only
    log.info("Filtering CrossRef results for APT relevance …")
    apt_works  = []
    rejected   = []
    for work in tqdm(works, desc="Filtering", unit="paper"):
        relevant, reason = is_apt_relevant(work)
        if relevant:
            apt_works.append(work)
        else:
            title = (work.get("title") or ["(no title)"])[0]
            doi   = work.get("DOI", "")
            rejected.append(f"{reason}  |  {doi}  |  {title[:90]}")

    log.info(f"  → {len(apt_works)} APT-relevant | {len(rejected)} rejected as off-topic")

    # Write rejected list for manual review
    with open(rejected_file, "w", encoding="utf-8") as f:
        f.write(f"Papers filtered out as non-APT ({len(rejected)} total)\n")
        f.write("=" * 70 + "\n")
        f.write("\n".join(rejected))
    log.info(f"  → Rejected list written to: {rejected_file}")

    # Compare APT papers against local collection
    missing = []
    report_lines = [
        f"APT Literature Scan Report",
        f"Query : {QUERY_TERM}",
        f"Total CrossRef records fetched  : {len(works)}",
        f"After APT relevance filter      : {len(apt_works)}",
        f"Rejected as off-topic           : {len(rejected)}",
        f"Match threshold (title fuzzy)   : {TITLE_MATCH_THRESH}",
        "=" * 70,
    ]

    log.info("Comparing APT records against local collection …")
    for work in tqdm(apt_works, desc="Matching", unit="paper"):
        doi   = work.get("DOI", "").strip()
        title = (work.get("title") or ["(no title)"])[0]
        year  = (work.get("published", {}).get("date-parts") or [[""]])[0][0]

        found, method = paper_exists_locally(work, local_index)

        status = "HAVE" if found else "MISSING"
        report_lines.append(
            f"[{status:<7}] ({method:<20}) {year}  {doi}  |  {title[:90]}"
        )

        if not found and doi:
            missing.append(doi)

    # Write missing DOIs
    with open(missing_dois_file, "w", encoding="utf-8") as f:
        f.write("\n".join(missing))
    log.info(f"\n✅ {len(missing)} missing DOIs written to: {missing_dois_file}")

    # Write full report
    report_lines.append("=" * 70)
    report_lines.append(f"MISSING: {len(missing)} / {len(apt_works)} APT papers not found locally.")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    log.info(f"✅ Full report written to: {report_file}")

    # Summary
    print("\n" + "═" * 60)
    print(f"  CrossRef records fetched : {len(works)}")
    print(f"  Passed APT filter        : {len(apt_works)}")
    print(f"  Rejected (off-topic)     : {len(rejected)}")
    print(f"  Already in your folder   : {len(apt_works) - len(missing)}")
    print(f"  Missing (DOIs to fetch)  : {len(missing)}")
    print(f"\n  → missing_dois.txt  : {missing_dois_file}")
    print(f"  → full report       : {report_file}")
    print(f"  → rejected list     : {rejected_file}")
    print("═" * 60)


if __name__ == "__main__":
    run()