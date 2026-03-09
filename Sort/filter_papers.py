"""
=============================================================================
  Atom Probe Tomography — Paper Filter / Relevance Cleaner
=============================================================================
  The scraper caught false positives — papers about "probe" or "tomography"
  in other fields (ultrasound probes, optical coherence tomography, etc.)

  This script:
    1. Scores every paper for APT relevance
    2. Removes clearly irrelevant papers
    3. Saves a cleaned JSON for the downloader to use

  Input  : apt_output/apt_papers_combined.json
  Output : apt_output/apt_papers_filtered.json
           apt_output/apt_papers_rejected.json  (so you can audit removals)
=============================================================================
"""

import json
import os
from datetime import datetime

# =============================================================================
#  CONFIGURATION
# =============================================================================

INPUT_FILE    = "apt_output/apt_papers_combined.json"
OUTPUT_FILE   = "apt_output/apt_papers_filtered.json"
REJECTED_FILE = "apt_output/apt_papers_rejected.json"

# =============================================================================
#  RELEVANCE SCORING
# =============================================================================

# Strong positive signals — paper is almost certainly about APT
APT_STRONG_TERMS = [
    "atom probe tomography",
    "atom probe microscopy",
    "field ion microscopy",
    "field evaporation",
    "atom probe",
    "apt analysis",
    "leap ",           # Local Electrode Atom Probe (LEAP) instrument
    "atom-by-atom",
    "mass spectrum",
    "reconstruction algorithm",
    "proximity histogram",
    "proxigram",
    "iso-concentration surface",
    "isoconcentration",
    "radial distribution function",
    "atom probe data",
    "poscap",
    "reflectron",
    "evaporation field",
    "standing voltage",
    "voltage pulse",
    "laser pulse atom",
    "needle specimen",
    "tip preparation",
    "focused ion beam",   # FIB is commonly used in APT sample prep
]

# Weak positive — relevant but not definitive on their own
APT_WEAK_TERMS = [
    "nanoscale composition",
    "solute clustering",
    "precipitate composition",
    "segregation",
    "grain boundary segregation",
    "spinodal decomposition",
    "compositional analysis",
    "elemental mapping",
    "3d reconstruction",
    "three-dimensional reconstruction",
    "materials characterization",
    "microstructural",
    "nanostructure",
    "alloy composition",
]

# Strong negative signals — paper is almost certainly NOT about APT
# If a paper matches these and has NO strong APT terms, it gets rejected
EXCLUSION_TERMS = [
    "optical coherence tomography",
    "ultrasound probe",
    "endoscopic probe",
    "biopsy needle",
    "medical imaging",
    "computed tomography",
    "ct scan",
    "mri",
    "positron emission",
    "x-ray tomography",
    "electron tomography",   # different technique (TEM-based)
    "neutron tomography",
    "seismic",
    "geological probe",
    "space probe",
    "atmospheric probe",
    "solar probe",
    "plasma probe",
    "langmuir probe",
    "scanning tunneling microscopy",   # different technique
    "afm ",                             # atomic force microscopy
    "atomic force microscopy",
    "scanning electron microscopy",    # SEM alone (without APT context)
    "transmission electron microscopy",# TEM alone
    "raman spectroscopy",
    "fluorescent probe",
    "molecular probe",
    "dna probe",
    "hybridization probe",
    "probe hybridization",
]

# Journals that almost never publish APT papers — extra signal for rejection
EXCLUDED_JOURNALS = [
    "optics",
    "biomedical",
    "medical",
    "clinical",
    "radiology",
    "oncology",
    "cardiology",
    "neuroscience",
    "geophysics",
    "seismology",
    "astronomy",
    "astrophysics",
]


def score_paper(paper: dict) -> tuple[int, list[str]]:
    """
    Score a paper's relevance to Atom Probe Tomography.
    Returns (score, reasons_list).

    Score interpretation:
      >= 2  : Keep (relevant)
      1     : Keep (probably relevant)
      0     : Borderline — keep but flag
      < 0   : Reject (not about APT)
    """
    score   = 0
    reasons = []

    # Combine all text fields for searching
    title    = (paper.get("title", "") or "").lower()
    abstract = (paper.get("abstract", "") or "").lower()
    journal  = (paper.get("journal", "") or "").lower()
    combined = f"{title} {abstract}"

    # ── Strong APT terms ─────────────────────────────────────────
    for term in APT_STRONG_TERMS:
        if term.lower() in combined:
            score += 3
            reasons.append(f"STRONG_MATCH: '{term}'")
            break  # one strong match is enough for a big boost

    # Check for multiple strong matches (extra confidence)
    strong_count = sum(1 for t in APT_STRONG_TERMS if t.lower() in combined)
    if strong_count >= 2:
        score += 2
        reasons.append(f"MULTIPLE_STRONG_MATCHES: {strong_count}")

    # ── Weak APT terms ───────────────────────────────────────────
    weak_count = sum(1 for t in APT_WEAK_TERMS if t.lower() in combined)
    if weak_count >= 1:
        score += weak_count
        reasons.append(f"WEAK_MATCHES: {weak_count}")

    # ── Exclusion terms ──────────────────────────────────────────
    for term in EXCLUSION_TERMS:
        if term.lower() in combined:
            score -= 4
            reasons.append(f"EXCLUSION_TERM: '{term}'")
            break  # one exclusion is enough to penalize heavily

    # ── Journal check ────────────────────────────────────────────
    for jterm in EXCLUDED_JOURNALS:
        if jterm.lower() in journal:
            score -= 2
            reasons.append(f"EXCLUDED_JOURNAL_SIGNAL: '{jterm}' in '{journal}'")
            break

    # ── Title must contain at least one relevant signal ──────────
    # Papers with zero mention of anything APT-related in the title
    # AND no abstract get a penalty
    if not abstract and not any(t.lower() in title for t in APT_STRONG_TERMS):
        score -= 1
        reasons.append("NO_ABSTRACT_NO_TITLE_MATCH")

    return score, reasons


def filter_papers():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading {INPUT_FILE}...")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        papers = json.load(f)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Scoring {len(papers)} papers...")

    kept     = []
    rejected = []

    for paper in papers:
        score, reasons = score_paper(paper)
        paper["_relevance_score"]   = score
        paper["_relevance_reasons"] = reasons

        if score >= 0:
            kept.append(paper)
        else:
            rejected.append(paper)

    # Sort kept papers by relevance score (most relevant first), then year
    kept.sort(key=lambda p: (
        -p.get("_relevance_score", 0),
        -(p.get("year") or 0)
    ))

    # Save outputs
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(kept, f, indent=2, ensure_ascii=False)

    with open(REJECTED_FILE, "w", encoding="utf-8") as f:
        json.dump(rejected, f, indent=2, ensure_ascii=False)

    # Print summary
    print(f"\n{'='*60}")
    print(f"  FILTER COMPLETE")
    print(f"{'='*60}")
    print(f"  Original papers  : {len(papers)}")
    print(f"  Kept (relevant)  : {len(kept)}")
    print(f"  Rejected         : {len(rejected)}")
    print(f"  Removal rate     : {len(rejected)/len(papers)*100:.1f}%")

    oa = sum(1 for p in kept if p.get("oa_pdf_url"))
    print(f"\n  Of kept papers:")
    print(f"    Free PDF links : {oa}")
    print(f"    Need inst. access: {len(kept) - oa}")

    # Show score distribution
    scores = [p["_relevance_score"] for p in kept]
    print(f"\n  Relevance score distribution (kept papers):")
    for threshold in [10, 7, 5, 3, 2, 1, 0]:
        count = sum(1 for s in scores if s >= threshold)
        print(f"    Score >= {threshold:2d} : {count} papers")

    print(f"\n  Output : {OUTPUT_FILE}")
    print(f"  Rejected (audit): {REJECTED_FILE}")
    print(f"{'='*60}")
    print(f"\n  TIP: Open apt_papers_rejected.json to verify the")
    print(f"  removals look correct before running the downloader.")


if __name__ == "__main__":
    filter_papers()
