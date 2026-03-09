"""
=============================================================================
  Atom Probe Tomography — Stage 3: Local AI PDF Analyser
=============================================================================
  Requirements:
    pip install ollama pymupdf tqdm

    Install Ollama: https://ollama.com/download
    Pull model:     ollama pull llama3.1

  Input  : apt_output/pdfs/
  Output : apt_output/analysis/
             papers_database.json
             papers_database.csv
             irrelevant_papers.txt
             skipped_files.txt
             duplicates_removed.txt
             analysis_errors.txt
=============================================================================
"""

import os, json, csv, re, time, hashlib
import fitz  # PyMuPDF
from datetime import datetime
from tqdm import tqdm

try:
    import ollama
except ImportError:
    print("ERROR: Run:  pip install ollama pymupdf tqdm")
    exit(1)

# =============================================================================
#  CONFIGURATION
# =============================================================================

PDF_DIR         = "apt_output/pdfs"
OUTPUT_DIR      = "apt_output/analysis"
DB_FILE         = "apt_output/analysis/papers_database.json"
CSV_FILE        = "apt_output/analysis/papers_database.csv"
ERROR_FILE      = "apt_output/analysis/analysis_errors.txt"
IRRELEVANT_FILE = "apt_output/analysis/irrelevant_papers.txt"
SKIPPED_FILE    = "apt_output/analysis/skipped_files.txt"

OLLAMA_MODEL        = "llama3.1"
MAX_CHARS_PER_CHUNK = 40000   # Large chunks for RTX 3050 8GB
CHUNK_OVERLAP       = 800
SKIP_ALREADY_DONE   = True

# =============================================================================
#  INSTRUMENT -> MANUFACTURER LOOKUP TABLE  (Fix #3)
#  Every instrument maps to its manufacturer, configuration, and laser.
#  The LLM only needs to identify the instrument name; everything else is derived.
# =============================================================================

MANUFACTURERS = {
    "CAMECA": {
        "instruments": [
            "LEAP 3000X HR", "LEAP 4000X HR", "LEAP 5000 XR", "LEAP 6000 XR",
            "LEAP 3000X Si", "LEAP 4000X Si", "LEAP 5000 XS",
            "EIKOS", "EIKOS-UV", "Invizo 6000",
        ],
        "configurations": {
            "LEAP 3000X HR": "Reflectron Fitted",
            "LEAP 4000X HR": "Reflectron Fitted",
            "LEAP 5000 XR" : "Reflectron Fitted",
            "LEAP 6000 XR" : "Reflectron Fitted",
            "LEAP 3000X Si": "Straight Flight Path",
            "LEAP 4000X Si": "Straight Flight Path",
            "LEAP 5000 XS" : "Straight Flight Path",
            "EIKOS"        : "Straight Flight Path",
            "EIKOS-UV"     : "Straight Flight Path",
            "Invizo 6000"  : "Straight Flight Path",
        },
        "lasers": {
            "LEAP 3000X HR": "532 nm",
            "LEAP 4000X HR": "355 nm",
            "LEAP 5000 XR" : "355 nm",
            "LEAP 6000 XR" : "257.5 nm",
            "LEAP 3000X Si": "532 nm",
            "LEAP 4000X Si": "355 nm",
            "LEAP 5000 XS" : "355 nm",
            "EIKOS"        : "-",
            "EIKOS-UV"     : "355 nm",
            "Invizo 6000"  : "257.5 nm",
        },
    },
    "INSPICO": {
        "instruments": ["Orbis-mAPT"],
        "configurations": {"Orbis-mAPT": "Straight Flight Path"},
        "lasers":         {"Orbis-mAPT": "-"},
    },
    "FAU Erlangen-Nurnberg": {
        "instruments": ["Oxcart"],
        "configurations": {"Oxcart": "Straight Flight Path"},
        "lasers":         {"Oxcart": "-"},
    },
    "CAMECA/GPM": {
        "instruments": ["LAWATAP"],
        "configurations": {"LAWATAP": "Straight Flight Path"},
        "lasers":         {"LAWATAP": "344 nm"},
    },
    "NIST": {
        "instruments": ["EUV Atom Probe", "Photonic Atom Probe"],
        "configurations": {
            "EUV Atom Probe"     : "Straight Flight Path",
            "Photonic Atom Probe": "Straight Flight Path",
        },
        "lasers": {
            "EUV Atom Probe"     : "29.6 nm",
            "Photonic Atom Probe": "266 nm",
        },
    },
}

VALID_INSTRUMENTS = [i for d in MANUFACTURERS.values() for i in d["instruments"]]

# =============================================================================
#  CATEGORY SCHEMA
# =============================================================================

TYPES = [
    "Book", "Thesis", "Journal Article", "Literature Review",
    "Conference Proceeding", "Other",
]

TOPICS = {
    "Instrumentation and Technique Development": [
        "Hardware", "Detector", "Laser Sources", "Ion Optics", "Experimental Modes",
    ],
    "Specimen Preparation and Experimental Methodology": [
        "Electropolishing", "FIB", "Advanced Preparation Techniques",
        "Cryogenic Preparation", "Environmental Transfer", "Optimized Acquisition Parameters",
    ],
    "Data Reconstruction and Physical Modeling": [
        "Reconstruction Algorithms", "Field Evaporation Physics",
        "Spatial Resolution", "Simulations",
    ],
    "Data Analysis and Quantification Methods": [
        "Cluster Detection", "Segregation Analysis", "Proxigrams",
        "Composition Profiles", "Peak Ranging", "Uncertainty Quantification",
    ],
    "Computational and Machine-Learning Approaches": [
        "Automated Ranging", "Clustering Algorithms", "Segmentation",
        "Statistical Analysis", "Automated Data Extraction",
    ],
    "Correlative and Multimodal Characterization": ["-"],
    "Materials Science and Application Studies": [
        "Solid Solutions", "Precipitation", "Grain Boundaries and Interfaces",
        "Dislocations, Stacking Faults and Twins",
        "Microstructural Degradation Processes", "Geology",
        "Biological and Organic Materials",
    ],
}

MATERIAL_SYSTEMS = {
    "Metals and Alloys"    : ["Ferrous", "Non-Ferrous", "Superalloys", "Intermetallic Compounds"],
    "Ceramics and Glasses" : ["Traditional", "Advanced", "Glasses"],
    "Polymers"             : ["Thermoplastics", "Thermosets", "Elastomers"],
    "Composite Materials"  : ["Polymer Matrix", "Metal Matrix", "Ceramic Matrix"],
    "Semiconductors"       : ["IV", "III-V", "II-VI", "IV-IV", "Wide-Bandgap", "Organic", "Oxide", "2D"],
    "Functional Materials" : ["Magnetic", "Piezoelectric", "Optical", "Thermoelectric", "Shape-Memory"],
    "Nanomaterials"        : ["Nanoparticles", "Nanowires", "Thin Films", "Nanocomposites"],
    "Biomaterials"         : ["-"],
    "Geological Materials" : ["-"],
    "None"                 : ["-"],
}

# =============================================================================
#  JOURNAL ABBREVIATION -> FULL NAME MAP  (Fix #7)
#  Sourced from ISIJ International ISO 4 database + common APT journals
# =============================================================================

JOURNAL_ABBREV_MAP = {
    "acc. chem. res."                     : "Accounts of Chemical Research",
    "acta crystallogr."                   : "Acta Crystallographica",
    "acta crystallogr. a"                 : "Acta Crystallographica Section A: Foundations and Advances",
    "acta mater."                         : "Acta Materialia",
    "acta metall."                        : "Acta Metallurgica",
    "acta metall. mater."                 : "Acta Metallurgica et Materialia",
    "acta metall. sin.-engl. lett."       : "Acta Metallurgica Sinica (English Letters)",
    "addit. manuf."                       : "Additive Manufacturing",
    "adv. eng. mater."                    : "Advanced Engineering Materials",
    "adv. mater."                         : "Advanced Materials",
    "appl. phys. lett."                   : "Applied Physics Letters",
    "appl. phys. rev."                    : "Applied Physics Reviews",
    "appl. surf. sci."                    : "Applied Surface Science",
    "calphad"                             : "Calphad-Computer Coupling of Phase Diagrams and Thermochemistry",
    "carbon"                              : "Carbon",
    "ceram. int."                         : "Ceramics International",
    "chem. rev."                          : "Chemical Reviews",
    "comput. mater. sci."                 : "Computational Materials Science",
    "corros. sci."                        : "Corrosion Science",
    "curr. opin. solid state mater. sci." : "Current Opinion in Solid State and Materials Science",
    "electrochim. acta"                   : "Electrochimica Acta",
    "geochim. cosmochim. acta"            : "Geochimica et Cosmochimica Acta",
    "int. j. fatigue"                     : "International Journal of Fatigue",
    "int. j. hydrogen energy"             : "International Journal of Hydrogen Energy",
    "int. j. mater. res."                 : "International Journal of Materials Research",
    "int. j. plast."                      : "International Journal of Plasticity",
    "int. mater. rev."                    : "International Materials Reviews",
    "intermetallics"                      : "Intermetallics",
    "isij int."                           : "ISIJ International",
    "j. alloy. compd."                    : "Journal of Alloys and Compounds",
    "j. am. ceram. soc."                  : "Journal of the American Ceramic Society",
    "j. anal. at. spectrom."              : "Journal of Analytical Atomic Spectrometry",
    "j. appl. crystallogr."               : "Journal of Applied Crystallography",
    "j. appl. phys."                      : "Journal of Applied Physics",
    "j. chem. phys."                      : "Journal of Chemical Physics",
    "j. cryst. growth"                    : "Journal of Crystal Growth",
    "j. electrochem. soc."                : "Journal of the Electrochemical Society",
    "j. eur. ceram. soc."                 : "Journal of the European Ceramic Society",
    "j. magn. magn. mater."               : "Journal of Magnetism and Magnetic Materials",
    "j. mater. eng. perform."             : "Journal of Materials Engineering and Performance",
    "j. mater. process. technol."         : "Journal of Materials Processing Technology",
    "j. mater. res."                      : "Journal of Materials Research",
    "j. mater. res. technol."             : "Journal of Materials Research and Technology",
    "j. mater. sci."                      : "Journal of Materials Science",
    "j. mater. sci. technol."             : "Journal of Materials Science & Technology",
    "j. mech. phys. solids"               : "Journal of the Mechanics and Physics of Solids",
    "j. microsc."                         : "Journal of Microscopy",
    "j. nucl. mater."                     : "Journal of Nuclear Materials",
    "j. nucl. sci. technol."              : "Journal of Nuclear Science and Technology",
    "j. phys. chem. solids"               : "Journal of Physics and Chemistry of Solids",
    "j. phys. d"                          : "Journal of Physics D: Applied Physics",
    "j. phys.-condens. matter"            : "Journal of Physics: Condensed Matter",
    "j. phys.-conf. ser."                 : "Journal of Physics: Conference Series",
    "j. vac. sci. technol."               : "Journal of Vacuum Science & Technology",
    "jom"                                 : "JOM",
    "jpn. j. appl. phys."                 : "Japanese Journal of Applied Physics",
    "mater. charact."                     : "Materials Characterization",
    "mater. des."                         : "Materials & Design",
    "mater. lett."                        : "Materials Letters",
    "mater. res. lett."                   : "Materials Research Letters",
    "mater. sci. eng."                    : "Materials Science and Engineering",
    "mater. sci. eng. a"                  : "Materials Science and Engineering A",
    "mater. sci. technol."                : "Materials Science and Technology",
    "mater. today"                        : "Materials Today",
    "mater. today commun."                : "Materials Today Communications",
    "mater. trans"                        : "Materials Transactions",
    "materialia"                          : "Materialia",
    "metall. mater. trans. a"             : "Metallurgical and Materials Transactions A",
    "metall. mater. trans. b"             : "Metallurgical and Materials Transactions B",
    "metallogr. microstruct. anal."       : "Metallography, Microstructure, and Analysis",
    "microsc. microanal."                 : "Microscopy and Microanalysis",
    "model. simul. mater. sci. eng."      : "Modelling and Simulation in Materials Science and Engineering",
    "mrs bull."                           : "MRS Bulletin",
    "nat. commun."                        : "Nature Communications",
    "nat. mater."                         : "Nature Materials",
    "nature"                              : "Nature",
    "nanostruct. mater."                  : "Nanostructured Materials",
    "philos. mag."                        : "Philosophical Magazine",
    "philos. mag. a"                      : "Philosophical Magazine A",
    "philos. mag. lett."                  : "Philosophical Magazine Letters",
    "phys. rev. b"                        : "Physical Review B",
    "phys. rev. lett."                    : "Physical Review Letters",
    "phys. rev. mater."                   : "Physical Review Materials",
    "prog. mater. sci."                   : "Progress in Materials Science",
    "rev. sci. instrum."                  : "Review of Scientific Instruments",
    "scr. mater."                         : "Scripta Materialia",
    "scr. metall."                        : "Scripta Metallurgica",
    "scr. metall. mater."                 : "Scripta Metallurgica et Materialia",
    "science"                             : "Science",
    "surf. sci."                          : "Surface Science",
    "ultramicroscopy"                     : "Ultramicroscopy",
    "microelectron. eng."                 : "Microelectronic Engineering",
    "thin solid films"                    : "Thin Solid Films",
    "nanoscale"                           : "Nanoscale",
    "nano lett."                          : "Nano Letters",
    "acs nano"                            : "ACS Nano",
    "npj comput. mater."                  : "npj Computational Materials",
    "npj quantum mater."                  : "npj Quantum Materials",
}

def expand_journal(raw: str) -> str:
    if not raw:
        return raw
    key = raw.strip().lower().rstrip(".")
    if key in JOURNAL_ABBREV_MAP:
        return JOURNAL_ABBREV_MAP[key]
    if raw.strip().lower() in JOURNAL_ABBREV_MAP:
        return JOURNAL_ABBREV_MAP[raw.strip().lower()]
    return raw.strip()

# =============================================================================
#  SETUP
# =============================================================================

os.makedirs(OUTPUT_DIR, exist_ok=True)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# =============================================================================
#  FIX #1 — SKIP NON-RESEARCH FILES BEFORE CALLING THE LLM
# =============================================================================

SKIP_FILENAME_KEYWORDS = [
    "manual", "datasheet", "data sheet", "data-sheet",
    "slides", "powerpoint", "presentation", "brochure",
    "catalogue", "catalog", "product guide", "user guide",
    "certificate of analysis", "spec sheet", "specification sheet",
    "installation guide", "quick start", "release notes",
]

def should_skip_by_filename(filename: str) -> str | None:
    name_lower = filename.lower()
    for kw in SKIP_FILENAME_KEYWORDS:
        if kw in name_lower:
            return f"Filename contains '{kw}'"
    return None

def should_skip_by_content(text: str) -> str | None:
    sample = text[:2000].lower()
    signal_groups = [
        ("table of contents", "installation", "warranty"),
        ("part number", "serial number", "model number"),
        ("safety precautions", "do not operate", "power supply"),
    ]
    for group in signal_groups:
        hits = [s for s in group if s in sample]
        if len(hits) >= 2:
            return f"Content looks like a manual/datasheet ({hits})"
    return None

# =============================================================================
#  FIX #9 — DUPLICATE DETECTION
# =============================================================================

def file_hash(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def text_fingerprint(text: str) -> str:
    normalised = re.sub(r"\s+", " ", text[:3000]).strip().lower()
    return hashlib.md5(normalised.encode()).hexdigest()

# =============================================================================
#  PDF TEXT EXTRACTION
# =============================================================================

def extract_text(pdf_path: str) -> str:
    try:
        doc = fitz.open(pdf_path)
        pages = [page.get_text() for page in doc]
        doc.close()
        full = "\n".join(pages)
        full = re.sub(r"\n{3,}", "\n\n", full)
        full = re.sub(r" {3,}", " ", full)
        return full.strip()
    except Exception as e:
        return f"ERROR extracting text: {e}"

def chunk_text(text: str) -> list[str]:
    if len(text) <= MAX_CHARS_PER_CHUNK:
        return [text]
    chunks, start = [], 0
    while start < len(text):
        chunks.append(text[start:start + MAX_CHARS_PER_CHUNK])
        start += MAX_CHARS_PER_CHUNK - CHUNK_OVERLAP
    return chunks

# =============================================================================
#  BUILD PROMPT
# =============================================================================

# Build instrument list block for the prompt so LLM sees the complete mapping
_INST_LINES = []
for _mfr, _data in MANUFACTURERS.items():
    for _inst in _data["instruments"]:
        _cfg = _data["configurations"].get(_inst, "-")
        _las = _data["lasers"].get(_inst, "-")
        _INST_LINES.append(f'  "{_inst}" → {_mfr} | {_cfg} | laser: {_las}')
INSTRUMENT_PROMPT_BLOCK = "\n".join(_INST_LINES)


def build_prompt(text: str, chunk_index: int = 0, total_chunks: int = 1) -> str:
    chunk_note = (
        "This is the complete paper."
        if total_chunks == 1
        else f"This is chunk {chunk_index+1} of {total_chunks}. Extract what is visible; set missing fields to null."
    )
    return f"""You are a materials science expert specialising in Atom Probe Tomography (APT).
Read the paper text and return ONLY a valid JSON object. No explanation, no markdown fences.

=== PAPER TEXT (part {chunk_index+1}/{total_chunks}) ===
{text}
=== END ===
{chunk_note}

Return this exact JSON schema:
{{
  "type": "One of: {', '.join(TYPES)}",
  "title": "Full paper title in Title Case",
  "authors": "All authors as 'Last, First; Last, Second' semicolon-separated, Title Case",
  "first_author_last_name": "Last name only of the first author, Title Case",
  "doi": "Bare DOI only e.g. 10.1016/j.actamat.2020.01.001 — no URL prefix",
  "journal": "FULL unabbreviated journal name e.g. 'Acta Materialia' not 'Acta Mater.'",
  "year": 2024,
  "abstract": "Full abstract or null",
  "keywords": "Comma-separated keywords or null",
  "topic": "The single most accurate topic from: {list(TOPICS.keys())} — or empty string if genuinely unclear",
  "focus": "The exact focus label from the allowed list only — or empty string if not clearly determinable. DO NOT guess.",
  "material_system": "One of: {list(MATERIAL_SYSTEMS.keys())} — or empty string if no material is studied (e.g. perspective, review, methods paper)",
  "material_subclass": "Exact subclass label from the allowed list — or empty string if not determinable. DO NOT guess.",
  "instrument": "EXACT name from this list only — empty string if no instrument is mentioned or used:\\n{INSTRUMENT_PROMPT_BLOCK}",
  "analysis_mode": "Voltage or Laser — empty string if this paper does not report experimental APT measurements",
  "pulse_fraction_or_energy": "Exact value from paper e.g. '20%' or '50 pJ' — empty string if not reported",
  "detection_rate": "Exact value e.g. '0.5%' — empty string if not reported",
  "pulse_rate": "Exact value e.g. '200 kHz' — empty string if not reported",
  "base_temperature": "Exact value e.g. '50 K' — empty string if not reported"
}}

STRICT RULES:
- Books: fill only title, authors, first_author_last_name, doi, year. All other fields null.
- authors: MUST be "Last, First; Last, Second" format. Title Case. Semicolons between authors.
- doi: MUST be bare (strip https://doi.org/ or doi: prefix).
- journal: MUST be full name, never abbreviated.
- instrument: MUST be chosen ONLY from the provided list above. Use '-' for anything else.
- Do NOT include manufacturer, configuration, or laser — these are derived automatically.
- topic: READ THE ENTIRE PAPER before deciding. Use the title, abstract, methods, and conclusions together. Here are guidelines:
    * Papers primarily about instrument hardware, detectors, laser systems, or ion optics → "Instrumentation and Technique Development"
    * Papers about how to prepare specimens (FIB, electropolishing, etc.) → "Specimen Preparation and Experimental Methodology"
    * Papers about reconstruction algorithms, field evaporation physics, spatial resolution → "Data Reconstruction and Physical Modeling"
    * Papers about data analysis methods (cluster detection, proxigrams, composition profiles) → "Data Analysis and Quantification Methods"
    * Papers using machine learning, automated algorithms on APT data → "Computational and Machine-Learning Approaches"
    * Papers using APT alongside TEM, SIMS, XRD, or other techniques → "Correlative and Multimodal Characterization"
    * Papers applying APT to study a specific material or phenomenon → "Materials Science and Application Studies"
- focus: After identifying the topic, pick the most specific focus that matches. Examples:
    * Studying grain boundary segregation → focus: "Grain Boundaries and Interfaces"
    * Studying precipitate formation → focus: "Precipitation"
    * Paper about a new LEAP detector design → focus: "Detector"
    * Paper about FIB sample preparation → focus: "FIB"
    * Paper using multiple characterization methods → topic: Correlative, focus: "-"
- instrument: ONLY fill if the paper explicitly names an instrument from the list in its methods/experimental section. Perspective papers, reviews, and theoretical papers that discuss instruments without using one should have instrument = "".
- analysis_mode / analysis params: ONLY fill if the paper reports actual experimental measurements with specific values. Leave empty for reviews, perspectives, and simulation-only papers.
- material_system / material_subclass: ONLY fill if the paper actually studies a specific material. Leave empty for pure methods papers, instrument development papers, or software papers with no material.
- focus: ONLY use a label from the exact allowed list for the identified topic. If no focus clearly fits, use empty string — do NOT pick the closest-sounding one.
- DO NOT assign values just because a topic is mentioned in passing. The paper must be PRIMARILY about that topic/material/instrument.
- Return ONLY the JSON. Nothing else whatsoever."""


# =============================================================================
#  LLM CALL + MERGE
# =============================================================================

def call_llm_single(prompt: str) -> dict | None:
    for attempt in range(3):
        try:
            if attempt > 0:
                log(f"    Retry {attempt+1}/3...")
                time.sleep(5)
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": 0.1, "num_predict": 1500, "num_ctx": 16384},
            )
            raw = response["message"]["content"].strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            return json.loads(raw.strip())
        except json.JSONDecodeError as e:
            log(f"    JSON parse error: {e}")
            return None
        except Exception as e:
            log(f"    LLM error: {e}")
            if attempt == 2:
                return None
    return None


def merge_results(results: list[dict]) -> dict:
    if not results:
        return {}
    if len(results) == 1:
        return results[0]
    merged = {}
    scalar_fields = [
        "type", "title", "authors", "first_author_last_name", "doi",
        "journal", "year", "topic", "focus",
        "material_system", "material_subclass", "instrument",
        "analysis_mode", "pulse_fraction_or_energy",
        "detection_rate", "pulse_rate", "base_temperature",
    ]
    for field in scalar_fields:
        for r in results:
            val = r.get(field)
            if val and str(val).strip().lower() not in ("null", "none", "unknown", "-", ""):
                merged[field] = val
                break
        if field not in merged:
            merged[field] = results[0].get(field)
    abstracts = [r.get("abstract") for r in results if r.get("abstract")]
    merged["abstract"] = max(abstracts, key=len) if abstracts else ""
    all_kws = []
    for r in results:
        for kw in re.split(r"[,;]", r.get("keywords") or ""):
            kw = kw.strip()
            if kw and kw.lower() not in [k.lower() for k in all_kws]:
                all_kws.append(kw)
    # Strip stray "null" tokens from keyword lists
    all_kws = [k for k in all_kws if k.lower() != "null"]
    merged["keywords"] = ", ".join(all_kws) if all_kws else ""
    return merged


def call_llm(text: str) -> dict | None:
    chunks = chunk_text(text)
    log(f"    {len(text)} chars → {len(chunks)} chunk(s)")
    results = []
    for idx, chunk in enumerate(chunks):
        if len(chunks) > 1:
            log(f"    Chunk {idx+1}/{len(chunks)}...")
        r = call_llm_single(build_prompt(chunk, idx, len(chunks)))
        if r:
            results.append(r)
    return merge_results(results) if results else None

# =============================================================================
#  POST-PROCESSING
# =============================================================================

def pick_closest(value, allowed, default=""):
    """Exact match first, then case-insensitive, then partial. Used for topic/type."""
    if not value:
        return default
    v = str(value).strip()
    if v in allowed:
        return v
    for a in allowed:
        if v.lower() == a.lower():
            return a
    for a in allowed:
        if v.lower() in a.lower() or a.lower() in v.lower():
            return a
    return default


def pick_exact(value, allowed, default=""):
    """Only exact or case-insensitive match — no partial guessing.
    Used for focus, material_subclass, instrument where we must not invent values."""
    if not value:
        return default
    v = str(value).strip()
    if v in allowed:
        return v
    for a in allowed:
        if v.lower() == a.lower():
            return a
    return default


def clean_authors(raw: str) -> tuple[str, str]:
    """
    Fix #4: Normalise to 'Last, First; Last, Second' Title Case.
    Returns (cleaned_string, first_author_last_name).
    """
    if not raw:
        return raw, ""
    parts = re.split(r";| and |\n|&", raw)
    parts = [p.strip() for p in parts if p.strip()]
    cleaned = []
    for author in parts:
        author = author.strip().rstrip(",")
        if "," in author:
            segments = [s.strip() for s in author.split(",", 1)]
            last  = segments[0].title()
            first = segments[1].title() if len(segments) > 1 else ""
            cleaned.append(f"{last}, {first}" if first else last)
        else:
            tokens = author.split()
            if len(tokens) >= 2:
                last  = tokens[-1].title()
                first = " ".join(tokens[:-1]).title()
                cleaned.append(f"{last}, {first}")
            else:
                cleaned.append(author.title())
    result   = "; ".join(cleaned)
    first_ln = cleaned[0].split(",")[0].strip() if cleaned else ""
    return result, first_ln


def clean_doi(raw: str) -> str:
    """Fix #6: Always return bare DOI e.g. 10.1016/j.actamat.2020.01.001"""
    if not raw:
        return raw
    doi = raw.strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.IGNORECASE)
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.IGNORECASE)
    return doi.strip()


def derive_instrument_fields(instrument: str) -> tuple[str, str, str]:
    """
    Fix #3 + #8: Derive manufacturer, configuration, laser from instrument name.
    Only accepts instruments defined in our schema.
    """
    if not instrument or instrument in ("-", "Unknown", "unknown", ""):
        return "", "", ""
    inst_l = instrument.lower().strip()
    # Exact match
    for mfr, data in MANUFACTURERS.items():
        for valid in data["instruments"]:
            if inst_l == valid.lower():
                return mfr, data["configurations"].get(valid, "-"), data["lasers"].get(valid, "-")
    # Partial match
    for mfr, data in MANUFACTURERS.items():
        for valid in data["instruments"]:
            if inst_l in valid.lower() or valid.lower() in inst_l:
                return mfr, data["configurations"].get(valid, "-"), data["lasers"].get(valid, "-")
    # Not in schema — reject (fix #8)
    return "", "", ""


def validate_and_clean(result: dict) -> dict:
    doc_type = pick_closest(result.get("type"), TYPES, "Journal Article")
    result["type"] = doc_type

    # Fix #5: Books get minimal fields only
    if doc_type == "Book":
        raw_auth  = result.get("authors") or ""
        c_auth, _ = clean_authors(raw_auth)
        return {
            "type"                   : "Book",
            "title"                  : (result.get("title") or "").strip(),
            "authors"                : c_auth,
            "first_author_last_name" : (result.get("first_author_last_name") or "").strip().title(),
            "doi"                    : clean_doi(result.get("doi") or ""),
            "year"                   : result.get("year"),
                }

    # Fix #4: Authors
    raw_auth = result.get("authors") or ""
    c_auth, first_ln = clean_authors(raw_auth)
    result["authors"] = c_auth
    result["first_author_last_name"] = (
        (result.get("first_author_last_name") or first_ln or "").strip().title()
    )

    # Fix #6: DOI
    result["doi"] = clean_doi(result.get("doi") or "")

    # Fix #7: Journal full name
    result["journal"] = expand_journal(result.get("journal") or "")

    # Fix #3 + #8: Instrument → derive all instrument fields
    raw_inst   = result.get("instrument") or ""
    # Use exact match only — never guess an instrument
    canon_inst = pick_exact(raw_inst, VALID_INSTRUMENTS, "")
    # Also try partial only for known instrument families (e.g. "LEAP 4000" → "LEAP 4000X HR")
    if not canon_inst and raw_inst and raw_inst not in ("-", "null", "none", "unknown"):
        raw_lower = raw_inst.lower().strip()
        for valid in VALID_INSTRUMENTS:
            # Only match if the raw value is a clear substring of a valid name
            if raw_lower in valid.lower() and len(raw_lower) >= 6:
                canon_inst = valid
                break
    result["instrument"] = canon_inst
    if canon_inst:
        mfr, cfg, laser = derive_instrument_fields(canon_inst)
    else:
        mfr, cfg, laser = "", "", ""
    result["manufacturer"]  = mfr
    result["configuration"] = cfg
    result["laser"]         = laser

    # Analysis params: N/A when not found, and force N/A for non-experimental paper types
    NON_EXPERIMENTAL_TYPES = {"Literature Review", "Book"}
    for ap in ["analysis_mode", "pulse_fraction_or_energy", "detection_rate", "pulse_rate", "base_temperature"]:
        v = result.get(ap)
        if doc_type in NON_EXPERIMENTAL_TYPES:
            result[ap] = "N/A"
        elif not v or str(v).strip().lower() in ("null", "none", "", "unknown", "-"):
            result[ap] = "N/A"

    # Topic / focus
    result["topic"] = pick_closest(result.get("topic"), list(TOPICS.keys()), "")
    if result["topic"] in TOPICS:
        valid_focuses = TOPICS[result["topic"]]
        focus_val = pick_exact(result.get("focus"), valid_focuses, "")
        result["focus"] = focus_val if focus_val else "Other"
    else:
        result["focus"] = "Other"

    # Material
    result["material_system"] = pick_exact(
        result.get("material_system"), list(MATERIAL_SYSTEMS.keys()), ""
    )
    # Fallback: pick_closest only if pick_exact found nothing
    if not result["material_system"]:
        result["material_system"] = pick_closest(
            result.get("material_system"), list(MATERIAL_SYSTEMS.keys()), ""
        )
    if result["material_system"] in MATERIAL_SYSTEMS:
        result["material_subclass"] = pick_exact(
            result.get("material_subclass"),
            MATERIAL_SYSTEMS[result["material_system"]], ""
        )
    else:
        result["material_subclass"] = ""

    return result

# =============================================================================
#  RELEVANCE CHECK
# =============================================================================

APT_SIGNALS = [
    "atom probe", "apt ", "field evaporation", "field ion",
    "leap ", "lawatap", "reflectron", "proxigram",
    "isoconcentration", "reconstruction algorithm",
    "evaporation field", "voltage pulse", "laser pulse",
    "mass-to-charge", "time-of-flight",
]

def is_relevant(result: dict, text: str) -> tuple[bool, str]:
    if result.get("type") == "Book":
        return False, "Document type is Book — excluded per schema"
    text_l = text.lower()
    found  = [s for s in APT_SIGNALS if s in text_l]
    if not found:
        return False, "No APT-related terms found in text"
    if result.get("topic") == "Unknown" and len(found) < 2:
        return False, f"Topic unidentifiable, weak APT signals: {found}"
    return True, ""

# =============================================================================
#  CSV
# =============================================================================

CSV_FIELDS = [
    "title", "authors", "first_author_last_name", "doi", "journal",
    "year", "abstract", "keywords",
    "type", "topic", "focus",
    "material_system", "material_subclass",
    "manufacturer", "instrument", "configuration", "laser",
    "analysis_mode", "pulse_fraction_or_energy",
    "detection_rate", "pulse_rate", "base_temperature",
]

def save_csv(database: list[dict]):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(database)

# =============================================================================
#  MAIN
# =============================================================================

def main():
    log("=" * 60)
    log(f"  APT Stage 3 — PDF Analyser  |  model: {OLLAMA_MODEL}")
    log("=" * 60)

    try:
        model_names = [m.model for m in ollama.list().models]
        log(f"Ollama OK. Models: {model_names}")
        if not any(OLLAMA_MODEL in m for m in model_names):
            log(f"WARNING: '{OLLAMA_MODEL}' not found. Run: ollama pull {OLLAMA_MODEL}")
            return
    except Exception as e:
        log(f"ERROR: Cannot connect to Ollama — {e}")
        return

    # Resume: load list of already-processed filenames from sidecar file
    SIDECAR = DB_FILE + ".processed.txt"
    database = []
    already_done_files = set()
    if SKIP_ALREADY_DONE:
        if os.path.exists(SIDECAR):
            already_done_files = set(open(SIDECAR, encoding="utf-8").read().splitlines())
            log(f"Resuming — {len(already_done_files)} files already processed")
        if os.path.exists(DB_FILE):
            with open(DB_FILE, encoding="utf-8") as f:
                database = json.load(f)

    pdf_files = sorted(f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf"))
    log(f"Found {len(pdf_files)} PDFs")

    # Deduplicate by file hash + text fingerprint
    log("Checking for duplicates...")
    seen_hashes, seen_fps = {}, {}
    unique_files, skipped_dupes = [], []
    for fname in pdf_files:
        path = os.path.join(PDF_DIR, fname)
        fh = file_hash(path)
        if fh in seen_hashes:
            skipped_dupes.append((fname, f"Identical bytes to '{seen_hashes[fh]}'"))
            continue
        text_preview = extract_text(path)
        fp = text_fingerprint(text_preview)
        if fp in seen_fps:
            skipped_dupes.append((fname, f"Same content as '{seen_fps[fp]}'"))
            continue
        seen_hashes[fh] = fname
        seen_fps[fp]    = fname
        unique_files.append(fname)

    if skipped_dupes:
        log(f"Removed {len(skipped_dupes)} duplicates → {len(unique_files)} unique PDFs")
        with open(os.path.join(OUTPUT_DIR, "duplicates_removed.txt"), "w", encoding="utf-8") as f:
            f.write(f"# {len(skipped_dupes)} duplicates removed\n\n")
            for fname, reason in skipped_dupes:
                f.write(f"{fname}\n  → {reason}\n\n")
    else:
        log("No duplicates found")

    errors, irrelevant, skipped_files = [], [], []
    new_count = 0

    for i, filename in enumerate(tqdm(unique_files, desc="Analysing")):
        if filename in already_done_files:
            continue

        pdf_path = os.path.join(PDF_DIR, filename)
        log(f"\n[{i+1}/{len(unique_files)}] {filename[:70]}")

        # Skip manuals/datasheets by filename
        skip_reason = should_skip_by_filename(filename)
        if skip_reason:
            log(f"  → SKIPPED: {skip_reason}")
            skipped_files.append((filename, skip_reason))
            already_done_files.add(filename)
            continue

        # Extract text
        text = extract_text(pdf_path)
        if text.startswith("ERROR"):
            log(f"  Text extraction failed")
            errors.append(f"{filename}: {text}")
            continue

        # Skip manuals/datasheets by content
        skip_reason = should_skip_by_content(text)
        if skip_reason:
            log(f"  → SKIPPED: {skip_reason}")
            skipped_files.append((filename, skip_reason))
            already_done_files.add(filename)
            continue

        log(f"  Extracted {len(text)} chars")
        log(f"  Calling {OLLAMA_MODEL}...")
        result = call_llm(text)
        if not result:
            log("  LLM returned no valid JSON")
            errors.append(f"{filename}: LLM returned invalid JSON")
            continue

        result = validate_and_clean(result)
        log(f"  Type      : {result.get('type')}")
        log(f"  Title     : {str(result.get('title',''))[:60]}")
        log(f"  Authors   : {str(result.get('authors',''))[:60]}")
        log(f"  DOI       : {result.get('doi')}")
        log(f"  Journal   : {result.get('journal')}")
        log(f"  Topic     : {result.get('topic')} → {result.get('focus')}")
        log(f"  Material  : {result.get('material_system')} → {result.get('material_subclass')}")
        log(f"  Instrument: {result.get('manufacturer')} / {result.get('instrument')}")

        relevant, reason = is_relevant(result, text)
        if not relevant:
            log(f"  ✗ EXCLUDED — {reason}")
            irrelevant.append({
                "filename": filename,
                "title"   : result.get("title", ""),
                "type"    : result.get("type", ""),
                "reason"  : reason,
            })
        else:
            log(f"  ✓ Added to database")
            database.append(result)
            new_count += 1

        # Incremental save — write DB and mark file as done
        already_done_files.add(filename)
        with open(SIDECAR, "a", encoding="utf-8") as sf:
            sf.write(filename + "\n")
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(database, f, indent=2, ensure_ascii=False)
        save_csv(database)

    # Write output logs
    if errors:
        with open(ERROR_FILE, "w", encoding="utf-8") as f:
            f.write(f"Errors — {datetime.now()}\n\n")
            for e in errors:
                f.write(e + "\n")

    if irrelevant:
        with open(IRRELEVANT_FILE, "w", encoding="utf-8") as f:
            f.write(f"# {len(irrelevant)} excluded (not APT-relevant)\n\n")
            for idx, r in enumerate(irrelevant, 1):
                f.write(f"{idx}. {r['title'][:80]}\n   {r['filename']}\n   {r['reason']}\n\n")

    if skipped_files:
        with open(SKIPPED_FILE, "w", encoding="utf-8") as f:
            f.write(f"# {len(skipped_files)} skipped (manuals/datasheets/slides)\n\n")
            for idx, (fname, reason) in enumerate(skipped_files, 1):
                f.write(f"{idx}. {fname}\n   {reason}\n\n")

    log("\n" + "=" * 60)
    log("  DONE")
    log("=" * 60)
    log(f"  Unique PDFs          : {len(unique_files)}")
    log(f"  Duplicates removed   : {len(skipped_dupes)}")
    log(f"  Newly analysed       : {new_count}")
    log(f"  Skipped (manual etc) : {len(skipped_files)}")
    log(f"  Excluded (irrelevant): {len(irrelevant)}")
    log(f"  Errors               : {len(errors)}")
    log(f"  Database : {DB_FILE}  ({len(database)} total papers)")
    log(f"  CSV      : {CSV_FILE}")
    log("=" * 60)


if __name__ == "__main__":
    main()