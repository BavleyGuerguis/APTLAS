# APTLAS
### Atom Probe Tomography Literature Atlas

A searchable, LLM-curated database of APT journal articles - filterable by material, instrument, analysis mode, and more.

**By Bavley Guerguis**

---

## Using the Tool

You only need two files: **`index.html`** and **`data.json`**. Download them into the same folder and open `index.html` in any browser. 

The interface lets you search and filter hundreds of curated APT papers by title, author, material system, instrument model, analysis mode, topic, and year. Click any result to expand its full metadata and DOI link.

---

## Repository Structure

```
APTLAS/
├── index.html              ← Open this in your browser
├── data.json               ← The paper database
│
└── llm_sorter/             ← Pipeline used to build data.json
    ├── filter_papers.py    ← Stage 1: keyword-based relevance filter
    ├── analyse_papers.py   ← Stage 2: local LLM reads each PDF
    ├── apt_literature_checker.py  ← Stage 3: validates and exports
    ├── requirements.txt
    └── apt_output/
        ├── analysis_errors.txt
        ├── duplicates_removed.txt
        └── irrelevant_papers.txt
```

The `llm_sorter/` folder is provided for transparency — **you do not need it to use the tool.**

---

## How the LLM Sorting Works

`data.json` was built from a collection of raw APT PDFs using a three-stage local pipeline.

**Stage 1 — Relevance Filter**
Web scrapers are noisy. Terms like "probe" and "tomography" appear in medicine, geology, and astronomy too. Each paper is scored against lists of strong APT terms (e.g. `"field evaporation"`, `"proxigram"`, `"LEAP"`) and exclusion terms (e.g. `"optical coherence tomography"`, `"MRI"`). Papers scoring below zero are rejected.

**Stage 2 — Local LLM Extraction**
Each surviving PDF is passed to a locally-running LLM ([llama3.1](https://ollama.com) via [Ollama](https://ollama.com)) with a structured prompt. The model reads the paper and extracts a fixed set of fields: title, authors, journal, year, topic, material system, instrument, manufacturer, analysis mode, pulse parameters, base temperature, abstract, and keywords. Fields not found in the paper are returned as `null`.

A local model is used deliberately - no data leaves your machine, no API key is needed, and there are no usage costs.

**Stage 3 — Validation & Assembly**
Extracted records are validated for required fields, deduplicated by content hash, and checked for relevance once more using the LLM's own output. The final records are assembled into `data.json`.

---

## Running the Pipeline

> Only needed if you want to rebuild the database from your own PDF collection.

**Requirements:**
```bash
pip install ollama pymupdf tqdm
```
Install [Ollama](https://ollama.com/download), then pull the model:
```bash
ollama pull llama3.1
```

**Run:**
```bash
cd llm_sorter/
python filter_papers.py          # Stage 1
python analyse_papers.py         # Stage 2 — reads PDFs, calls local LLM
python apt_literature_checker.py # Stage 3 — validates and builds data.json
```

Place your PDFs in `apt_output/pdfs/` before running Stage 2.

---

## Known Limitations

- **Null fields:** Many papers don't report every experimental parameter. A `null` value means the information wasn't found.
- **Coverage:** The database reflects papers collected in March 2026. It is not exhaustive and will need periodic updates.

---

*APTLAS is an independent research tool.*