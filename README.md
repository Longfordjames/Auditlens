**CFA AI Investment Challenge 2025–2026 | Team Alpha 1 | Durham University**

**Public repository:** [https://github.com/Longfordjames/Auditlens](https://github.com/Longfordjames/Auditlens) · `git clone https://github.com/Longfordjames/Auditlens.git`

> *From Narrative to Reality: Capturing Alpha by Quantifying the "Integrity Discount" in ESG Disclosures*

---

## Judge Quickstart (CFA Rule 4.4 — zero API cost)

```bash
pip install -r requirements.txt
python3 run_auditlens.py --use-cached --sector-scan
```

Then open: `data/Investment_Committee_Memo.md`, `data/portfolio_tearsheet.html`, `data/provenance_audit.json`, and per-company `data/{company}_auditlens_dashboard.html` (e.g. `shell_auditlens_dashboard.html`). Single-company: `python3 run_auditlens.py --use-cached`.

---

## Overview

AuditLens is a **research-grade competition prototype**: a multi-agent pipeline that automates forensic
ESG auditing to detect greenwashing. It quantifies the **Integrity Discount (I_d)**
— the mathematical gap between a firm's transition narrative and its operational
capital allocation reality.

**Subject:** Four-name energy sector scan (Shell, BP, Ørsted, TotalEnergies) — workbook-backed CSVs

**Key finding (shipped `data/energy_sector/` defaults, `--sector-scan --use-cached`):** Hard metrics are transcribed from the team’s internal ESG workbooks into the six-column audit schema. **Total energy consumption (million MWh)** drives ΔResource Intensity; year columns from the spreadsheets are mapped to AuditLens nodes **2015 / 2021 / 2024** with citations noting the mapping. On that basis, **TotalEnergies** shows rising energy use and a **positive** $I_d$ (HIGH-RISK tier in the demo run), while **Shell, BP, and Ørsted** sit on the **declining-resource branch** with **negative** $I_d$ and green-premium WACC tiers in the latest snapshot. **Drop custom CSVs into `Data_Input/`** (see `Data_Input/README.md`) or pass `--input-dir` to override without editing the repo baseline.

**All data paths are designed for public filing URLs in `source_url`; Phase 2 tightens page-level cites when annual reports are linked.**

**All data is 100% publicly sourced. No proprietary vendors used.**

---

## Architecture: The MAD Protocol

```
┌─────────────────────────────────────────────────────────┐
│  LAYER 1: CorporateAgent (Neural)                       │
│  → LLM extracts ΔNarrative Sentiment from public filings│
│  → Sources: Shell Annual Report 2024, SR 2024           │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│  LAYER 2: AuditorAgent (Ground-Truth OSINT)             │
│  → Reads Data_Input/ then data/energy_sector/*.csv      │
│  → Every metric has source_citation + source_url        │
│  → Computes ΔResource Intensity, Disclosure Density     │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│  LAYER 3: ScepticAgent (Symbolic + optional reflection) │
│  → I_d = ΔSentiment / (ΔResource × Disclosure Density)  │
│  → Deterministic formula, cross-checks, WACC mapping    │
│  → If CSV shows surge vs positive narrative: one cached │
│    LLM “professional skepticism” re-read (single-co run)│
└─────────────────────────────────────────────────────────┘
```

---

## Reproducibility Guide

> **For Judges — CFA Rule 4.4 Compliance**
>
> The `--use-cached` flag reads pre-populated LLM responses from
> `data/api_cache.json`. **No API key is needed. No cost is incurred.**
> All results are bit-for-bit reproducible.

### Step 1: Install dependency

```bash
pip install -r requirements.txt
```

* **`--use-cached` (judges):** No API keys; no network for LLM calls. Cached responses load from `data/api_cache.json` (CFA Rule 4.4).  
* **Live dual-model mode:** Set `ANTHROPIC_API_KEY` and install `anthropic` (see `requirements.txt`) so Layer 1 can call both OpenAI and Anthropic; otherwise the pipeline falls back to OpenAI-only with a logged warning.

### Step 2: Run in cached mode (recommended for judges)

```bash
python3 run_auditlens.py --use-cached
```

**Sector scan + deliverables (leaderboard, portfolio HTML, IC Memo, provenance, fairness):**

```bash
python3 run_auditlens.py --sector-scan --use-cached
```

The CLI uses defensive parsing of public CSVs, bounded WACC signals, and a top-level error panel (no raw tracebacks on success paths). Corrupt `data/api_cache.json` is auto-backed up and replaced with an empty cache with a warning.

### Step 3: Inspect output

- **Console:** Colour-coded glass-box audit log with source citations
- **Files:** `data/{company}_auditlens_report.json`, `{company}_auditlens_summary.md`, `{company}_auditlens_dashboard.html` (default company: `shell`)

### Optional: Live API mode

If you wish to reproduce results from scratch with a real API call:

```bash
export OPENAI_API_KEY="sk-..."   # Your OpenAI key
python3 run_auditlens.py
```

Estimated cost: **< $0.05** using `gpt-4o-mini` at current pricing
(well under the $20 CFA Rule 4.4 threshold).

### Data loader smoke-test

```bash
python3 data_loader.py
```

Prints all 7 audit metrics with their source citations and URLs.

---

## Project Structure

```
AI_Competition/
├── run_auditlens.py              ← Main entrypoint
├── Data_Input/                   ← PM drop zone (optional CSV overrides)
├── data_loader.py                ← Public data pipeline with provenance logging
├── llm_wrapper.py                ← Caching LLM wrapper (Rule 4.4)
├── agents/
│   ├── __init__.py
│   ├── corporate_agent.py        ← Layer 1: Narrative sentiment extraction
│   ├── auditor_agent.py          ← Layer 2: Ground-truth OSINT
│   └── sceptic_agent.py          ← Layer 3: I_d, cross-checks, optional reflection
├── data/
│   ├── shell_public_audit_2024.csv  ← Public audit dataset (7 metrics, cited)
│   ├── energy_sector/               ← Multi-company CSVs for --sector-scan
│   ├── api_cache.json               ← Pre-populated LLM cache (Rule 4.4)
│   └── {company}_auditlens_report.json  ← Per run (e.g. shell_…)
├── README.md
├── technical_explanation.md
└── LICENSE
```

---

## **Reproducibility & Test Suite**

AuditLens ships with a **303-test pytest suite** (`pytest tests/ --collect-only -q`) verifying formulas, caching, agents, valuation, fairness, simulation, and exporters. All tests run **100% offline** — zero API calls, zero cost, zero API key required.

### Run the full test suite

```bash
# Option A: CI/CD runner with rich-formatted PASS/FAIL summary
bash scripts/run_tests.sh

# Option B: Direct pytest (verbose)
python3 -m pytest -v tests/

# Option C: Single module
python3 -m pytest -v tests/test_sceptic_agent.py
```

### Test coverage (representative)

| Module | What is verified |
|--------|------------------|
| `test_sceptic_agent.py` | I_d formula, ε-guards, risk tiers, WACC mapping, trajectory, dual-model bias, neuro-symbolic reflection triggers, sensitivity |
| `test_data_loader.py` | Rule 4.6 schema (`source_citation`, `source_url`), ΔResource, disclosure density, Scope 3 |
| `test_llm_wrapper.py` | SHA-256 cache keys, `--use-cached` miss path, corrupt cache backup |
| `test_valuation_engine.py` | Integrity score, WACC premium, DCF scenarios |
| *(other modules)* | Portfolio optimizer, provenance logger, fairness audit, simulation, PM assistant, HTML/IC paths |

Run `python3 -m pytest tests/ --collect-only -q` for the exact current count.

### PM / custom data (`Data_Input/`)

Portfolio managers can place `{company}_public_audit_2024.csv` in **`Data_Input/`** (see [`Data_Input/README.md`](Data_Input/README.md) and `TEMPLATE_public_audit_2024.csv`). The loader checks that folder **first**, then `data/energy_sector/`, then legacy `data/`. CLI: `python3 run_auditlens.py --input-dir /path/to/folder --company shell --use-cached`.

### Scaling beyond four names (path to completion)

New issuers drop into `data/energy_sector/` as **CSV files** with the same provenance columns (`source_citation`, `source_url`). **Metric aliases** live in [`config/mapping.json`](config/mapping.json); [`agents/auditor_agent.py`](agents/auditor_agent.py) resolves column names via exact alias match then **`difflib`-based fuzzy matching**—extend coverage by editing JSON, not by forking Python for each company.

### Offline guarantee

Tests use `unittest.mock` and `pytest`'s `tmp_path` fixture — no network calls, no writes to production files, no API keys needed. Safe to run during judge evaluation.

### Optional: GitHub Pages (FAQ — “tool” feel)

Not required for a CLI submission. If you want a public static preview:

1. In the GitHub repo: **Settings → Pages → Build and deployment → Deploy from a branch** (e.g. `main` + `/docs` or root).
2. Add `docs/index.html` that links to the repo and explains judges should clone and run locally, **or** copy `data/portfolio_tearsheet.html` into `docs/` as `index.html` (self-contained).
3. Add the published Pages URL to `technical_explanation.md` §2 alongside the repository link.

---

## Data Provenance (CFA Rule 4.6)

All source data is freely and publicly accessible. No paid terminals,
employer data, or proprietary third-party ESG data feeds are used at any stage.

| Metric | Source (current CSV row) | URL (hub / filing) |
|--------|---------------------------|---------------------|
| Total energy consumption (2015/2021/2024 nodes) | Internal ESG workbook extract → `shell_public_audit_2024.csv` | https://reports.shell.com/sustainability-report/2024/ |
| Total Scope 3 GHG Emissions | Same workbook (Scope 3 indirect, million t × 1e6) | https://reports.shell.com/sustainability-report/2024/ |
| General ESG Disclosure Score (proxy) | Workbook DE&I index FY2024 | https://reports.shell.com/annual-report/2024/ |
| Transition Capex Contraction | Workbook Green Capex/Revenue vs 2021 | https://www.shell.com/investors/investor-presentations.html |
| Jurisdictional Arbitrage Flag | Workbook dummy (0); legal cross-check Phase 2 | https://find-and-update.company-information.service.gov.uk/company/04366849 |

---

## Formula

$$I_d = \frac{\Delta \text{Narrative Sentiment}}{\Delta \text{Resource Intensity} \times \text{Disclosure Density}}$$

| Symbol | Definition | Source |
|--------|-----------|--------|
| ΔNarrative Sentiment | Change in LLM-scored management tone (2015→2024) | Shell Annual Reports |
| ΔResource Intensity | Proportional change in **Total energy consumption** (million MWh) on mapped nodes | `data/energy_sector/shell_public_audit_2024.csv` |
| Disclosure Density | ESG / disclosure proxy score / 100 (2024 row) | same CSV |

---

## AI Use Statement (CFA Rules 4.2, 4.5)

| Role | Tool | Purpose |
|------|------|---------|
| Development / writing assistant | Claude (e.g. 3.5 Sonnet via Cursor), ChatGPT-style tools | Architecture, docs, code review (disclosed in `technical_explanation.md` §6.8) |
| Regulatory monitoring | Perplexity AI | Public regulatory update tracking (disclosed in technical explanation) |
| **Runtime — Layer 1** | **OpenAI** `gpt-4o-mini` | Narrative sentiment + verbatim phrase extraction from **public** filing excerpts |
| **Runtime — Layer 1 (dual-model)** | **Anthropic** `claude-3-5-sonnet-20241022` | Independent second score; conservative `min()` consensus; optional if key missing |
| **Runtime — reflection pass** | **OpenAI** `gpt-4o-mini` | Optional second pass when symbolic rules detect narrative vs operations tension (`CorporateAgent_reflection_*` cache keys) |

All runtime LLM calls use `temperature=0.0` and are recorded in `data/api_cache.json`. Reproduce at **$0** with `python3 run_auditlens.py --use-cached` (Rule 4.4).

Core research hypothesis, MAD Protocol design, and I_d formula are **human-originated** by **Zongchen Nan**. The Round 1 concept PDF (*Durham_Alpha1_AuditLens.pdf*, repository root) is authored by Zongchen Nan and frames the problem and Integrity Discount definition extended here for Stage 2.

---

## See also

- [`SUBMISSION_NOTES.md`](SUBMISSION_NOTES.md) — Appendix B export (PDF/Word), push checklist.
- [`STAGE3_READINESS.md`](STAGE3_READINESS.md) — Round 3 presentation storyboard and Q&A.

## License

MIT License — see [LICENSE](LICENSE). Attribution: [NOTICE](NOTICE).
