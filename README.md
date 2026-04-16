# OAGIS Mapping Assistant

A RAG-powered web app that maps new attributes (from a Technical Data Package or any source system) to the OAGIS standard using your team's prior mappings as ground truth.

## How it works

1. **Ingest** — Upload Excel or CSV files of existing mappings. The app parses each row, extracts the source attribute + OAGIS path (plus optional metadata like data type, description, notes, and context), and embeds them with `sentence-transformers/all-MiniLM-L6-v2` into a persistent ChromaDB collection.
2. **Retrieve** — For each new attribute, the app builds a rich query vector from its name, type, description, and context, then pulls the top-K most similar prior mappings from the vector store.
3. **Recommend** — The retrieved examples are handed to the configured LLM (Claude Sonnet 4.6 by default, via an OpenAI-compatible endpoint) along with a system prompt tuned for OAGIS. The LLM returns 1–3 ranked OAGIS path candidates with confidence, rationale, and supporting examples. Attributes with no close precedent are flagged for human review.

## Running locally

```bash
pip install -r requirements.txt
cp .env.example .env                    # then fill in LLM_API_KEY
python server.py                        # http://localhost:5000
```

The server reads `LLM_API_KEY` from the environment (or `.env`). `data/` is where ChromaDB, SQLite metadata, and ingested files live — it's gitignored. Delete it to wipe state.

## Giving the LLM additional context

There are three ways to layer team-specific context on top of the built-in OAGIS system prompt:

1. **`team_conventions.md`** — a team-editable markdown file at the repo root. It's appended to the system prompt on **every** call under a `TEAM CONVENTIONS` header. Use it for house rules: path notation, preferred nouns, extension patterns, terminology. The file is re-read on every request, so edits don't require a server restart.
2. **Per-request "Extra instructions"** — both the single and batch tabs have an optional textarea for one-off guidance that applies only to that request (e.g. "this batch is all serialized hardware — prefer `/ItemInstance` paths"). Sent as `extra_instructions` in the JSON payload to `/api/map`.
3. **Canonical OAGIS schema** — seed the vector store directly from an OAGIS XSD release so the LLM sees valid schema paths even when no prior mapping is close. See next section.

## Running a batch from a spreadsheet

The **Batch map** tab accepts an Excel or CSV file (`.xlsx`, `.xlsm`, `.xls`, `.csv`) of attributes to map. Upload the file, and the app auto-detects which column holds the attribute name / data type / description / context using the same heuristics as the library ingest. You can override any column from the dropdowns before running. A preview of the first eight parsed rows shows you exactly what will be sent to the LLM.

A legacy pipe-delimited textarea (`name | data_type | description | context`, one row per line) is still available under “Or paste attributes as text” for quick one-off runs without needing a file.

Under the hood this posts the file to `POST /api/batch/parse`, which returns the attribute rows ready to submit to `/api/map`. The parse endpoint caps each run at 2000 rows by default — split larger files into multiple batches.

## Seeding canonical OAGIS paths from the XSD

Out of the box, the assistant's only OAGIS knowledge comes from the mappings you've ingested. For full schema awareness, point `scripts/seed_oagis_xsd.py` at a local copy of the OAGIS XSDs (download from [oagi.org](https://oagi.org)) and it will flatten every element path in the schema and load them into the vector store under a single `OAGIS canonical schema` pseudo-upload, tagged with `kind="canonical"` so the LLM can distinguish them from precedent mappings.

```bash
# Dry run — print stats + sample rows, no server calls
python scripts/seed_oagis_xsd.py --xsd-dir /path/to/OAGIS/10.11/Model/Nouns --dry-run

# Seed everything (uses /api/seed/canonical on a running server)
python scripts/seed_oagis_xsd.py --xsd-dir /path/to/OAGIS/10.11/Model/Nouns

# Seed only specific Nouns (much smaller index, faster retrieval)
python scripts/seed_oagis_xsd.py --xsd-dir .../Nouns \
  --noun ItemMaster --noun ItemInstance \
  --noun PurchaseOrder --noun SalesOrder \
  --noun Invoice --noun Shipment

# Tune recursion depth (default 6)
python scripts/seed_oagis_xsd.py --xsd-dir .../Nouns --max-depth 8
```

Re-running the seeder replaces the existing canonical entries by default (send `replace_existing=false` in the payload to append instead). The seeded schema shows up in the library panel as `OAGIS canonical schema` and can be deleted like any other upload.

At retrieval time, prior mappings and canonical paths are pulled together by cosine similarity, then split into two labeled sections in the prompt so the LLM treats precedents as authoritative and canonical paths as a validity check / fallback.

## Stack

- **FastAPI** — single process, serves static frontend + REST API
- **ChromaDB** (local, persistent) — vector store with cosine similarity
- **sentence-transformers** (`all-MiniLM-L6-v2`, 384-dim) — embeddings, runs on CPU
- **Anthropic Claude** — recommendation generation (swap `DEFAULT_LLM_MODEL` in `server.py`)
- **Vanilla HTML/CSS/JS** — no build step, no framework

## File layout

```
server.py                   # FastAPI app, ingestion + retrieval + LLM pipeline
team_conventions.md         # Team-editable house rules appended to system prompt
scripts/seed_oagis_xsd.py   # CLI: load canonical OAGIS paths from an XSD directory
static/index.html           # Single-page UI
static/app.css              # Styles (dark theme)
static/app.js               # Frontend logic
make_sample.py              # Generates a 30-row synthetic mapping spreadsheet
sample_mappings.xlsx        # Example mappings (ItemMaster, PO, Invoice, Shipment)
data/                       # Runtime state (chroma + sqlite + ingested files) — gitignored
```

## Extending

- **Different LLM** — change `DEFAULT_LLM_MODEL` and/or the `base_url` of `openai_client` in `server.py`. Any OpenAI-compatible chat-completions endpoint works.
- **Refine LLM behavior** — edit `team_conventions.md` to add house rules without touching code. Runtime reload, no restart needed.
- **Different embeddings** — change `EMBED_MODEL_NAME`. Make sure you wipe `data/chroma` after switching, since dims differ.
- **Databricks deployment** — the retrieval pipeline is a pure function of `(query_text, top_k) -> list[dict]`; lift-and-shift into a Databricks notebook and swap ChromaDB for Databricks Vector Search by implementing a parallel `retrieve()` against a Delta-backed index.
- **Add your ontology** — see the seeding section above. `scripts/seed_oagis_xsd.py` walks an OAGIS XSD release and loads every element path as a canonical entry.
