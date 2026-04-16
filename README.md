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

There are two ways to layer team-specific context on top of the built-in OAGIS system prompt:

1. **`team_conventions.md`** — a team-editable markdown file at the repo root. It's appended to the system prompt on **every** call under a `TEAM CONVENTIONS` header. Use it for house rules: path notation, preferred nouns, extension patterns, terminology. The file is re-read on every request, so edits don't require a server restart.
2. **Per-request "Extra instructions"** — both the single and batch tabs have an optional textarea for one-off guidance that applies only to that request (e.g. "this batch is all serialized hardware — prefer `/ItemInstance` paths"). Sent as `extra_instructions` in the JSON payload to `/api/map`.

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
- **Add your ontology** — if you have a machine-readable OAGIS schema (XSD or RDF), you can seed the Chroma collection with canonical paths (empty `source_attribute`, OAGIS path as the key) to give the LLM a grounding even when no prior mapping exists.
