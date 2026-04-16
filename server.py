"""
OAGIS Mapping Assistant — RAG over previous attribute mappings.

Backend: FastAPI
Vector store: ChromaDB (persistent, on-disk)
Embeddings: sentence-transformers (all-MiniLM-L6-v2, local)
LLM: Anthropic Claude via platform credentials
"""

import hashlib
import io
import json
import logging
import os
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import chromadb
import pandas as pd
from anthropic import Anthropic
from chromadb.config import Settings
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("oagis")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
CHROMA_DIR = DATA_DIR / "chroma"
META_DB = DATA_DIR / "meta.sqlite"
STATIC_DIR = BASE_DIR / "static"

EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
COLLECTION_NAME = "oagis_mappings"
DEFAULT_LLM_MODEL = "claude_sonnet_4_6"

# ---------------------------------------------------------------------------
# Shared singletons
# ---------------------------------------------------------------------------
log.info("Loading embedding model: %s", EMBED_MODEL_NAME)
embedder = SentenceTransformer(EMBED_MODEL_NAME)
log.info("Embedding model ready (dim=%d)", embedder.get_sentence_embedding_dimension())

chroma_client = chromadb.PersistentClient(
    path=str(CHROMA_DIR),
    settings=Settings(anonymized_telemetry=False, allow_reset=True),
)
collection = chroma_client.get_or_create_collection(
    name=COLLECTION_NAME,
    metadata={"hnsw:space": "cosine"},
)
log.info("Chroma collection ready: %s (count=%d)", COLLECTION_NAME, collection.count())

anthropic_client = Anthropic()


# ---------------------------------------------------------------------------
# Metadata DB (tracks uploaded files and column mappings)
# ---------------------------------------------------------------------------
@contextmanager
def db():
    conn = sqlite3.connect(META_DB)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS uploads (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                sheet_name TEXT,
                row_count INTEGER NOT NULL,
                columns_json TEXT NOT NULL,
                created_at REAL NOT NULL
            );
        """)


init_db()


# ---------------------------------------------------------------------------
# Ingestion helpers
# ---------------------------------------------------------------------------
def _clean(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none") else s


def build_document(row: dict, columns: dict) -> tuple[str, dict]:
    """Build the embedding document and metadata for one row.

    columns is a dict like:
        {"source_attribute": "Attribute Name",
         "oagis_path": "OAGIS XPath",
         "data_type": "Type",               # optional
         "description": "Description",      # optional
         "notes": "Mapping Notes",          # optional
         "context": "Source System"}         # optional
    """
    # Pull values. Any column that isn't mapped is ignored.
    values = {role: _clean(row.get(col, "")) for role, col in columns.items() if col}

    src = values.get("source_attribute", "")
    path = values.get("oagis_path", "")
    dtype = values.get("data_type", "")
    desc = values.get("description", "")
    notes = values.get("notes", "")
    context = values.get("context", "")

    # Rich text for embedding — every signal concatenated, labeled.
    parts = []
    if src:
        parts.append(f"Attribute: {src}")
    if dtype:
        parts.append(f"Type: {dtype}")
    if desc:
        parts.append(f"Description: {desc}")
    if context:
        parts.append(f"Source Context: {context}")
    if path:
        parts.append(f"Mapped to OAGIS: {path}")
    if notes:
        parts.append(f"Notes: {notes}")
    doc = " | ".join(parts)

    # Chroma metadata must be primitive types.
    meta = {
        "source_attribute": src,
        "oagis_path": path,
        "data_type": dtype,
        "description": desc,
        "notes": notes,
        "context": context,
    }
    return doc, meta


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------
class PreviewResponse(BaseModel):
    upload_id: str
    sheets: list[str]
    active_sheet: str
    columns: list[str]
    preview_rows: list[dict]
    detected: dict  # suggested role -> column mapping


class CommitRequest(BaseModel):
    upload_id: str
    sheet_name: str
    columns: dict  # role -> column name
    replace_existing: bool = False


class AttributeQuery(BaseModel):
    name: str
    data_type: Optional[str] = ""
    description: Optional[str] = ""
    context: Optional[str] = ""


class MapRequest(BaseModel):
    attributes: list[AttributeQuery]
    top_k: int = 6
    model: Optional[str] = DEFAULT_LLM_MODEL


# ---------------------------------------------------------------------------
# Column auto-detection heuristics
# ---------------------------------------------------------------------------
DETECTION_HINTS = {
    "source_attribute": [
        "source attribute",
        "attribute name",
        "attribute",
        "field name",
        "source field",
        "tdp attribute",
        "element",
        "name",
    ],
    "oagis_path": [
        "oagis path",
        "oagis",
        "target path",
        "xpath",
        "target",
        "mapping",
        "mapped to",
        "bod path",
        "noun path",
    ],
    "data_type": ["data type", "type", "datatype", "format"],
    "description": ["description", "definition", "meaning", "desc"],
    "notes": ["notes", "comment", "comments", "rationale", "justification"],
    "context": ["source system", "system", "domain", "package", "tdp", "context"],
}


def detect_columns(columns: list[str]) -> dict:
    lc = {c: c.lower().strip() for c in columns}
    chosen: dict = {}
    for role, hints in DETECTION_HINTS.items():
        best = None
        best_score = 0
        for col, col_lc in lc.items():
            if col in chosen.values():
                continue
            for i, hint in enumerate(hints):
                if col_lc == hint:
                    score = 100 - i
                elif col_lc.startswith(hint) or col_lc.endswith(hint):
                    score = 80 - i
                elif hint in col_lc:
                    score = 60 - i
                else:
                    continue
                if score > best_score:
                    best_score = score
                    best = col
        if best:
            chosen[role] = best
    return chosen


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="OAGIS Mapping Assistant")


@app.get("/")
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "mappings_indexed": collection.count(),
        "embedding_model": EMBED_MODEL_NAME,
        "llm_model": DEFAULT_LLM_MODEL,
    }


@app.get("/api/uploads")
def list_uploads():
    with db() as conn:
        rows = conn.execute(
            "SELECT id, filename, sheet_name, row_count, created_at FROM uploads ORDER BY created_at DESC"
        ).fetchall()
    return {"uploads": [dict(r) for r in rows], "total_indexed": collection.count()}


@app.delete("/api/uploads/{upload_id}")
def delete_upload(upload_id: str):
    with db() as conn:
        row = conn.execute("SELECT id FROM uploads WHERE id = ?", (upload_id,)).fetchone()
        if not row:
            raise HTTPException(404, "upload not found")
        conn.execute("DELETE FROM uploads WHERE id = ?", (upload_id,))
    # Drop vectors that belong to this upload
    try:
        collection.delete(where={"upload_id": upload_id})
    except Exception as e:  # pragma: no cover
        log.warning("chroma delete failed: %s", e)
    return {"ok": True}


@app.post("/api/upload/preview", response_model=PreviewResponse)
async def upload_preview(
    file: UploadFile = File(...),
    sheet: Optional[str] = Form(None),
):
    """Stage an uploaded spreadsheet: return sheets, columns, preview rows, and
    auto-detected role mappings. No vectors are written until /commit is called."""
    if not file.filename:
        raise HTTPException(400, "no filename")

    raw = await file.read()
    if not raw:
        raise HTTPException(400, "empty file")

    upload_id = uuid.uuid4().hex[:12]
    staging_path = DATA_DIR / f"staging_{upload_id}_{file.filename}"
    staging_path.write_bytes(raw)

    try:
        xls = pd.ExcelFile(staging_path, engine="openpyxl") if staging_path.suffix.lower() in (".xlsx", ".xlsm", ".xls") else None
        if xls is None:
            # CSV fallback
            df = pd.read_csv(staging_path)
            sheets = ["(csv)"]
            active_sheet = "(csv)"
        else:
            sheets = xls.sheet_names
            active_sheet = sheet if sheet and sheet in sheets else sheets[0]
            df = pd.read_excel(staging_path, sheet_name=active_sheet, engine="openpyxl")
    except Exception as e:
        staging_path.unlink(missing_ok=True)
        raise HTTPException(400, f"could not parse file: {e}")

    df = df.fillna("")
    columns = [str(c) for c in df.columns]
    preview = df.head(8).astype(str).to_dict(orient="records")
    detected = detect_columns(columns)

    return PreviewResponse(
        upload_id=upload_id,
        sheets=sheets,
        active_sheet=active_sheet,
        columns=columns,
        preview_rows=preview,
        detected=detected,
    )


@app.post("/api/upload/commit")
def upload_commit(req: CommitRequest):
    """Index the staged upload into ChromaDB using the user-confirmed column mapping."""
    staging_candidates = list(DATA_DIR.glob(f"staging_{req.upload_id}_*"))
    if not staging_candidates:
        raise HTTPException(404, "staged upload not found (may have expired)")
    staging_path = staging_candidates[0]
    filename = staging_path.name.split(f"staging_{req.upload_id}_", 1)[-1]

    if not req.columns.get("source_attribute") or not req.columns.get("oagis_path"):
        raise HTTPException(400, "source_attribute and oagis_path column mappings are required")

    try:
        if staging_path.suffix.lower() == ".csv":
            df = pd.read_csv(staging_path)
        else:
            df = pd.read_excel(staging_path, sheet_name=req.sheet_name, engine="openpyxl")
    except Exception as e:
        raise HTTPException(400, f"could not re-parse file: {e}")

    df = df.fillna("")
    log.info("Committing %d rows from %s / %s", len(df), filename, req.sheet_name)

    if req.replace_existing:
        # Wipe and restart
        chroma_client.delete_collection(COLLECTION_NAME)
        global collection
        collection = chroma_client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        with db() as conn:
            conn.execute("DELETE FROM uploads")

    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict] = []
    skipped = 0

    for _, row in df.iterrows():
        rowd = {str(k): v for k, v in row.to_dict().items()}
        doc, meta = build_document(rowd, req.columns)
        if not meta["source_attribute"] or not meta["oagis_path"]:
            skipped += 1
            continue
        # Stable hash so re-ingesting the same file doesn't duplicate
        h = hashlib.sha1(
            f"{meta['source_attribute']}||{meta['oagis_path']}||{meta['description']}".encode()
        ).hexdigest()[:16]
        meta["upload_id"] = req.upload_id
        meta["source_file"] = filename
        ids.append(h)
        docs.append(doc)
        metas.append(meta)

    if not docs:
        raise HTTPException(400, "no valid rows found with both a source attribute and OAGIS path")

    # Batch-embed for speed
    log.info("Embedding %d rows...", len(docs))
    t0 = time.time()
    embeddings = embedder.encode(docs, batch_size=64, show_progress_bar=False).tolist()
    log.info("Embedded in %.2fs", time.time() - t0)

    # Chroma upsert in chunks
    CHUNK = 500
    for i in range(0, len(docs), CHUNK):
        collection.upsert(
            ids=ids[i : i + CHUNK],
            documents=docs[i : i + CHUNK],
            metadatas=metas[i : i + CHUNK],
            embeddings=embeddings[i : i + CHUNK],
        )

    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO uploads (id, filename, sheet_name, row_count, columns_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                req.upload_id,
                filename,
                req.sheet_name,
                len(docs),
                json.dumps(req.columns),
                time.time(),
            ),
        )

    # Keep the raw file in data/ for reference but rename out of staging
    final_path = DATA_DIR / f"ingested_{req.upload_id}_{filename}"
    staging_path.rename(final_path)

    return {
        "ok": True,
        "indexed": len(docs),
        "skipped_missing_required": skipped,
        "total_in_index": collection.count(),
    }


# ---------------------------------------------------------------------------
# Retrieval + LLM recommendation
# ---------------------------------------------------------------------------
def build_query_text(a: AttributeQuery) -> str:
    parts = [f"Attribute: {a.name}"]
    if a.data_type:
        parts.append(f"Type: {a.data_type}")
    if a.description:
        parts.append(f"Description: {a.description}")
    if a.context:
        parts.append(f"Source Context: {a.context}")
    return " | ".join(parts)


def retrieve(query_text: str, top_k: int) -> list[dict]:
    if collection.count() == 0:
        return []
    emb = embedder.encode([query_text]).tolist()
    res = collection.query(query_embeddings=emb, n_results=top_k)
    out = []
    for i in range(len(res["ids"][0])):
        md = res["metadatas"][0][i] or {}
        dist = res["distances"][0][i] if res.get("distances") else None
        # cosine distance -> similarity
        sim = round(1 - dist, 4) if dist is not None else None
        out.append(
            {
                "source_attribute": md.get("source_attribute", ""),
                "oagis_path": md.get("oagis_path", ""),
                "data_type": md.get("data_type", ""),
                "description": md.get("description", ""),
                "notes": md.get("notes", ""),
                "context": md.get("context", ""),
                "source_file": md.get("source_file", ""),
                "similarity": sim,
            }
        )
    return out


SYSTEM_PROMPT = """You are an expert data modeler specializing in the OAGIS (Open Applications Group Integration Specification) standard. You help a data engineering team map attributes from Technical Data Packages and source systems to the correct location within the OAGIS data model (Nouns, BODs, components, and their nested elements like ItemMaster/Specification/Property, PurchaseOrder/PurchaseOrderLine, etc.).

You will be given:
1. A new attribute that needs to be mapped.
2. A set of RETRIEVED prior mappings from the team's internal database — these are the ground truth for how the team has historically mapped similar attributes. They should strongly inform your recommendation.

Your job:
- Propose 1-3 candidate OAGIS paths, ranked by confidence.
- Lean heavily on the retrieved examples — if a near-identical attribute is in the examples, reuse its mapping.
- If nothing close is retrieved, say so, and propose a path based on OAGIS conventions while flagging the uncertainty.
- Be specific about the full path (e.g. /ItemMaster/Specification/Property/Value, not just "ItemMaster").
- Always explain your reasoning, citing specific retrieved rows by their source attribute when relevant.

Return STRICT JSON with this schema and nothing else — no prose outside the JSON:

{
  "recommendations": [
    {
      "oagis_path": "string - full suggested OAGIS path",
      "confidence": "high | medium | low",
      "rationale": "string - 1-3 sentence explanation",
      "supporting_examples": ["string - source_attribute names from retrieved examples that back this mapping"]
    }
  ],
  "notes": "string - optional broader observations, e.g. ambiguity, missing context, suggested clarifying questions",
  "needs_human_review": true | false
}"""


def build_user_prompt(a: AttributeQuery, retrieved: list[dict]) -> str:
    ctx_lines = []
    for i, r in enumerate(retrieved, 1):
        ctx_lines.append(
            f"[{i}] similarity={r['similarity']}\n"
            f"    source_attribute: {r['source_attribute']}\n"
            f"    data_type: {r['data_type']}\n"
            f"    description: {r['description']}\n"
            f"    context: {r['context']}\n"
            f"    -> mapped to OAGIS path: {r['oagis_path']}\n"
            f"    notes: {r['notes']}\n"
            f"    from file: {r['source_file']}"
        )
    ctx_block = "\n\n".join(ctx_lines) if ctx_lines else "(no prior mappings retrieved — index is empty or no close matches)"

    return f"""NEW ATTRIBUTE TO MAP:
  name: {a.name}
  data_type: {a.data_type or "(not provided)"}
  description: {a.description or "(not provided)"}
  context: {a.context or "(not provided)"}

RETRIEVED PRIOR MAPPINGS (ranked by similarity, most similar first):

{ctx_block}

Recommend the best OAGIS path(s) for this attribute. Respond with the JSON schema only."""


def call_llm(user_prompt: str, model: str) -> dict:
    try:
        resp = anthropic_client.messages.create(
            model=model,
            max_tokens=1500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = resp.content[0].text.strip()
        # Claude may wrap in ```json ... ```
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            if text.endswith("```"):
                text = text.rsplit("```", 1)[0]
            text = text.strip()
            if text.startswith("json"):
                text = text[4:].strip()
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error("LLM returned non-JSON: %s", e)
        return {
            "recommendations": [],
            "notes": f"LLM response could not be parsed as JSON. Raw: {text[:400]}",
            "needs_human_review": True,
        }
    except Exception as e:
        log.exception("LLM call failed")
        raise HTTPException(500, f"LLM call failed: {e}")


@app.post("/api/map")
def map_attributes(req: MapRequest):
    if not req.attributes:
        raise HTTPException(400, "provide at least one attribute")
    model = req.model or DEFAULT_LLM_MODEL
    top_k = max(1, min(req.top_k, 20))

    results = []
    for a in req.attributes:
        qtext = build_query_text(a)
        retrieved = retrieve(qtext, top_k)
        user_prompt = build_user_prompt(a, retrieved)
        llm_out = call_llm(user_prompt, model)
        results.append(
            {
                "input": a.model_dump(),
                "retrieved": retrieved,
                "recommendation": llm_out,
            }
        )
    return {"results": results, "model": model, "indexed_mappings": collection.count()}


# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=5000, log_level="info")
