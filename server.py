"""
OAGIS Mapping Assistant — RAG over previous attribute mappings.

Backend: FastAPI
Vector store: ChromaDB (persistent, on-disk)
Embeddings: sentence-transformers (all-MiniLM-L6-v2, local)
LLM: OpenAI-compatible chat completions endpoint (configurable via base_url + LLM_API_KEY)
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
from dotenv import load_dotenv
from openai import OpenAI
from chromadb.config import Settings
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
load_dotenv()

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
TEAM_CONVENTIONS_PATH = BASE_DIR / "team_conventions.md"

# Pseudo-upload used to hold canonical OAGIS schema entries seeded from an XSD.
# Lives alongside user uploads in the same uploads table so it can be listed
# and deleted from the library UI like any other source.
CANONICAL_UPLOAD_ID = "oagis_canonical"
CANONICAL_SOURCE_FILE = "OAGIS canonical schema"

EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
COLLECTION_NAME = "oagis_mappings"
DEFAULT_LLM_MODEL = "claude-4-5-sonnet-latest"

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

openai_client = OpenAI(
    api_key=os.getenv("LLM_API_KEY"),
    base_url="https://api.ai.us.lmco.com/v1"
)


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
        # 'mapping' = a prior team mapping from an uploaded spreadsheet.
        # 'canonical' = a path seeded from the OAGIS XSD — a valid location
        # in the standard, not a precedent.
        "kind": "mapping",
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
    # Ad-hoc instructions injected into the system prompt for this request only.
    # Useful for one-off hints like "this batch is all from the PLM system" or
    # "prefer /ItemInstance paths when in doubt".
    extra_instructions: Optional[str] = ""
    # When true, retrieval is restricted to canonical OAGIS paths (seeded from
    # the XSD) and prior team mappings are hidden from the LLM entirely. Used
    # when the team wants a from-scratch recommendation grounded solely in the
    # schema — e.g. to sanity-check a prior mapping or to seed a fresh Noun.
    canonical_only: bool = False


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
    total = collection.count()
    try:
        canonical = collection.get(where={"kind": "canonical"}, include=[])
        canonical_count = len(canonical.get("ids", []))
    except Exception:
        canonical_count = 0
    return {
        "status": "ok",
        "mappings_indexed": max(total - canonical_count, 0),
        "canonical_indexed": canonical_count,
        "total_indexed": total,
        "embedding_model": EMBED_MODEL_NAME,
        "llm_model": DEFAULT_LLM_MODEL,
    }


@app.get("/api/uploads")
def list_uploads():
    with db() as conn:
        rows = conn.execute(
            "SELECT id, filename, sheet_name, row_count, created_at FROM uploads ORDER BY created_at DESC"
        ).fetchall()
    uploads = []
    canonical_count = 0
    for r in rows:
        d = dict(r)
        if d["id"] == CANONICAL_UPLOAD_ID:
            d["kind"] = "canonical"
            canonical_count = d["row_count"]
        else:
            d["kind"] = "mapping"
        uploads.append(d)
    return {
        "uploads": uploads,
        "total_indexed": collection.count(),
        "canonical_indexed": canonical_count,
    }


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

    # Dedupe by stable ID as we go. Chroma's upsert() will raise
    # DuplicateIDError if the same ID appears twice in a single call, and large
    # spreadsheets routinely have duplicate (source_attribute, oagis_path,
    # description) triples. We keep the LAST occurrence for each ID so the
    # most recent row in the sheet wins.
    seen: dict[str, int] = {}          # id -> index in the parallel lists below
    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict] = []
    skipped = 0
    duplicates = 0

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
        if h in seen:
            # Same logical mapping already seen in this batch — overwrite in
            # place with the latest row rather than crashing the upsert.
            idx = seen[h]
            docs[idx] = doc
            metas[idx] = meta
            duplicates += 1
            continue
        seen[h] = len(ids)
        ids.append(h)
        docs.append(doc)
        metas.append(meta)

    if not docs:
        raise HTTPException(400, "no valid rows found with both a source attribute and OAGIS path")

    if duplicates:
        log.info(
            "Collapsed %d duplicate rows (same source_attribute+oagis_path+description) during ingest",
            duplicates,
        )

    # Batch-embed for speed
    log.info("Embedding %d rows...", len(docs))
    t0 = time.time()
    embeddings = embedder.encode(docs, batch_size=64, show_progress_bar=False).tolist()
    log.info("Embedded in %.2fs", time.time() - t0)

    # Chroma upsert in chunks. Wrap each chunk so one bad chunk doesn't abort
    # the whole ingest — we fall back to per-row upsert and log the offenders.
    CHUNK = 500
    failed_rows = 0
    for i in range(0, len(docs), CHUNK):
        chunk_ids = ids[i : i + CHUNK]
        chunk_docs = docs[i : i + CHUNK]
        chunk_metas = metas[i : i + CHUNK]
        chunk_embs = embeddings[i : i + CHUNK]
        try:
            collection.upsert(
                ids=chunk_ids,
                documents=chunk_docs,
                metadatas=chunk_metas,
                embeddings=chunk_embs,
            )
        except Exception as chunk_err:
            log.warning(
                "Chunk upsert failed (%s) — retrying row-by-row for rows %d..%d",
                chunk_err,
                i,
                i + len(chunk_ids) - 1,
            )
            for j in range(len(chunk_ids)):
                try:
                    collection.upsert(
                        ids=[chunk_ids[j]],
                        documents=[chunk_docs[j]],
                        metadatas=[chunk_metas[j]],
                        embeddings=[chunk_embs[j]],
                    )
                except Exception as row_err:
                    failed_rows += 1
                    log.error(
                        "Skipping row %d (id=%s, source_attribute=%r): %s",
                        i + j,
                        chunk_ids[j],
                        chunk_metas[j].get("source_attribute"),
                        row_err,
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

    indexed = len(docs) - failed_rows
    return {
        "ok": True,
        "indexed": indexed,
        "skipped_missing_required": skipped,
        "collapsed_duplicates": duplicates,
        "failed_rows": failed_rows,
        "total_in_index": collection.count(),
    }


# ---------------------------------------------------------------------------
# Batch-map spreadsheet parsing
#
# The Batch Map UI historically required users to type `name | data_type |
# description | context` lines by hand. This endpoint accepts an uploaded
# spreadsheet (xlsx / xlsm / xls / csv) and returns the attribute rows already
# parsed into the shape /api/map expects. Column roles are auto-detected using
# the same DETECTION_HINTS used for the library upload flow; the caller can
# override via the `columns` form field (JSON: {role: column_name}).
#
# Nothing is written to the vector store — this is a pure read that turns a
# spreadsheet into JSON so the browser can POST it to /api/map.
# ---------------------------------------------------------------------------
BATCH_ROLE_TO_FIELD = {
    "source_attribute": "name",
    "data_type": "data_type",
    "description": "description",
    "context": "context",
}


@app.post("/api/batch/parse")
async def batch_parse(
    file: UploadFile = File(...),
    sheet: Optional[str] = Form(None),
    columns: Optional[str] = Form(None),
    max_rows: Optional[int] = Form(None),
):
    """Parse a spreadsheet into a list of batch-map attribute rows.

    Returns the full set of sheets + columns so the UI can offer a picker
    (mirrors /api/upload/preview), and also returns the parsed attribute rows
    ready to feed straight into /api/map.
    """
    if not file.filename:
        raise HTTPException(400, "no filename")
    raw = await file.read()
    if not raw:
        raise HTTPException(400, "empty file")

    suffix = Path(file.filename).suffix.lower()
    buf = io.BytesIO(raw)
    try:
        if suffix in (".xlsx", ".xlsm", ".xls"):
            xls = pd.ExcelFile(buf, engine="openpyxl")
            sheets = xls.sheet_names
            active_sheet = sheet if sheet and sheet in sheets else sheets[0]
            df = pd.read_excel(buf, sheet_name=active_sheet, engine="openpyxl")
        elif suffix == ".csv":
            sheets = ["(csv)"]
            active_sheet = "(csv)"
            df = pd.read_csv(io.BytesIO(raw))
        else:
            raise HTTPException(400, f"unsupported file type: {suffix or '(none)'}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"could not parse file: {e}")

    df = df.fillna("")
    all_columns = [str(c) for c in df.columns]
    detected = detect_columns(all_columns)

    # Caller override (JSON blob in a form field, since this is multipart)
    chosen: dict[str, str] = {}
    if columns:
        try:
            override = json.loads(columns)
            if isinstance(override, dict):
                for role, col in override.items():
                    if isinstance(col, str) and col and col in all_columns:
                        chosen[role] = col
        except json.JSONDecodeError:
            raise HTTPException(400, "`columns` must be a JSON object")

    # Fill in anything the caller didn't override
    for role, col in detected.items():
        chosen.setdefault(role, col)

    name_col = chosen.get("source_attribute")
    if not name_col:
        return {
            "ok": False,
            "error": "Could not find an attribute-name column. Pick one explicitly.",
            "sheets": sheets,
            "active_sheet": active_sheet,
            "columns": all_columns,
            "detected": detected,
            "attributes": [],
            "skipped_empty": 0,
            "total_rows": int(len(df)),
        }

    # Build attribute rows
    attrs: list[dict] = []
    skipped_empty = 0
    # Cap defensively — Batch Map hits the LLM once per row
    hard_cap = max(1, min(int(max_rows or 2000), 5000))

    for _, r in df.iterrows():
        def pick(role: str) -> str:
            col = chosen.get(role)
            if not col:
                return ""
            v = r.get(col, "")
            return "" if v is None else str(v).strip()

        name = pick("source_attribute")
        if not name:
            skipped_empty += 1
            continue
        attrs.append(
            {
                "name": name,
                "data_type": pick("data_type"),
                "description": pick("description"),
                "context": pick("context"),
            }
        )
        if len(attrs) >= hard_cap:
            break

    return {
        "ok": True,
        "sheets": sheets,
        "active_sheet": active_sheet,
        "columns": all_columns,
        "detected": detected,
        "resolved": chosen,
        "attributes": attrs,
        "skipped_empty": skipped_empty,
        "total_rows": int(len(df)),
        "truncated": len(attrs) >= hard_cap and int(len(df)) > hard_cap,
    }


# ---------------------------------------------------------------------------
# Canonical OAGIS schema seeding
# ---------------------------------------------------------------------------
class CanonicalRow(BaseModel):
    oagis_path: str
    source_attribute: Optional[str] = ""
    description: Optional[str] = ""
    data_type: Optional[str] = ""
    context: Optional[str] = ""
    notes: Optional[str] = ""


class SeedCanonicalRequest(BaseModel):
    rows: list[CanonicalRow]
    # If true (default), wipe any existing canonical entries before seeding so
    # re-running the seeder doesn't leave stale paths in the index.
    replace_existing: bool = True


@app.post("/api/seed/canonical")
def seed_canonical(req: SeedCanonicalRequest):
    """Bulk-load canonical OAGIS paths (e.g. from scripts/seed_oagis_xsd.py).

    Paths are upserted into the same Chroma collection as user mappings but
    tagged with kind='canonical' so retrieval/prompting can distinguish them.
    They share a single pseudo-upload (CANONICAL_UPLOAD_ID) so the library UI
    can show/delete them as a unit.
    """
    if not req.rows:
        raise HTTPException(400, "provide at least one canonical row")

    # Optional clean slate so re-running the seeder replaces rather than piles on.
    if req.replace_existing:
        try:
            collection.delete(where={"upload_id": CANONICAL_UPLOAD_ID})
        except Exception as e:  # pragma: no cover - non-fatal
            log.warning("Could not clear prior canonical entries: %s", e)
        with db() as conn:
            conn.execute("DELETE FROM uploads WHERE id = ?", (CANONICAL_UPLOAD_ID,))

    seen: dict[str, int] = {}
    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict] = []
    duplicates = 0

    for r in req.rows:
        path = (r.oagis_path or "").strip()
        if not path:
            continue
        el_name = (r.source_attribute or path.rsplit("/", 1)[-1] or "").strip()
        dtype = (r.data_type or "").strip()
        desc = (r.description or "").strip()
        context = (r.context or "OAGIS canonical schema").strip()
        notes = (r.notes or "Canonical OAGIS schema entry (not a prior mapping).").strip()

        parts = [f"OAGIS Path: {path}"]
        if el_name:
            parts.append(f"Element: {el_name}")
        if dtype:
            parts.append(f"XSD Type: {dtype}")
        if desc:
            parts.append(f"Documentation: {desc}")
        doc = " | ".join(parts)

        meta = {
            "source_attribute": el_name,
            "oagis_path": path,
            "data_type": dtype,
            "description": desc,
            "notes": notes,
            "context": context,
            "kind": "canonical",
            "upload_id": CANONICAL_UPLOAD_ID,
            "source_file": CANONICAL_SOURCE_FILE,
        }

        # Canonical IDs are deterministic on path only — each path is unique.
        h = "c" + hashlib.sha1(path.encode()).hexdigest()[:15]
        if h in seen:
            idx = seen[h]
            docs[idx] = doc
            metas[idx] = meta
            duplicates += 1
            continue
        seen[h] = len(ids)
        ids.append(h)
        docs.append(doc)
        metas.append(meta)

    if not docs:
        raise HTTPException(400, "no valid canonical rows (every row had an empty oagis_path)")

    log.info("Embedding %d canonical OAGIS paths...", len(docs))
    t0 = time.time()
    embeddings = embedder.encode(docs, batch_size=64, show_progress_bar=False).tolist()
    log.info("Embedded canonical schema in %.2fs", time.time() - t0)

    CHUNK = 500
    failed_rows = 0
    for i in range(0, len(docs), CHUNK):
        try:
            collection.upsert(
                ids=ids[i : i + CHUNK],
                documents=docs[i : i + CHUNK],
                metadatas=metas[i : i + CHUNK],
                embeddings=embeddings[i : i + CHUNK],
            )
        except Exception as chunk_err:
            log.warning("Canonical chunk failed (%s) — retrying row-by-row", chunk_err)
            for j in range(len(ids[i : i + CHUNK])):
                k = i + j
                try:
                    collection.upsert(
                        ids=[ids[k]],
                        documents=[docs[k]],
                        metadatas=[metas[k]],
                        embeddings=[embeddings[k]],
                    )
                except Exception as row_err:
                    failed_rows += 1
                    log.error("Skipping canonical path %s: %s", metas[k]["oagis_path"], row_err)

    indexed = len(docs) - failed_rows
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO uploads (id, filename, sheet_name, row_count, columns_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                CANONICAL_UPLOAD_ID,
                CANONICAL_SOURCE_FILE,
                None,
                indexed,
                json.dumps({"kind": "canonical"}),
                time.time(),
            ),
        )

    return {
        "ok": True,
        "indexed": indexed,
        "collapsed_duplicates": duplicates,
        "failed_rows": failed_rows,
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


def retrieve(
    query_text: str,
    top_k: int,
    canonical_only: bool = False,
) -> list[dict]:
    if collection.count() == 0:
        return []
    emb = embedder.encode([query_text]).tolist()
    query_kwargs: dict = {"query_embeddings": emb, "n_results": top_k}
    if canonical_only:
        # Restrict ANN search to rows tagged kind="canonical" (seeded from the
        # OAGIS XSD). If no canonical rows exist, return [] so the caller can
        # surface a helpful message instead of silently falling back.
        try:
            any_canonical = collection.get(
                where={"kind": "canonical"}, limit=1, include=[]
            )
            if not any_canonical.get("ids"):
                return []
        except Exception as e:  # pragma: no cover - defensive
            log.warning("canonical existence check failed: %s", e)
            return []
        query_kwargs["where"] = {"kind": "canonical"}
    res = collection.query(**query_kwargs)
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
                "kind": md.get("kind", "mapping"),
                "similarity": sim,
            }
        )
    return out


BASE_SYSTEM_PROMPT = """You are an expert data modeler specializing in the OAGIS (Open Applications Group Integration Specification) standard. You help a data engineering team map attributes from Technical Data Packages and source systems to the correct location within the OAGIS data model (Nouns, BODs, components, and their nested elements like ItemMaster/Specification/Property, PurchaseOrder/PurchaseOrderLine, etc.).

You will be given:
1. A new attribute that needs to be mapped.
2. RETRIEVED PRIOR MAPPINGS from the team's internal database — the ground truth for how the team has historically mapped similar attributes. These are your strongest signal; a near-identical prior mapping should almost always win. (This section may be intentionally omitted — see below.)
3. RETRIEVED CANONICAL OAGIS PATHS seeded from the OAGIS XSD — valid locations in the schema itself. Use these to verify that a path actually exists, or to propose one when no prior mapping is close. A canonical path alone is NOT a precedent — it just proves the location is legal. Prefer prior mappings over canonical paths when they conflict, unless the canonical data makes it obvious the prior mapping is wrong.
4. (Optional) TEAM CONVENTIONS — house rules on path notation, noun selection, and preferred extension patterns. Follow these strictly; they override generic OAGIS defaults.

If the user prompt contains a "CANONICAL-ONLY MODE" banner, the prior-mappings section has been deliberately hidden. In that mode you must NOT invent or reference prior mappings — base every recommendation strictly on the retrieved canonical OAGIS paths and general OAGIS conventions, and leave `supporting_examples` empty.

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


def load_system_prompt() -> str:
    """Build the system prompt from the base template plus any team conventions.

    Re-reads team_conventions.md on every call so edits don't require a restart.
    """
    prompt = BASE_SYSTEM_PROMPT
    try:
        if TEAM_CONVENTIONS_PATH.exists():
            conventions = TEAM_CONVENTIONS_PATH.read_text(encoding="utf-8").strip()
            if conventions:
                prompt += (
                    "\n\n---\nTEAM CONVENTIONS (project-wide house rules — follow these strictly):\n\n"
                    + conventions
                )
    except Exception as e:  # pragma: no cover - non-fatal
        log.warning("Could not load team conventions: %s", e)
    return prompt


def build_user_prompt(
    a: AttributeQuery,
    retrieved: list[dict],
    canonical_only: bool = False,
) -> str:
    mappings = [r for r in retrieved if r.get("kind") != "canonical"]
    canonical = [r for r in retrieved if r.get("kind") == "canonical"]

    def fmt_mapping(i: int, r: dict) -> str:
        return (
            f"[{i}] similarity={r['similarity']}\n"
            f"    source_attribute: {r['source_attribute']}\n"
            f"    data_type: {r['data_type']}\n"
            f"    description: {r['description']}\n"
            f"    context: {r['context']}\n"
            f"    -> mapped to OAGIS path: {r['oagis_path']}\n"
            f"    notes: {r['notes']}\n"
            f"    from file: {r['source_file']}"
        )

    def fmt_canonical(i: int, r: dict) -> str:
        return (
            f"[C{i}] similarity={r['similarity']}\n"
            f"    oagis_path: {r['oagis_path']}\n"
            f"    element: {r['source_attribute']}\n"
            f"    xsd_type: {r['data_type']}\n"
            f"    documentation: {r['description']}"
        )

    header = f"""NEW ATTRIBUTE TO MAP:
  name: {a.name}
  data_type: {a.data_type or "(not provided)"}
  description: {a.description or "(not provided)"}
  context: {a.context or "(not provided)"}"""

    if canonical_only:
        canonical_block = (
            "\n\n".join(fmt_canonical(i, r) for i, r in enumerate(canonical, 1))
            if canonical
            else "(no canonical OAGIS paths retrieved — the schema index is empty or has no close match; recommend based on OAGIS conventions and flag low confidence)"
        )
        return f"""{header}

CANONICAL-ONLY MODE: the team has asked you to evaluate this attribute strictly against the OAGIS XSD. Prior team mappings have been deliberately hidden for this request. Do NOT reference or invent prior mappings. Base your recommendation solely on the canonical paths below and general OAGIS conventions. Leave `supporting_examples` empty.

RETRIEVED CANONICAL OAGIS PATHS (valid locations in the schema — the only retrieval signal for this request):

{canonical_block}

Recommend the best OAGIS path(s) for this attribute. Respond with the JSON schema only."""

    mapping_block = (
        "\n\n".join(fmt_mapping(i, r) for i, r in enumerate(mappings, 1))
        if mappings
        else "(no prior mappings retrieved — either the index has none yet or none are close to this attribute)"
    )
    canonical_block = (
        "\n\n".join(fmt_canonical(i, r) for i, r in enumerate(canonical, 1))
        if canonical
        else "(no canonical OAGIS paths retrieved — the schema index is empty or has no close match)"
    )

    return f"""{header}

RETRIEVED PRIOR MAPPINGS (team's historical mappings — precedent, strongest signal):

{mapping_block}

RETRIEVED CANONICAL OAGIS PATHS (valid locations in the schema — use to confirm a path exists, or to propose one when there's no close prior mapping):

{canonical_block}

Recommend the best OAGIS path(s) for this attribute. Respond with the JSON schema only."""


def call_llm(user_prompt: str, model: str, extra_instructions: str = "") -> dict:
    system_prompt = load_system_prompt()
    if extra_instructions and extra_instructions.strip():
        system_prompt += (
            "\n\n---\nADDITIONAL CONTEXT FOR THIS REQUEST (user-supplied, applies only to this call):\n\n"
            + extra_instructions.strip()
        )
    try:
        resp = openai_client.chat.completions.create(
            model=model,
            max_tokens=1500,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
        )
        text = resp.choices[0].message.content.strip()
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
    canonical_only = bool(req.canonical_only)

    results = []
    for a in req.attributes:
        qtext = build_query_text(a)
        retrieved = retrieve(qtext, top_k, canonical_only=canonical_only)
        user_prompt = build_user_prompt(a, retrieved, canonical_only=canonical_only)
        llm_out = call_llm(user_prompt, model, extra_instructions=req.extra_instructions or "")
        results.append(
            {
                "input": a.model_dump(),
                "retrieved": retrieved,
                "recommendation": llm_out,
            }
        )
    return {
        "results": results,
        "model": model,
        "indexed_mappings": collection.count(),
        "canonical_only": canonical_only,
    }


# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=5000, log_level="info")
