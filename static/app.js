// ============================================================
// OAGIS Mapping Assistant — frontend
// ============================================================

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

// When deployed, __PORT_5000__ is replaced with the proxy path to the backend.
// Locally (dev), it stays as __PORT_5000__ — so we fall back to same-origin.
const __API_BASE = '__PORT_5000__';
const API_BASE = __API_BASE.startsWith('__') ? '' : __API_BASE;
const api = (path) => API_BASE + path;

const ROLES = [
  { key: 'source_attribute', label: 'Source attribute *', required: true },
  { key: 'oagis_path', label: 'OAGIS path *', required: true },
  { key: 'data_type', label: 'Data type', required: false },
  { key: 'description', label: 'Description', required: false },
  { key: 'notes', label: 'Notes / rationale', required: false },
  { key: 'context', label: 'Context / source system', required: false },
];

let currentPreview = null; // preview response
let lastBatchResults = null;
let currentBatchParse = null; // parsed spreadsheet for the Batch Map tab

// Roles we surface for the Batch Map column picker. Narrower than the
// Library's ROLES because Batch Map doesn't need oagis_path / notes.
const BATCH_ROLES = [
  { key: 'source_attribute', label: 'Attribute name *', required: true },
  { key: 'data_type', label: 'Data type', required: false },
  { key: 'description', label: 'Description', required: false },
  { key: 'context', label: 'Context / source system', required: false },
];

// ============================================================
// Tabs
// ============================================================
$$('.tab').forEach((t) =>
  t.addEventListener('click', () => {
    $$('.tab').forEach((x) => x.classList.toggle('active', x === t));
    const target = t.dataset.tab;
    $$('.panel').forEach((p) => p.classList.toggle('hidden', p.id !== `tab-${target}`));
    if (target === 'library') refreshUploads();
  })
);

// ============================================================
// Toast
// ============================================================
function toast(msg, isError = false) {
  const el = $('#toast');
  el.textContent = msg;
  el.classList.toggle('err', isError);
  el.classList.add('show');
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.remove('show'), 3500);
}

// ============================================================
// Health / status
// ============================================================
async function refreshHealth() {
  try {
    const r = await fetch(api('/api/health'));
    const j = await r.json();
    $('#status-pill').classList.add('ok');
    $('#status-pill').classList.remove('err');
    const mappings = (j.mappings_indexed ?? 0).toLocaleString();
    const canonical = j.canonical_indexed ?? 0;
    $('#status-text').textContent = canonical > 0
      ? `${mappings} mappings · ${canonical.toLocaleString()} canonical paths`
      : `${mappings} mappings indexed`;
    updateCanonicalOnlyAvailability(canonical);
  } catch (e) {
    $('#status-pill').classList.add('err');
    $('#status-text').textContent = 'server unreachable';
  }
}

// Enable/disable the canonical-only toggles based on whether any canonical
// paths have been seeded into the index. With none, the toggle has no effect
// and we don't want to let users pick it.
function updateCanonicalOnlyAvailability(canonicalCount) {
  const available = (canonicalCount || 0) > 0;
  [['q-canonical-only', 'q-canonical-only-hint'], ['batch-canonical-only', 'batch-canonical-only-hint']]
    .forEach(([inputId, hintId]) => {
      const input = document.getElementById(inputId);
      const hint = document.getElementById(hintId);
      if (!input) return;
      input.disabled = !available;
      if (!available) {
        input.checked = false;
        if (hint) hint.textContent = 'No canonical OAGIS paths seeded yet — run scripts/seed_oagis_xsd.py to enable.';
      } else if (hint) {
        hint.textContent = 'Ignore prior team mappings — evaluate solely against the seeded OAGIS XSD.';
      }
    });
}

// ============================================================
// Upload flow
// ============================================================
$('#file-input').addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append('file', file);
  toast('Parsing file…');
  try {
    const r = await fetch(api('/api/upload/preview'), { method: 'POST', body: fd });
    if (!r.ok) throw new Error((await r.json()).detail || r.statusText);
    currentPreview = await r.json();
    renderPreview();
    toast('Ready — confirm column mapping and index.');
  } catch (err) {
    toast('Upload failed: ' + err.message, true);
  }
});

function renderPreview() {
  if (!currentPreview) return;
  $('#preview-card').classList.remove('hidden');

  const sel = $('#sheet-select');
  sel.innerHTML = currentPreview.sheets
    .map((s) => `<option ${s === currentPreview.active_sheet ? 'selected' : ''}>${escapeHtml(s)}</option>`)
    .join('');
  sel.onchange = () => reloadSheet(sel.value);

  // Role -> column selects
  const roles = $('#col-roles');
  roles.innerHTML = ROLES.map((r) => {
    const options = [`<option value="">— none —</option>`]
      .concat(
        currentPreview.columns.map(
          (c) => `<option value="${escapeAttr(c)}" ${currentPreview.detected[r.key] === c ? 'selected' : ''}>${escapeHtml(c)}</option>`
        )
      )
      .join('');
    return `<label>${r.label}<select data-role="${r.key}">${options}</select></label>`;
  }).join('');

  // Preview table
  const cols = currentPreview.columns;
  const rows = currentPreview.preview_rows;
  const thead = `<thead><tr>${cols.map((c) => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${rows
    .map((row) => `<tr>${cols.map((c) => `<td>${escapeHtml(row[c] ?? '')}</td>`).join('')}</tr>`)
    .join('')}</tbody>`;
  $('#preview-table').innerHTML = thead + tbody;
}

async function reloadSheet(sheet) {
  // Re-parse so preview rows update for the new sheet
  const fileInput = $('#file-input');
  if (!fileInput.files[0]) return;
  const fd = new FormData();
  fd.append('file', fileInput.files[0]);
  fd.append('sheet', sheet);
  const r = await fetch(api('/api/upload/preview'), { method: 'POST', body: fd });
  if (r.ok) {
    currentPreview = await r.json();
    renderPreview();
  }
}

$('#btn-commit').addEventListener('click', async () => {
  if (!currentPreview) return;
  const columns = {};
  $$('#col-roles select').forEach((s) => {
    if (s.value) columns[s.dataset.role] = s.value;
  });
  if (!columns.source_attribute || !columns.oagis_path) {
    toast('Source attribute and OAGIS path column mappings are required.', true);
    return;
  }
  const btn = $('#btn-commit');
  btn.disabled = true;
  btn.textContent = 'Indexing…';
  try {
    const r = await fetch(api('/api/upload/commit'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        upload_id: currentPreview.upload_id,
        sheet_name: $('#sheet-select').value,
        columns,
        replace_existing: $('#replace-existing').checked,
      }),
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.detail || r.statusText);
    const bits = [`Indexed ${j.indexed.toLocaleString()} mappings`];
    if (j.skipped_missing_required) bits.push(`skipped ${j.skipped_missing_required} missing required fields`);
    if (j.collapsed_duplicates) bits.push(`collapsed ${j.collapsed_duplicates} duplicate rows`);
    if (j.failed_rows) bits.push(`${j.failed_rows} rows failed (see server log)`);
    bits.push(`Total in index: ${j.total_in_index.toLocaleString()}`);
    toast(bits.join(' — ') + '.');
    $('#preview-card').classList.add('hidden');
    $('#file-input').value = '';
    currentPreview = null;
    await refreshHealth();
    await refreshUploads();
  } catch (err) {
    toast('Commit failed: ' + err.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Index this mapping';
  }
});

async function refreshUploads() {
  const r = await fetch(api('/api/uploads'));
  const j = await r.json();
  const el = $('#uploads-list');
  if (!j.uploads.length) {
    el.innerHTML = '<div class="muted">(nothing indexed yet)</div>';
    return;
  }
  el.innerHTML = j.uploads
    .map((u) => {
      const date = new Date(u.created_at * 1000).toLocaleString();
      return `
      <div class="upload-row">
        <div>
          <div class="name">${escapeHtml(u.filename)}</div>
          <div class="sub">${escapeHtml(u.sheet_name || '')} · ${u.row_count.toLocaleString()} rows · ${date}</div>
        </div>
        <button class="danger" data-del="${u.id}">Remove</button>
      </div>`;
    })
    .join('');
  $$('[data-del]').forEach((b) =>
    b.addEventListener('click', async () => {
      if (!confirm('Remove this indexed file and all its mappings?')) return;
      const res = await fetch(api('/api/uploads/' + b.dataset.del), { method: 'DELETE' });
      if (res.ok) {
        toast('Removed.');
        await refreshHealth();
        await refreshUploads();
      } else {
        toast('Delete failed.', true);
      }
    })
  );
}

// ============================================================
// Single-attribute mapping
// ============================================================
$('#btn-map').addEventListener('click', async () => {
  const name = $('#q-name').value.trim();
  if (!name) {
    toast('Attribute name is required.', true);
    return;
  }
  const canonicalOnly = !!($('#q-canonical-only')?.checked);
  const payload = {
    attributes: [
      {
        name,
        data_type: $('#q-type').value.trim(),
        description: $('#q-desc').value.trim(),
        context: $('#q-context').value.trim(),
      },
    ],
    top_k: parseInt($('#q-k').value, 10) || 6,
    extra_instructions: $('#q-extra').value.trim(),
    canonical_only: canonicalOnly,
  };
  $('#result-empty').classList.add('hidden');
  $('#result-body').classList.add('hidden');
  $('#result-loading').classList.remove('hidden');

  try {
    const r = await fetch(api('/api/map'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.detail || r.statusText);
    renderSingleResult(j.results[0], { canonicalOnly: !!j.canonical_only });
  } catch (err) {
    $('#result-body').classList.remove('hidden');
    $('#result-body').innerHTML = `<div class="muted">Error: ${escapeHtml(err.message)}</div>`;
  } finally {
    $('#result-loading').classList.add('hidden');
  }
});

function renderSingleResult(result, opts = {}) {
  const { recommendation, retrieved } = result;
  const canonicalOnly = !!opts.canonicalOnly;
  const recs = (recommendation.recommendations || []).map(
    (r, i) => `
      <div class="rec">
        <div class="rec-head">
          <div class="rec-path">${escapeHtml(r.oagis_path || '—')}</div>
          <span class="conf ${String(r.confidence || 'low').toLowerCase()}">${escapeHtml(r.confidence || 'low')}</span>
        </div>
        <div class="rec-rationale">${escapeHtml(r.rationale || '')}</div>
        ${
          (r.supporting_examples || []).length
            ? `<div class="rec-support">Supported by: ${r.supporting_examples.map((s) => `<b>${escapeHtml(s)}</b>`).join(', ')}</div>`
            : ''
        }
      </div>`
  );

  const reviewFlag = recommendation.needs_human_review
    ? `<div class="review-flag">⚠ Flagged for human review — ${escapeHtml(recommendation.notes || '')}</div>`
    : recommendation.notes
    ? `<div class="muted small">${escapeHtml(recommendation.notes)}</div>`
    : '';

  const retrievedHtml = retrieved.length
    ? retrieved
        .map((r) => {
          const isCanonical = r.kind === 'canonical';
          const badge = isCanonical
            ? '<span class="kind-badge canonical">CANONICAL</span>'
            : '';
          return `
      <div class="ret-item${isCanonical ? ' canonical' : ''}">
        <div class="head">
          <div class="src">${badge}${escapeHtml(r.source_attribute)}</div>
          <div>sim ${r.similarity ?? '—'}</div>
        </div>
        <div class="path">${escapeHtml(r.oagis_path)}</div>
        <div class="meta">${r.data_type ? escapeHtml(r.data_type) + ' · ' : ''}${escapeHtml(r.description || '')}</div>
        ${r.notes && !isCanonical ? `<div class="meta"><i>Notes:</i> ${escapeHtml(r.notes)}</div>` : ''}
        <div class="meta muted">from ${escapeHtml(r.source_file || '')}</div>
      </div>`;
        })
        .join('')
    : (canonicalOnly
        ? '<div class="muted small">No canonical OAGIS paths retrieved for this attribute — the XSD index may be empty or none are close enough.</div>'
        : '<div class="muted small">No prior mappings retrieved (index may be empty).</div>');

  const retrievedHeader = canonicalOnly
    ? 'Retrieved canonical OAGIS paths <span class="muted small">(prior mappings ignored)</span>'
    : 'Retrieved prior mappings';
  const modeBanner = canonicalOnly
    ? '<div class="mode-banner">Canonical-only mode — prior team mappings were hidden from the LLM for this request.</div>'
    : '';

  $('#result-body').innerHTML = `
    ${modeBanner}
    <div class="rec-list">${recs.join('') || '<div class="muted">No recommendations returned.</div>'}</div>
    ${reviewFlag}
    <div class="retrieved">
      <h3>${retrievedHeader}</h3>
      ${retrievedHtml}
    </div>
  `;
  $('#result-body').classList.remove('hidden');
}

// ============================================================
// Batch mapping
// ============================================================

// -- Spreadsheet upload path (primary) --------------------------------------
$('#batch-file').addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  await parseBatchFile({}); // auto-detect columns on first parse
});

async function parseBatchFile({ sheet, columnsOverride }) {
  const fileInput = $('#batch-file');
  const file = fileInput.files[0];
  if (!file) return;

  const fd = new FormData();
  fd.append('file', file);
  if (sheet) fd.append('sheet', sheet);
  if (columnsOverride) fd.append('columns', JSON.stringify(columnsOverride));

  toast('Parsing spreadsheet…');
  try {
    const r = await fetch(api('/api/batch/parse'), { method: 'POST', body: fd });
    const j = await r.json();
    if (!r.ok) throw new Error(j.detail || r.statusText);
    currentBatchParse = j;
    currentBatchParse._file = file;
    renderBatchParse();
    if (j.ok) {
      toast(`Parsed ${j.attributes.length} attribute${j.attributes.length === 1 ? '' : 's'}.`);
    } else {
      toast(j.error || 'Parsed, but no attribute column found — pick one below.', true);
    }
  } catch (err) {
    toast('Parse failed: ' + err.message, true);
  }
}

function renderBatchParse() {
  const p = currentBatchParse;
  if (!p) return;
  $('#batch-preview').classList.remove('hidden');

  // File meta
  const f = p._file;
  $('#batch-file-meta').textContent = f ? `${f.name} · ${(f.size / 1024).toFixed(1)} KB` : '';

  // Sheet picker
  const sheetSel = $('#batch-sheet');
  sheetSel.innerHTML = (p.sheets || [])
    .map((s) => `<option ${s === p.active_sheet ? 'selected' : ''}>${escapeHtml(s)}</option>`)
    .join('');
  sheetSel.onchange = () => parseBatchFile({ sheet: sheetSel.value });

  // Column-role picker (only attribute-side roles)
  const rolesEl = $('#batch-col-roles');
  rolesEl.innerHTML = BATCH_ROLES.map((role) => {
    const current = (p.resolved && p.resolved[role.key]) || p.detected?.[role.key] || '';
    const opts = [`<option value="">— none —</option>`]
      .concat(
        (p.columns || []).map(
          (c) => `<option value="${escapeAttr(c)}" ${c === current ? 'selected' : ''}>${escapeHtml(c)}</option>`
        )
      )
      .join('');
    return `<label>${role.label}<select data-batch-role="${role.key}">${opts}</select></label>`;
  }).join('');
  $$('#batch-col-roles select').forEach((s) => {
    s.addEventListener('change', onBatchColumnsChanged);
  });

  // Summary line
  const bits = [];
  bits.push(`${p.attributes?.length || 0} attribute${(p.attributes?.length || 0) === 1 ? '' : 's'} ready`);
  if (p.skipped_empty) bits.push(`${p.skipped_empty} rows skipped (blank name)`);
  if (p.total_rows != null) bits.push(`${p.total_rows} total rows in sheet`);
  if (p.truncated) bits.push('truncated to 2000 rows — split your file if you need more');
  $('#batch-parse-summary').textContent = bits.join(' · ');

  // Preview of first 8 parsed attribute rows
  const tbl = $('#batch-preview-table');
  const rows = (p.attributes || []).slice(0, 8);
  if (!rows.length) {
    tbl.innerHTML = '<tbody><tr><td class="muted">No rows parsed yet — pick the attribute-name column above.</td></tr></tbody>';
  } else {
    const head = `<thead><tr><th>Attribute name</th><th>Data type</th><th>Description</th><th>Context</th></tr></thead>`;
    const body = `<tbody>${rows
      .map(
        (a) =>
          `<tr><td>${escapeHtml(a.name)}</td><td>${escapeHtml(a.data_type)}</td><td>${escapeHtml(a.description)}</td><td>${escapeHtml(a.context)}</td></tr>`
      )
      .join('')}</tbody>`;
    tbl.innerHTML = head + body;
  }
}

function onBatchColumnsChanged() {
  const override = {};
  $$('#batch-col-roles select').forEach((s) => {
    if (s.value) override[s.dataset.batchRole] = s.value;
  });
  parseBatchFile({ sheet: $('#batch-sheet').value || undefined, columnsOverride: override });
}

// -- Parse pipe-delimited text (fallback) -----------------------------------
function parsePipeText(raw) {
  return raw
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      const [name, data_type = '', description = '', context = ''] = l.split('|').map((s) => s.trim());
      return { name, data_type, description, context };
    });
}

// -- Run Batch --------------------------------------------------------------
$('#btn-batch').addEventListener('click', async () => {
  // Prefer the parsed spreadsheet; fall back to the pipe-text textarea.
  let attrs = [];
  if (currentBatchParse && currentBatchParse.ok && (currentBatchParse.attributes || []).length) {
    attrs = currentBatchParse.attributes;
  } else {
    const raw = ($('#batch-input').value || '').trim();
    if (raw) {
      attrs = parsePipeText(raw);
    }
  }

  if (!attrs.length) {
    toast('Upload a spreadsheet or paste at least one attribute.', true);
    return;
  }

  const btn = $('#btn-batch');
  btn.disabled = true;
  btn.textContent = `Running ${attrs.length}…`;
  $('#batch-result-card').classList.remove('hidden');
  $('#batch-results').innerHTML = '<div class="spinner"></div><div class="muted">Processing in parallel with Claude…</div>';
  $('#batch-summary').textContent = '';

  try {
    const r = await fetch(api('/api/map'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        attributes: attrs,
        top_k: parseInt($('#batch-k').value, 10) || 5,
        extra_instructions: $('#batch-extra').value.trim(),
        canonical_only: !!($('#batch-canonical-only')?.checked),
      }),
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.detail || r.statusText);
    lastBatchResults = j.results;
    renderBatch(j.results, { canonicalOnly: !!j.canonical_only });
  } catch (err) {
    $('#batch-results').innerHTML = `<div class="muted">Error: ${escapeHtml(err.message)}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = 'Run batch';
  }
});

function renderBatch(results, opts = {}) {
  const flagged = results.filter((r) => r.recommendation?.needs_human_review).length;
  const modeSuffix = opts.canonicalOnly ? ' · canonical-only mode (prior mappings ignored)' : '';
  $('#batch-summary').textContent = `${results.length} processed · ${flagged} flagged for human review${modeSuffix}`;

  $('#batch-results').innerHTML = results
    .map((r, idx) => {
      const top = r.recommendation?.recommendations?.[0];
      const alt = (r.recommendation?.recommendations || []).slice(1);
      return `
      <div class="batch-row">
        <div class="head">
          <div class="attr-name">${escapeHtml(r.input.name)}</div>
          ${top ? `<span class="conf ${String(top.confidence).toLowerCase()}">${escapeHtml(top.confidence)}</span>` : ''}
        </div>
        ${top ? `<div class="top-rec">${escapeHtml(top.oagis_path)}</div>` : ''}
        ${top ? `<div class="muted small" style="margin-top:6px">${escapeHtml(top.rationale)}</div>` : ''}
        ${
          alt.length
            ? `<details style="margin-top:8px"><summary class="muted small">${alt.length} alternative${alt.length > 1 ? 's' : ''}</summary>
            ${alt
              .map(
                (a) =>
                  `<div style="margin-top:6px"><span class="conf ${String(a.confidence).toLowerCase()}">${escapeHtml(a.confidence)}</span>
                   <span class="top-rec" style="margin-left:8px">${escapeHtml(a.oagis_path)}</span>
                   <div class="muted small">${escapeHtml(a.rationale)}</div></div>`
              )
              .join('')}
          </details>`
            : ''
        }
        ${r.recommendation?.needs_human_review ? `<div class="review-flag" style="margin-top:10px">⚠ ${escapeHtml(r.recommendation.notes || 'Needs human review')}</div>` : ''}
      </div>`;
    })
    .join('');
}

$('#btn-batch-csv').addEventListener('click', () => {
  if (!lastBatchResults) return;
  const rows = [['Attribute', 'Data Type', 'Description', 'Context', 'Top OAGIS Path', 'Confidence', 'Rationale', 'Alt Paths', 'Needs Review']];
  for (const r of lastBatchResults) {
    const recs = r.recommendation?.recommendations || [];
    const top = recs[0] || {};
    const alts = recs
      .slice(1)
      .map((a) => `${a.confidence}: ${a.oagis_path}`)
      .join(' ; ');
    rows.push([
      r.input.name,
      r.input.data_type,
      r.input.description,
      r.input.context,
      top.oagis_path || '',
      top.confidence || '',
      (top.rationale || '').replace(/\s+/g, ' '),
      alts,
      r.recommendation?.needs_human_review ? 'YES' : '',
    ]);
  }
  const csv = rows.map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `oagis-mappings-${Date.now()}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
});

// ============================================================
// Helpers
// ============================================================
function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
function escapeAttr(s) {
  return escapeHtml(s);
}

// ============================================================
// Init
// ============================================================
refreshHealth();
refreshUploads();
setInterval(refreshHealth, 30000);
