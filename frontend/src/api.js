/**
 * DataBobIQ — API client
 *
 * All functions talk to the FastAPI backend via Vite's /api proxy.
 * Streaming chat uses fetch + ReadableStream since EventSource only
 * supports GET requests.
 */

const BASE = '/api'

async function req(path, options = {}) {
  const res = await fetch(BASE + path, options)
  if (!res.ok) {
    let msg = `HTTP ${res.status}`
    try { const body = await res.json(); msg = body.detail ?? JSON.stringify(body) } catch { /* text */ }
    throw new Error(msg)
  }
  if (res.status === 204) return null
  return res.json()
}

const json = (body) => ({
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
})

// ── Upload ──────────────────────────────────────────────────────────────────

/** Upload an xlsx/xls/csv/tsv file.  Returns list[DatasetResponse]. */
export async function uploadFile(file) {
  const fd = new FormData()
  fd.append('file', file)
  return req('/upload', { method: 'POST', body: fd })
}

// ── Datasets ────────────────────────────────────────────────────────────────

/** List all non-deleted datasets with columns + relationships. Returns list[SchemaResponse]. */
export async function getDatasets() {
  return req('/datasets')
}

/** Get a single dataset's full schema. */
export async function getDataset(datasetId) {
  return req(`/datasets/${datasetId}`)
}

/** Update a column's role or display name. */
export async function updateColumnRole(datasetId, columnId, updates) {
  return req(`/datasets/${datasetId}/columns/${columnId}`, {
    method: 'PATCH',
    ...json(updates),
  })
}

/** Soft-delete a dataset. */
export async function deleteDataset(datasetId) {
  return req(`/datasets/${datasetId}`, { method: 'DELETE' })
}

// ── Baseline ────────────────────────────────────────────────────────────────

/**
 * Build the enriched baseline by joining the fact table with dimension tables.
 * @param {string} factDatasetId
 * @param {string[]} relationshipIds  IDs of DatasetRelationship records to include
 * @returns {Promise<{columns: string[], data: any[][], row_count: number}>}
 */
export async function getBaseline(factDatasetId, relationshipIds = []) {
  return req('/datasets/baseline', {
    method: 'POST',
    ...json({
      fact_dataset_id: factDatasetId,
      relationships: relationshipIds.map((id) => ({ rel_id: id })),
    }),
  })
}

// ── Relationships ────────────────────────────────────────────────────────────

export async function createRelationship(body) {
  return req('/relationships', { method: 'POST', ...json(body) })
}

export async function updateRelationship(id, body) {
  return req(`/relationships/${id}`, { method: 'PUT', ...json(body) })
}

export async function deleteRelationship(id) {
  return req(`/relationships/${id}`, { method: 'DELETE' })
}

// ── Scenarios ────────────────────────────────────────────────────────────────

/** List scenarios, optionally filtered by datasetId. */
export async function getScenarios(datasetId) {
  const qs = datasetId ? `?dataset_id=${datasetId}` : ''
  return req(`/scenarios${qs}`)
}

export async function createScenario(body) {
  return req('/scenarios', { method: 'POST', ...json(body) })
}

export async function updateScenario(id, body) {
  return req(`/scenarios/${id}`, { method: 'PUT', ...json(body) })
}

export async function deleteScenario(id) {
  return req(`/scenarios/${id}`, { method: 'DELETE' })
}

// ── Chat (SSE streaming) ─────────────────────────────────────────────────────

/**
 * Stream a chat response from the AI.
 * Yields typed event objects until {type:'done'}.
 *
 * Event shapes:
 *   {type:'text_delta', text:'...'}
 *   {type:'tool_executing', tool:'query_data', input:{...}}
 *   {type:'tool_result', tool:'query_data', result:{...}}
 *   {type:'scenario_rule', rule:{...}}
 *   {type:'done'}
 *   {type:'error', message:'...'}
 */
export async function* streamChat(message, datasetId, history = [], signal) {
  const res = await fetch(`${BASE}/chat`, {
    method: 'POST',
    signal,
    ...json({
      message,
      dataset_id: datasetId,
      conversation_history: history,
    }),
  })

  if (!res.ok) {
    let msg = `HTTP ${res.status}`
    try { const b = await res.json(); msg = b.detail ?? msg } catch { /* skip */ }
    throw new Error(msg)
  }

  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue
        const payload = line.slice(6).trim()
        if (!payload) continue
        try {
          yield JSON.parse(payload)
        } catch {
          /* skip malformed SSE line */
        }
      }
    }
  } finally {
    reader.releaseLock()
  }
}
