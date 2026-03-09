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

// ── Models ───────────────────────────────────────────────────────────────────

export async function listModels() {
  return req('/models')
}

export async function createModel(body) {
  return req('/models', { method: 'POST', ...json(body) })
}

export async function updateModel(id, body) {
  return req(`/models/${id}`, { method: 'PATCH', ...json(body) })
}

export async function deleteModel(id) {
  return req(`/models/${id}`, { method: 'DELETE' })
}

// ── Upload ──────────────────────────────────────────────────────────────────

/** Upload an xlsx/xls/csv/tsv file scoped to a model.  Returns list[DatasetResponse]. */
export async function uploadFile(file, modelId = null) {
  const fd = new FormData()
  fd.append('file', file)
  const path = modelId ? `/models/${modelId}/upload` : '/upload'
  return req(path, { method: 'POST', body: fd })
}

// ── Datasets ────────────────────────────────────────────────────────────────

/** List all non-deleted datasets in a model. */
export async function getDatasets(modelId) {
  if (modelId) return req(`/models/${modelId}/datasets`)
  return req('/datasets')
}

/** Get a single dataset's full schema. */
export async function getDataset(datasetId, modelId = null) {
  if (modelId) return req(`/models/${modelId}/datasets/${datasetId}`)
  return req(`/datasets/${datasetId}`)
}

/** Update a column's role or display name. */
export async function updateColumnRole(datasetId, columnId, updates, modelId = null) {
  if (modelId) {
    return req(`/models/${modelId}/datasets/${datasetId}/columns/${columnId}`, {
      method: 'PATCH',
      ...json(updates),
    })
  }
  return req(`/datasets/${datasetId}/columns/${columnId}`, {
    method: 'PATCH',
    ...json(updates),
  })
}

/** Soft-delete a dataset. */
export async function deleteDataset(datasetId, modelId = null) {
  if (modelId) return req(`/models/${modelId}/datasets/${datasetId}`, { method: 'DELETE' })
  return req(`/datasets/${datasetId}`, { method: 'DELETE' })
}

// ── Baseline ────────────────────────────────────────────────────────────────

/**
 * Build the enriched baseline by joining the fact table with dimension tables.
 */
export async function getBaseline(factDatasetId, relationshipIds = [], modelId = null) {
  const body = {
    fact_dataset_id: factDatasetId,
    relationships: relationshipIds.map((id) => ({ rel_id: id })),
  }
  const path = modelId
    ? `/models/${modelId}/datasets/baseline`
    : '/datasets/baseline'
  return req(path, { method: 'POST', ...json(body) })
}

// ── Relationships ────────────────────────────────────────────────────────────

export async function createRelationship(body, modelId = null) {
  const path = modelId ? `/models/${modelId}/relationships` : '/relationships'
  return req(path, { method: 'POST', ...json(body) })
}

export async function updateRelationship(id, body, modelId = null) {
  if (modelId) return req(`/models/${modelId}/relationships/${id}`, { method: 'PUT', ...json(body) })
  return req(`/relationships/${id}`, { method: 'PUT', ...json(body) })
}

export async function deleteRelationship(id, modelId = null) {
  if (modelId) return req(`/models/${modelId}/relationships/${id}`, { method: 'DELETE' })
  return req(`/relationships/${id}`, { method: 'DELETE' })
}

// ── Scenarios ────────────────────────────────────────────────────────────────

/** List scenarios for a model. */
export async function getScenarios(datasetId, modelId = null) {
  if (modelId) return req(`/models/${modelId}/scenarios`)
  const qs = datasetId ? `?dataset_id=${datasetId}` : ''
  return req(`/scenarios${qs}`)
}

export async function createScenario(body, modelId = null) {
  const path = modelId ? `/models/${modelId}/scenarios` : '/scenarios'
  return req(path, { method: 'POST', ...json(body) })
}

export async function updateScenario(id, body, modelId = null) {
  if (modelId) return req(`/models/${modelId}/scenarios/${id}`, { method: 'PUT', ...json(body) })
  return req(`/scenarios/${id}`, { method: 'PUT', ...json(body) })
}

export async function deleteScenario(id, modelId = null) {
  if (modelId) return req(`/models/${modelId}/scenarios/${id}`, { method: 'DELETE' })
  return req(`/scenarios/${id}`, { method: 'DELETE' })
}

/** Compute a scenario server-side. Returns columns, rows (including projections). */
export async function computeScenario(scenarioId, factDatasetId, relationshipIds = [], valueColumn = null, modelId = null) {
  const body = {
    fact_dataset_id: factDatasetId,
    relationships: relationshipIds.map(id => ({ rel_id: id })),
  }
  if (valueColumn) body.value_column = valueColumn
  const path = modelId
    ? `/models/${modelId}/scenarios/${scenarioId}/compute`
    : `/scenarios/${scenarioId}/compute`
  return req(path, { method: 'POST', ...json(body) })
}

// ── Knowledge entries ─────────────────────────────────────────────────────────

export async function getKnowledge(datasetId, modelId = null) {
  if (modelId) return req(`/models/${modelId}/knowledge`)
  return req(`/datasets/${datasetId}/knowledge`)
}

export async function createKnowledge(datasetId, body, modelId = null) {
  if (modelId) return req(`/models/${modelId}/knowledge`, { method: 'POST', ...json({ ...body, dataset_id: datasetId }) })
  return req(`/datasets/${datasetId}/knowledge`, { method: 'POST', ...json(body) })
}

export async function updateKnowledge(entryId, body, modelId = null) {
  if (modelId) return req(`/models/${modelId}/knowledge/${entryId}`, { method: 'PUT', ...json(body) })
  return req(`/knowledge/${entryId}`, { method: 'PUT', ...json(body) })
}

export async function deleteKnowledge(entryId, modelId = null) {
  if (modelId) return req(`/models/${modelId}/knowledge/${entryId}`, { method: 'DELETE' })
  return req(`/knowledge/${entryId}`, { method: 'DELETE' })
}

// ── Chat (SSE streaming) ─────────────────────────────────────────────────────

/**
 * Stream a chat response from the AI.
 * Yields typed event objects until {type:'done'}.
 */
export async function* streamChat(message, datasetId, history = [], signal, agentMode = "scenario", modelId = null) {
  const path = modelId ? `/models/${modelId}/chat` : '/chat'
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    signal,
    ...json({
      message,
      dataset_id: datasetId,
      conversation_history: history,
      agent_mode: agentMode,
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
