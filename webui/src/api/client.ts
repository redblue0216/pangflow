const BASE_URL = '';

export interface Workflow {
  id: string;
  name: string;
  version: string;
  status: string;
  updated_at: string;
}

export interface Execution {
  id: string;
  workflow_id: string;
  workflow_name: string;
  status: string;
  started_at: string;
  ended_at: string | null;
  run_id: string;
}

export interface ExecutionNode {
  id: number;
  timestamp: string;
  node_id: string;
  node_name: string;
  status: string;
  duration_ms: number | null;
  message: string | null;
  exception: string | null;
}

export interface ModelArtifact {
  id: string;
  name: string;
  version: string;
  stage: string;
  created_at: string;
}

export interface LineageEdge {
  source: string;
  target: string;
  type: string;
}

export interface SettingsConfig {
  version: string;
  environment: string;
  database_url: string;
  log_level: string;
}

async function request<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function requestPost<T>(path: string, body?: Record<string, unknown>): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export const api = {
  getWorkflows: (): Promise<Workflow[]> => request<Workflow[]>('/api/workflows'),
  getExecutions: (): Promise<Execution[]> => request<Execution[]>('/api/executions'),
  getExecutionNodes: (runId: string): Promise<ExecutionNode[]> =>
    request<ExecutionNode[]>(`/api/executions/${runId}/nodes`),
  getModels: (): Promise<ModelArtifact[]> => request<ModelArtifact[]>('/api/models'),
  getLineage: (): Promise<LineageEdge[]> => request<LineageEdge[]>('/api/lineage'),
  getSettings: (): Promise<SettingsConfig> => request<SettingsConfig>('/api/settings'),
  promoteModel: (id: string): Promise<{ success: boolean }> =>
    requestPost<{ success: boolean }>(`/api/models/${id}/promote`, { stage: 'production' }),
  rollbackModel: (id: string): Promise<{ success: boolean }> =>
    requestPost<{ success: boolean }>(`/api/models/${id}/rollback`),
};
