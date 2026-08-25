import { getVercelOidcToken } from '@vercel/oidc';

const BASE_URL = 'https://skills.sh';

export type SkillListItem = {
  id: string;
  slug: string;
  name: string;
  source: string;
  installs: number;
  sourceType: string;
  installUrl: string | null;
  url: string;
  isDuplicate?: boolean;
};

export type LeaderboardResponse = {
  data: SkillListItem[];
  pagination: {
    page: number;
    perPage: number;
    total: number;
    hasMore: boolean;
  };
};

export type SkillDetail = {
  id: string;
  source: string;
  slug: string;
  installs: number;
  hash: string | null;
  files: Array<{ path: string; contents: string }> | null;
};

async function oidcHeaders() {
  const token = await getVercelOidcToken();
  if (!token) throw new Error('Vercel OIDC token is unavailable');
  return { Authorization: `Bearer ${token}` };
}

async function fetchJson<T>(url: string, init?: RequestInit, retries = 4): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, init);
      if (response.ok) return (await response.json()) as T;

      if (response.status === 429 || response.status === 503) {
        const retryAfter = Number(response.headers.get('retry-after') ?? 0);
        const delayMs = retryAfter > 0 ? retryAfter * 1000 : Math.min(1000 * 2 ** attempt, 15000);
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        continue;
      }

      throw new Error(`${response.status} ${response.statusText}: ${await response.text()}`);
    } catch (error) {
      lastError = error;
      if (attempt === retries) break;
      await new Promise((resolve) => setTimeout(resolve, Math.min(1000 * 2 ** attempt, 15000)));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

export async function fetchLeaderboardPage(page: number, perPage = 500) {
  const headers = await oidcHeaders();
  const url = `${BASE_URL}/api/v1/skills?view=all-time&page=${page}&per_page=${perPage}`;
  return fetchJson<LeaderboardResponse>(url, { headers });
}

export async function searchSkills(query: string, limit = 200, owner?: string) {
  const headers = await oidcHeaders();
  const params = new URLSearchParams({ q: query, limit: String(limit) });
  if (owner) params.set('owner', owner);
  return fetchJson<{ data: SkillListItem[]; count: number; query: string; searchType: string }>(
    `${BASE_URL}/api/v1/skills/search?${params.toString()}`,
    { headers },
  );
}

export async function fetchSkillDetail(id: string) {
  const headers = await oidcHeaders();
  const encoded = id.split('/').map(encodeURIComponent).join('/');
  return fetchJson<SkillDetail>(`${BASE_URL}/api/v1/skills/${encoded}`, { headers });
}

export async function fetchLegacySearch(query: string) {
  const params = new URLSearchParams({ q: query });
  return fetchJson<{
    skills: Array<{ id: string; name: string; installs: number; source: string }>;
    count?: number;
  }>(`${BASE_URL}/api/search?${params.toString()}`);
}

export async function fetchLegacyDownload(id: string) {
  const encoded = id.split('/').map(encodeURIComponent).join('/');
  return fetchJson<{
    id?: string;
    hash?: string | null;
    files?: Array<{ path: string; contents: string }>;
  }>(`${BASE_URL}/api/download/${encoded}`);
}

export function findSkillMd(files: Array<{ path: string; contents: string }> | null | undefined) {
  if (!files?.length) return null;
  return (
    files.find((file) => file.path.toLowerCase() === 'skill.md') ??
    files.find((file) => file.path.toLowerCase().endsWith('/skill.md')) ??
    null
  );
}
