import type { Revision } from '@/api/types';

// Draft revisions cannot be re-read from the server (no revision GET API),
// so the last known draft snapshot is kept in sessionStorage to survive page
// refreshes within the tab. Stale optimistic-lock versions are handled by the
// standard conflict flow.

function storageKey(tenantId: string, appId: string): string {
  return `trpc.admin.draft.${tenantId}.${appId}`;
}

export function loadDraftSession(tenantId: string, appId: string): Revision | null {
  try {
    const raw = sessionStorage.getItem(storageKey(tenantId, appId));
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as Revision;
    if (parsed?.AppID !== appId || parsed?.TenantID !== tenantId || typeof parsed?.Revision !== 'number') {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export function saveDraftSession(revision: Revision): void {
  try {
    sessionStorage.setItem(storageKey(revision.TenantID, revision.AppID), JSON.stringify(revision));
  } catch {
    // ignore storage failures
  }
}

export function clearDraftSession(tenantId: string, appId: string): void {
  try {
    sessionStorage.removeItem(storageKey(tenantId, appId));
  } catch {
    // ignore storage failures
  }
}
