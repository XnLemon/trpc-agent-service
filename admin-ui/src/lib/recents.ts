// Session-scoped record of recently opened resource IDs. IDs are not secrets
// (unlike tokens/route keys), so sessionStorage is acceptable; everything is
// cleared when the browser tab closes.

export type RecentKind = 'tenant' | 'app' | 'model' | 'backend' | 'binding';

export interface RecentItem {
  id: string;
  label: string;
  openedAt: number;
}

const MAX_ITEMS = 20;

function storageKey(kind: RecentKind, tenantId: string | null): string {
  return `trpc.admin.recents.${kind}.${tenantId ?? 'global'}`;
}

export function listRecents(kind: RecentKind, tenantId: string | null): RecentItem[] {
  try {
    const raw = sessionStorage.getItem(storageKey(kind, tenantId));
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as RecentItem[]) : [];
  } catch {
    return [];
  }
}

export function addRecent(kind: RecentKind, tenantId: string | null, item: { id: string; label: string }): RecentItem[] {
  const existing = listRecents(kind, tenantId).filter((entry) => entry.id !== item.id);
  const next = [{ ...item, openedAt: Date.now() }, ...existing].slice(0, MAX_ITEMS);
  try {
    sessionStorage.setItem(storageKey(kind, tenantId), JSON.stringify(next));
  } catch {
    // ignore storage failures
  }
  return next;
}

export function removeRecent(kind: RecentKind, tenantId: string | null, id: string): RecentItem[] {
  const next = listRecents(kind, tenantId).filter((entry) => entry.id !== id);
  try {
    sessionStorage.setItem(storageKey(kind, tenantId), JSON.stringify(next));
  } catch {
    // ignore storage failures
  }
  return next;
}
