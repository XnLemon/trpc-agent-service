import { useCallback, useEffect, useState } from 'react';
import type { ListResponse } from '@/api/client';
import { ApiError } from '@/api/client';
import { categoryMessage } from '@/lib/mutation';

export interface CursorListParams {
  q?: string;
  status?: string;
  cursor?: string;
  limit: number;
  [key: string]: string | number | undefined;
}

export type CursorListLoader<T> = (
  params: CursorListParams,
) => Promise<{ data: ListResponse<T> }>;

export function useCursorResourceList<T>(loader: CursorListLoader<T>, pageSize = 25) {
  const [items, setItems] = useState<T[]>([]);
  const [query, setQuery] = useState('');
  const [status, setStatus] = useState('');
  const [page, setPage] = useState(1);
  const [pageCursors, setPageCursors] = useState<Record<number, string | undefined>>({ 1: undefined });
  const [nextCursor, setNextCursor] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadPage = useCallback(
    async (targetPage: number, targetQuery: string, targetStatus: string, cursor?: string) => {
      setLoading(true);
      setError(null);
      try {
        const result = await loader({ q: targetQuery, status: targetStatus, cursor, limit: pageSize });
        const response = result.data;
        setItems(response.items ?? []);
        setNextCursor(response.next_cursor ?? '');
        setPage(targetPage);
        setPageCursors((current) => {
          const next = { ...current };
          if (targetPage === 1) {
            next[1] = undefined;
          }
          next[targetPage + 1] = response.next_cursor || undefined;
          for (const key of Object.keys(next)) {
            if (Number(key) > targetPage + 1) {
              delete next[Number(key)];
            }
          }
          return next;
        });
      } catch (cause) {
        setItems([]);
        setNextCursor('');
        setError(cause instanceof ApiError ? categoryMessage(cause.category) : '列表加载失败，请稍后重试。');
      } finally {
        setLoading(false);
      }
    },
    [loader, pageSize],
  );

  const search = useCallback(() => {
    void loadPage(1, query, status);
  }, [loadPage, query, status]);

  const reset = useCallback(() => {
    setQuery('');
    setStatus('');
    void loadPage(1, '', '');
  }, [loadPage]);

  const changePage = useCallback(
    (targetPage: number) => {
      if (targetPage < 1 || targetPage === page || loading) {
        return;
      }
      if (targetPage > page && !nextCursor) {
        return;
      }
      const cursor = pageCursors[targetPage];
      if (targetPage > 1 && !cursor) {
        return;
      }
      void loadPage(targetPage, query, status, cursor);
    },
    [loadPage, loading, nextCursor, page, pageCursors, query, status],
  );

  useEffect(() => {
    void loadPage(1, '', '');
  }, [loadPage]);

  return {
    items,
    query,
    setQuery,
    status,
    setStatus,
    page,
    pageSize,
    nextCursor,
    hasMore: Boolean(nextCursor),
    loading,
    error,
    reload: search,
    search,
    reset,
    changePage,
  };
}
