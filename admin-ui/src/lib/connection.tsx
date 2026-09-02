import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react';
import { AdminClient, type Connection } from '@/api/client';

const BASE_URL_STORAGE_KEY = 'trpc.admin.baseUrl';

interface ConnectionContextValue {
  connection: Connection | null;
  client: AdminClient | null;
  connect: (connection: Connection) => void;
  disconnect: () => void;
}

const ConnectionContext = createContext<ConnectionContextValue>({
  connection: null,
  client: null,
  connect: () => undefined,
  disconnect: () => undefined,
});

export function readStoredBaseUrl(): string {
  try {
    return sessionStorage.getItem(BASE_URL_STORAGE_KEY) ?? '';
  } catch {
    return '';
  }
}

/**
 * Holds the operator-supplied credentials in memory only. The token is never
 * persisted (docs/docs/admin-web-ui.md security boundary); only the API base
 * URL is kept in sessionStorage for convenience. A browser refresh therefore
 * always returns to the connect page.
 */
export function ConnectionProvider({ children }: { children: ReactNode }) {
  const [connection, setConnection] = useState<Connection | null>(null);

  const disconnect = useCallback(() => {
    setConnection(null);
  }, []);

  const connect = useCallback((next: Connection) => {
    try {
      sessionStorage.setItem(BASE_URL_STORAGE_KEY, next.baseUrl);
    } catch {
      // ignore storage failures
    }
    setConnection(next);
  }, []);

  const client = useMemo(() => {
    if (!connection) {
      return null;
    }
    return new AdminClient(connection, disconnect);
  }, [connection, disconnect]);

  const value = useMemo(
    () => ({ connection, client, connect, disconnect }),
    [connection, client, connect, disconnect],
  );

  return <ConnectionContext.Provider value={value}>{children}</ConnectionContext.Provider>;
}

export function useConnection() {
  return useContext(ConnectionContext);
}

export function useAdminClient(): AdminClient {
  const { client } = useContext(ConnectionContext);
  if (!client) {
    throw new Error('AdminClient is not available before connecting');
  }
  return client;
}
