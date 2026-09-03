import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { AdminClient, type AdminPrincipal, type Connection } from '@/api/client';

interface ConnectionContextValue {
  connection: Connection | null;
  client: AdminClient | null;
  principal: AdminPrincipal | null;
  status: 'loading' | 'authenticated' | 'unauthenticated';
  connect: (principal: AdminPrincipal) => void;
  disconnect: () => void;
}

const ConnectionContext = createContext<ConnectionContextValue>({
  connection: null,
  client: null,
  principal: null,
  status: 'loading',
  connect: () => undefined,
  disconnect: () => undefined,
});

export function ConnectionProvider({ children }: { children: ReactNode }) {
  const connection = useMemo<Connection>(() => ({ baseUrl: '' }), []);
  const [principal, setPrincipal] = useState<AdminPrincipal | null>(null);
  const [status, setStatus] = useState<'loading' | 'authenticated' | 'unauthenticated'>('loading');

  const client = useMemo(() => new AdminClient(connection, () => {
    setPrincipal(null);
    setStatus('unauthenticated');
  }), [connection]);

  useEffect(() => {
    client.getSession()
      .then(({ data }) => { setPrincipal(data); setStatus('authenticated'); })
      .catch(() => { setPrincipal(null); setStatus('unauthenticated'); });
  }, [client]);

  const disconnect = useCallback(() => {
    void client.logout().catch(() => undefined);
    setPrincipal(null);
    setStatus('unauthenticated');
  }, [client]);

  const connect = useCallback((next: AdminPrincipal) => {
    setPrincipal(next);
    setStatus('authenticated');
  }, []);

  const value = useMemo(
    () => ({ connection, client, principal, status, connect, disconnect }),
    [connection, client, connect, disconnect, principal, status],
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
