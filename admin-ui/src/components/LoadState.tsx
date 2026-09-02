import type { ReactNode } from 'react';
import { Alert, Button, Loading } from 'tdesign-react';

interface LoadStateProps {
  loading: boolean;
  error: string | null;
  onRetry?: () => void;
  children: ReactNode;
}

/** Uniform loading / load-failure wrapper for detail pages. */
export function LoadState({ loading, error, onRetry, children }: LoadStateProps) {
  if (loading) {
    return <Loading loading text="加载中…" className="admin-load-state" />;
  }
  if (error) {
    return (
      <Alert
        theme="error"
        message="加载失败"
        description={error}
        operation={onRetry ? <Button size="small" variant="text" theme="primary" onClick={onRetry}>重试</Button> : undefined}
      />
    );
  }
  return <>{children}</>;
}
