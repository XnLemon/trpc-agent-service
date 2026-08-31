package attachment

import (
	"context"
	"io"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// StoreReader loads attachments from a tenant-scoped ObjectStore. The object
// key is the internal Reference.ID; provider metadata is never used for lookup.
type StoreReader struct {
	Store runtimestorage.ObjectStore
}

// Load reads and verifies one attachment from the configured object store.
func (reader StoreReader) Load(ctx context.Context, tenantID string, reference Reference) (Content, error) {
	normalized, err := reference.Normalize()
	if err != nil {
		return Content{}, err
	}
	if reader.Store == nil {
		return Content{}, ErrInvalid
	}
	body, info, err := reader.Store.GetObject(ctx, tenantID, normalized.ID)
	if err != nil {
		return Content{}, err
	}
	if body == nil {
		return Content{}, ErrInvalid
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(io.LimitReader(body, normalized.Size+1))
	if err != nil {
		return Content{}, err
	}
	if int64(len(data)) > normalized.Size || info.TenantID != tenantID || info.ObjectKey != normalized.ID || info.Size != normalized.Size || info.ContentType != normalized.MIMEType {
		return Content{}, ErrInvalid
	}
	content := Content{Data: data}
	if err := content.Validate(normalized); err != nil {
		return Content{}, err
	}
	return content, nil
}
