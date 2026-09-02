package admin

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListOptionsAndCursorContract(t *testing.T) {
	defaults := listOptions(nil)
	if defaults.Limit != 50 {
		t.Fatalf("default list options = %+v", defaults)
	}
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants?q=%20hello%20&status=active&cursor=next&limit=999", nil)
	options := listOptions(request)
	if options.Query != "hello" || options.Status != "active" || options.Cursor != "next" || options.Limit != 200 {
		t.Fatalf("normalized list options = %+v", options)
	}
	values := listURLValues(options)
	if values.Get("q") != "hello" || values.Get("status") != "active" || values.Get("cursor") != "next" || values.Get("limit") != "200" {
		t.Fatalf("list URL values = %q", values.Encode())
	}
	encoded := encodeCursor(17)
	if decoded, err := decodeCursor(encoded); err != nil || decoded != 17 {
		t.Fatalf("cursor round trip = decoded=%d err=%v", decoded, err)
	}
	if _, err := decodeCursor("not-base64"); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
	negative := base64.RawURLEncoding.EncodeToString([]byte("-1"))
	if _, err := decodeCursor(negative); err == nil {
		t.Fatal("negative cursor was accepted")
	}
}
