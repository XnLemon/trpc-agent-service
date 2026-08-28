package wechat

import (
	"context"
	"testing"
)

type fakePublic struct{}

func (fakePublic) SendPublic(context.Context, Message) (string, error) { return "public-1", nil }

type fakeCustomer struct{}

func (fakeCustomer) SendCustomerService(context.Context, Message) (string, error) {
	return "customer-1", nil
}

func TestProvidersKeepProductBoundaries(t *testing.T) {
	public, err := NewPublicProvider(PublicConfig{AppID: "public", AppSecret: "secret"}, fakePublic{})
	if err != nil {
		t.Fatal(err)
	}
	customer, err := NewCustomerServiceProvider(CustomerServiceConfig{AppID: "customer", AppSecret: "secret"}, fakeCustomer{})
	if err != nil {
		t.Fatal(err)
	}
	if public.Product() != ProductPublicAccount || customer.Product() != ProductCustomerService {
		t.Fatal("provider product boundary was not explicit")
	}
	if id, err := public.Send(context.Background(), Message{ToUser: "u", Text: "hello"}); err != nil || id != "public-1" {
		t.Fatalf("public send = %q, %v", id, err)
	}
	if id, err := customer.Send(context.Background(), Message{ToUser: "u", Text: "hello"}); err != nil || id != "customer-1" {
		t.Fatalf("customer send = %q, %v", id, err)
	}
}
