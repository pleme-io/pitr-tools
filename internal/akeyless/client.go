// Package akeyless wraps the akeyless-go/v5 SDK with a thin Client that
// authenticates via the akeyless `k8s` auth method (Decision 13;
// Conventions Section 6) and exposes the secret CRUD verbs the pitr-akeyless
// drill Jobs need: CreateSecret, DescribeItem, DeleteItem.
//
// The k8s auth method binds trust to a specific (namespace, ServiceAccount)
// combo. The Job pod's mounted SA token is validated against the cluster's
// TokenReview API by akeyless; on success akeyless returns a temp token used
// for subsequent API calls.
//
// Per Decision 9 — k8s auth (not aws_iam) for cloud portability, tighter
// trust binding than IRSA-role-wide, and reuse across future packaged
// primitives.
package akeyless

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	akl "github.com/akeylesslabs/akeyless-go/v5"
)

// DefaultSATokenPath is where the K8s API server projects the Job pod's
// ServiceAccount token. Standard for every K8s pod with automountServiceAccountToken.
const DefaultSATokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// Client wraps an authenticated akeyless API session.
type Client struct {
	api   *akl.APIClient
	token string
}

// AuthConfig is the operator-supplied per-tenant configuration for the k8s
// auth method. AccessID + ConfigName are populated by ASM-18083 coordination
// (PITR Project Hub Confluence page 4042457094) and threaded into the chart
// via ApplicationSet helm.parameters → ConfigMap → env vars on Job containers.
type AuthConfig struct {
	GatewayURL  string // e.g. https://saas-uam-service-ingress.default.svc.cluster.local:443
	AccessID    string // p-... (per-tenant)
	ConfigName  string // k8s auth config name on the akeyless side (per-tenant)
	SATokenPath string // optional; defaults to DefaultSATokenPath
}

// NewClient authenticates via the akeyless k8s auth method and returns a
// Client carrying the resulting temp token.
func NewClient(ctx context.Context, cfg AuthConfig) (*Client, error) {
	if cfg.GatewayURL == "" {
		return nil, errors.New("akeyless: AuthConfig.GatewayURL is empty")
	}
	if cfg.AccessID == "" {
		return nil, errors.New("akeyless: AuthConfig.AccessID is empty")
	}
	if cfg.ConfigName == "" {
		return nil, errors.New("akeyless: AuthConfig.ConfigName is empty")
	}
	tokenPath := cfg.SATokenPath
	if tokenPath == "" {
		tokenPath = DefaultSATokenPath
	}
	saTokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("akeyless: read SA token at %q: %w", tokenPath, err)
	}
	saToken := strings.TrimSpace(string(saTokenBytes))
	if saToken == "" {
		return nil, fmt.Errorf("akeyless: SA token at %q is empty", tokenPath)
	}

	apiCfg := akl.NewConfiguration()
	apiCfg.Servers = []akl.ServerConfiguration{{URL: cfg.GatewayURL}}
	api := akl.NewAPIClient(apiCfg)

	authReq := akl.NewAuth()
	authReq.SetAccessType("k8s")
	authReq.SetAccessId(cfg.AccessID)
	authReq.SetK8sAuthConfigName(cfg.ConfigName)
	authReq.SetK8sServiceAccountToken(saToken)

	authOut, _, err := api.V2Api.Auth(ctx).Body(*authReq).Execute()
	if err != nil {
		return nil, fmt.Errorf("akeyless: auth (k8s): %w", err)
	}
	if authOut == nil || authOut.Token == nil || *authOut.Token == "" {
		return nil, errors.New("akeyless: auth succeeded but response carried no token")
	}
	return &Client{api: api, token: *authOut.Token}, nil
}

// CreateSecret creates a static secret at path with the given value. Returns
// nil on success; non-nil error includes the akeyless-side response body.
func (c *Client) CreateSecret(ctx context.Context, path, value string) error {
	body := akl.NewCreateSecret(path, value)
	body.SetToken(c.token)
	_, _, err := c.api.V2Api.CreateSecret(ctx).Body(*body).Execute()
	if err != nil {
		return fmt.Errorf("akeyless: create-secret %q: %w", path, err)
	}
	return nil
}

// DescribeItem returns nil if the item exists at path. Non-nil error indicates
// either the item is missing or another API error; callers checking existence
// should test errors.Is(err, ErrNotFound).
func (c *Client) DescribeItem(ctx context.Context, path string) error {
	body := akl.NewDescribeItem(path)
	body.SetToken(c.token)
	_, resp, err := c.api.V2Api.DescribeItem(ctx).Body(*body).Execute()
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: %s", ErrNotFound, path)
		}
		return fmt.Errorf("akeyless: describe-item %q: %w", path, err)
	}
	return nil
}

// DeleteItem deletes the item at path. Idempotent on 404 (already-deleted is
// success); non-nil error otherwise.
func (c *Client) DeleteItem(ctx context.Context, path string) error {
	body := akl.NewDeleteItem(path)
	body.SetToken(c.token)
	_, resp, err := c.api.V2Api.DeleteItem(ctx).Body(*body).Execute()
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil // idempotent — already gone
		}
		return fmt.Errorf("akeyless: delete-item %q: %w", path, err)
	}
	return nil
}

// ErrNotFound is returned by DescribeItem when the item does not exist.
// Callers detect via errors.Is.
var ErrNotFound = errors.New("akeyless: item not found")
