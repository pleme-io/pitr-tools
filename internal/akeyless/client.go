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

// WithGatewayURL returns a new Client with the same auth token but pointing
// at a different gateway URL. Used in the drill verify path: akeyless
// production routes paths via an Ingress (e.g. /auth → auth microservice,
// /describe-item → uam microservice) but the per-microservice saas-pitr
// Pods don't have that router. So the drill auths against auth-svc, then
// queries items against uam-svc, each as its own client carrying the
// shared token.
func (c *Client) WithGatewayURL(url string) *Client {
	apiCfg := akl.NewConfiguration()
	apiCfg.Servers = []akl.ServerConfiguration{{URL: url}}
	return &Client{api: akl.NewAPIClient(apiCfg), token: c.token}
}

// AuthConfig is the operator-supplied per-tenant configuration. The Client
// supports two auth modes:
//
//   - **k8s auth method** (Decision 13; production): AccessID + ConfigName
//     populated by ASM-18083 coordination (PITR Project Hub Confluence page
//     4042457094); the Job pod's projected SA token is the credential.
//   - **api-key auth** (smoke / pre-coordination): AccessID + AccessKey
//     populated from a static admin auth method (e.g. pitr-provisioning).
//     Used when the per-drill k8s auth method on akeyless hasn't been
//     materialized yet, or when the operator is running an offline /
//     debugging drill against the source saas. Mode is selected by
//     populating either ConfigName (k8s) or AccessKey (api-key).
type AuthConfig struct {
	GatewayURL  string // e.g. https://saas-uam-service-ingress.default.svc.cluster.local:443
	AccessID    string // p-... (per-tenant)
	ConfigName  string // k8s auth config name (per-tenant) — empty means api-key path
	AccessKey   string // raw access key for api-key auth — empty means k8s path
	SATokenPath string // optional; defaults to DefaultSATokenPath (k8s path only)
}

// NewClient authenticates via either the akeyless k8s auth method (when
// AuthConfig.ConfigName is set) or the api-key auth method (when
// AuthConfig.AccessKey is set), and returns a Client carrying the resulting
// temp token. Returns an error if neither field is set.
func NewClient(ctx context.Context, cfg AuthConfig) (*Client, error) {
	if cfg.GatewayURL == "" {
		return nil, errors.New("akeyless: AuthConfig.GatewayURL is empty")
	}
	if cfg.AccessID == "" {
		return nil, errors.New("akeyless: AuthConfig.AccessID is empty")
	}
	if cfg.ConfigName == "" && cfg.AccessKey == "" {
		return nil, errors.New("akeyless: AuthConfig must set ConfigName (k8s mode) OR AccessKey (api-key mode)")
	}

	apiCfg := akl.NewConfiguration()
	apiCfg.Servers = []akl.ServerConfiguration{{URL: cfg.GatewayURL}}
	api := akl.NewAPIClient(apiCfg)

	authReq := akl.NewAuth()
	authReq.SetAccessId(cfg.AccessID)

	if cfg.AccessKey != "" {
		// api-key path: no SA token, no k8s config name
		authReq.SetAccessType("access_key")
		authReq.SetAccessKey(cfg.AccessKey)
	} else {
		// k8s path: read SA token from projected volume
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
		authReq.SetAccessType("k8s")
		authReq.SetK8sAuthConfigName(cfg.ConfigName)
		authReq.SetK8sServiceAccountToken(saToken)
	}

	authOut, _, err := api.V2Api.Auth(ctx).Body(*authReq).Execute()
	if err != nil {
		mode := "k8s"
		if cfg.AccessKey != "" {
			mode = "access_key"
		}
		return nil, fmt.Errorf("akeyless: auth (%s): %w", mode, err)
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
