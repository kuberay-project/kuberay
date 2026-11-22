package rrsa

import (
	"context"
	"os"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials/providers"
)

// NewCredentialsProvider creates a new credentials.CredentialsProvider instance.
// TODO(ChenYi015): Add support for other credential providers.
func NewCredentialsProvider() (credentials.CredentialsProvider, error) {
	var provider providers.CredentialsProvider
	var err error

	if os.Getenv("LOCAL_TEST") == "true" {
		provider = providers.NewDefaultCredentialsProvider()
	} else {
		provider, err = providers.NewOIDCCredentialsProviderBuilder().Build()
	}

	if err != nil {
		return nil, err
	}

	return &OIDCCredentialsProvider{provider: provider}, nil
}

// OIDCCredentialsProvider implements credentials.CredentialsProvider interface.
type OIDCCredentialsProvider struct {
	provider providers.CredentialsProvider
}

// GetCredentials implements credentials.CredentialsProvider interface.
func (p *OIDCCredentialsProvider) GetCredentials(_ context.Context) (credentials.Credentials, error) {
	cred, err := p.provider.GetCredentials()
	if err != nil {
		return credentials.Credentials{}, err
	}

	return credentials.Credentials{
		AccessKeyID:     cred.AccessKeyId,
		AccessKeySecret: cred.AccessKeySecret,
		SecurityToken:   cred.SecurityToken,
	}, nil
}
