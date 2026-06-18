package utils

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
)

func NewSecureClient(certPath string) (*http.Client, error) {
	pemData, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("Cannot read the certificate: %v", err)
	}

	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, errors.New("Invalid certificate")
	}

	expectedCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse certificate: %v", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
					for _, raw := range rawCerts {
						if bytes.Equal(raw, expectedCert.Raw) {
							return nil
						}
					}
					return errors.New("Unknown certificate")
				},
			},
		},
	}
	return client, nil
}