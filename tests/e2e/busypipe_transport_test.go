package e2e

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestRelayOverBusyPipe(t *testing.T) {
	t.Parallel()
	if os.Getenv("RUN_BUSYPIPE_E2E") == "" {
		t.Skip("set RUN_BUSYPIPE_E2E=1 to run BusyPipe transport e2e")
	}
	ctx := context.Background()

	echoC, err := RunEchoContainer(ctx, SharedNetworkName)
	require.NoError(t, err)
	defer echoC.Terminate(ctx)

	echoIP, err := echoC.ContainerIP(ctx)
	require.NoError(t, err)

	serverCfg, err := writeTempConfig(fmt.Sprintf(`
services:
- name: relay-bp-server
  addr: :3307
  handler:
    type: relay
  listener:
    type: bp
    metadata:
      mptcp: true
      bp.minBps: 8000
`))
	require.NoError(t, err)
	defer os.Remove(serverCfg)

	serverC, err := RunGostContainerWithFiles(
		ctx,
		SharedNetworkName,
		serverCfg,
		nil,
		"3307/tcp",
	)
	require.NoError(t, err)
	defer serverC.Terminate(ctx)

	serverIP, err := serverC.ContainerIP(ctx)
	require.NoError(t, err)

	clientCfg, err := writeTempConfig(fmt.Sprintf(`
services:
- name: proxy
  addr: :8080
  handler:
    type: http
    chain: bp-chain
  listener:
    type: tcp

chains:
- name: bp-chain
  hops:
  - name: hop-0
    nodes:
    - name: relay-node
      addr: %s:3307
      connector:
        type: relay
      dialer:
        type: bp
        metadata:
          bp.minBps: 8000
`, serverIP))
	require.NoError(t, err)
	defer os.Remove(clientCfg)

	clientC, err := RunGostContainerWithFiles(
		ctx,
		SharedNetworkName,
		clientCfg,
		nil,
		"8080/tcp",
	)
	require.NoError(t, err)
	defer clientC.Terminate(ctx)

	cmd := []string{"curl", "-s", "-x", "http://127.0.0.1:8080", fmt.Sprintf("http://%s:5678", echoIP)}
	code, out, err := clientC.Exec(ctx, cmd)
	require.NoError(t, err)
	body, err := io.ReadAll(out)
	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.Contains(t, string(body), "hello-gost")
}

func TestRelayOverBusyPipeTLS(t *testing.T) {
	t.Parallel()
	if os.Getenv("RUN_BUSYPIPE_E2E") == "" {
		t.Skip("set RUN_BUSYPIPE_E2E=1 to run BusyPipe transport e2e")
	}
	ctx := context.Background()

	echoC, err := RunEchoContainer(ctx, SharedNetworkName)
	require.NoError(t, err)
	defer echoC.Terminate(ctx)

	echoIP, err := echoC.ContainerIP(ctx)
	require.NoError(t, err)

	certPath, keyPath, err := generateSelfSignedCertFiles(t.TempDir())
	require.NoError(t, err)

	serverCfg, err := writeTempConfig(`
services:
- name: relay-bptls-server
  addr: :3317
  handler:
    type: relay
  listener:
    type: bptls
    tls:
      certFile: /tls/server.crt
      keyFile: /tls/server.key
    metadata:
      mptcp: true
      bp.minBps: 8000
`)
	require.NoError(t, err)
	defer os.Remove(serverCfg)

	serverC, err := RunGostContainerWithFiles(
		ctx,
		SharedNetworkName,
		serverCfg,
		[]testcontainers.ContainerFile{
			{HostFilePath: certPath, ContainerFilePath: "/tls/server.crt", FileMode: 0644},
			{HostFilePath: keyPath, ContainerFilePath: "/tls/server.key", FileMode: 0600},
		},
		"3317/tcp",
	)
	require.NoError(t, err)
	defer serverC.Terminate(ctx)

	serverIP, err := serverC.ContainerIP(ctx)
	require.NoError(t, err)

	clientCfg, err := writeTempConfig(fmt.Sprintf(`
services:
- name: proxy
  addr: :8080
  handler:
    type: http
    chain: bptls-chain
  listener:
    type: tcp

chains:
- name: bptls-chain
  hops:
  - name: hop-0
    nodes:
    - name: relay-node
      addr: %s:3317
      connector:
        type: relay
      dialer:
        type: bptls
        tls:
          secure: false
        metadata:
          bp.minBps: 8000
`, serverIP))
	require.NoError(t, err)
	defer os.Remove(clientCfg)

	clientC, err := RunGostContainerWithFiles(
		ctx,
		SharedNetworkName,
		clientCfg,
		nil,
		"8080/tcp",
	)
	require.NoError(t, err)
	defer clientC.Terminate(ctx)

	cmd := []string{"curl", "-s", "-x", "http://127.0.0.1:8080", fmt.Sprintf("http://%s:5678", echoIP)}
	code, out, err := clientC.Exec(ctx, cmd)
	require.NoError(t, err)
	body, err := io.ReadAll(out)
	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.Contains(t, string(body), "hello-gost")
}

func writeTempConfig(content string) (string, error) {
	f, err := os.CreateTemp("", "gost-bp-*.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		return "", err
	}
	return f.Name(), nil
}

func generateSelfSignedCertFiles(dir string) (string, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return "", "", err
	}
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "gost-bptls-test",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}

	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	certFile, err := os.Create(certPath)
	if err != nil {
		return "", "", err
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		certFile.Close()
		return "", "", err
	}
	certFile.Close()

	keyFile, err := os.Create(keyPath)
	if err != nil {
		return "", "", err
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
		keyFile.Close()
		return "", "", err
	}
	keyFile.Close()
	return certPath, keyPath, nil
}
