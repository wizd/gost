package e2e

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func RunEchoContainer(ctx context.Context, networkName string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "hashicorp/http-echo:latest",
		Networks:     []string{networkName},
		Cmd:          []string{"-text=hello-gost", "-listen=:5678"},
		ExposedPorts: []string{"5678/tcp"},
		WaitingFor:   wait.ForHTTP("/").WithPort("5678/tcp"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func RunGostContainer(ctx context.Context, networkName, yamlPath string) (testcontainers.Container, error) {
	return RunGostContainerWithFiles(ctx, networkName, yamlPath, nil, nil)
}

func RunGostContainerWithFiles(
	ctx context.Context,
	networkName, yamlPath string,
	extraFiles []testcontainers.ContainerFile,
	waitStrategy wait.Strategy,
) (testcontainers.Container, error) {
	files := []testcontainers.ContainerFile{
		{HostFilePath: "/tmp/gost-test-bin", ContainerFilePath: "/bin/gost", FileMode: 0755},
		{HostFilePath: yamlPath, ContainerFilePath: "/config.yaml", FileMode: 0644},
	}
	files = append(files, extraFiles...)

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    ".",
			Dockerfile: "Dockerfile",
			Repo:       "gost-e2e",
			Tag:        "latest",
			KeepImage:  true,
		},
		Networks: []string{networkName},
		Files:    files,
		Cmd:      []string{"/bin/gost", "-C", "/config.yaml"},
	}
	if waitStrategy != nil {
		req.WaitingFor = waitStrategy
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}
