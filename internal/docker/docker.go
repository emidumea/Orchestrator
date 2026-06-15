package docker

import (
	"context"
	"io"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type ContainerInfo struct {
	ID string
	ContainerName string
	ImageName string
	Command []string
}

type DockerManager struct {
	cli *client.Client
}

func CreateDockerManager() (*DockerManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}

	manager := &DockerManager{cli}
	return manager, nil
}

func (dm *DockerManager) StartContainer(ctx context.Context, config ContainerInfo) (string, error) {
	// pull the image and show the downloading progress in terminal
	reader, err := dm.cli.ImagePull(ctx, config.ImageName, types.ImagePullOptions{})
	if err != nil {
		return "", err
	}

	defer reader.Close()

	io.Copy(os.Stdout, reader)

	// create container
	response, err := dm.cli.ContainerCreate(ctx, &container.Config{
		Image: config.ImageName,
		Cmd: config.Command,
		}, &container.HostConfig{}, nil, nil, config.ContainerName)
	if err != nil {
		return "", err
	}

	containerID := response.ID

	// start container
	if err := dm.cli.ContainerStart(ctx, containerID, types.ContainerStartOptions{}); err != nil {
		return "", err
	}

	return containerID, nil
}

func (dm *DockerManager) StopContainer(ctx context.Context, containerID string) error {
	if err := dm.cli.ContainerStop(ctx, containerID, container.StopOptions{}); err != nil {
		return err
	}

	if err := dm.cli.ContainerRemove(ctx, containerID, types.ContainerRemoveOptions{}); err != nil {
		return err
	}

	return nil
}

func (dm *DockerManager) WaitForContainer(ctx context.Context, containerID string) (int64, error){
	statusCh, errCh := dm.cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		return -1, err
	case status := <-statusCh:
		return status.StatusCode, nil
	}
}