//go:build !nodocker
// +build !nodocker

package logsource

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/moby/moby/client"
)

const (
	// dockerReopenMinDelay and dockerReopenMaxDelay bound the backoff
	// between attempts to reopen a log stream that has ended.
	dockerReopenMinDelay = time.Second
	dockerReopenMaxDelay = 30 * time.Second
)

// A DockerLogSource reads log records from the given Docker
// journal.
type DockerLogSource struct {
	client      DockerClient
	stream      io.Closer
	reader      *bufio.Reader
	containerID string
	LogSourceDefaults

	// sleep waits between reopen attempts. Tests replace it.
	sleep func(context.Context, time.Duration) error
	// reopenDelay is the next backoff delay. It resets once a
	// reopened stream yields a line.
	reopenDelay time.Duration
}

// A DockerClient is the client interface that client.Client
// provides. See https://pkg.go.dev/github.com/moby/moby/client
type DockerClient interface {
	io.Closer
	ContainerLogs(context.Context, string, client.ContainerLogsOptions) (client.ContainerLogsResult, error)
}

// NewDockerLogSource returns a log source for reading Docker logs.
func NewDockerLogSource(ctx context.Context, c DockerClient, containerID string) (*DockerLogSource, error) {
	r, err := c.ContainerLogs(ctx, containerID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Tail:       "0",
	})
	if err != nil {
		return nil, err
	}

	logSrc := &DockerLogSource{
		client:      c,
		stream:      r,
		containerID: containerID,
		reader:      bufio.NewReader(r),
		sleep:       sleepContext,
		reopenDelay: dockerReopenMinDelay,
	}

	return logSrc, nil
}

func (s *DockerLogSource) Close() error {
	s.stream.Close()
	return s.client.Close()
}

func (s *DockerLogSource) Path() string {
	return "docker:" + s.containerID
}

func (s *DockerLogSource) Read(ctx context.Context) (string, error) {
	for {
		line, err := s.reader.ReadString('\n')
		if err == nil || (err == io.EOF && line != "") {
			s.reopenDelay = dockerReopenMinDelay
			return strings.TrimSpace(line), nil
		}
		if err != io.EOF {
			return "", err
		}

		// The stream ends when the container stops, including when
		// it's restarted or replaced by a new container with the same
		// name. Follow the new stream from now on, instead of
		// silently collecting nothing.
		if err := s.reopen(ctx); err != nil {
			return "", err
		}
	}
}

// reopen replaces the ended log stream with a new one, starting where
// the old one ended. It retries with backoff while the container is
// missing or stopped, since a stopped container's stream ends
// immediately.
func (s *DockerLogSource) reopen(ctx context.Context) error {
	since := time.Now()
	s.stream.Close()

	for {
		if err := s.sleep(ctx, s.reopenDelay); err != nil {
			return err
		}
		s.reopenDelay = min(2*s.reopenDelay, dockerReopenMaxDelay)

		r, err := s.client.ContainerLogs(ctx, s.containerID, client.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Since:      since.Format(time.RFC3339Nano),
		})
		if err != nil {
			slog.Warn("Couldn't reopen Docker logs, retrying.", "container", s.containerID, "error", err.Error())
			continue
		}

		slog.Info("Reopened Docker logs.", "container", s.containerID)
		s.stream = r
		s.reader = bufio.NewReader(r)
		return nil
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// A dockerLogSourceFactory is a factory that can create
// DockerLogSources from command line flags.
type dockerLogSourceFactory struct {
	LogSourceFactoryDefaults
	containerID string
	enable      bool
}

func (f *dockerLogSourceFactory) Init(app *kingpin.Application) {
	app.Flag("docker.enable", "Read from Docker logs. Environment variable DOCKER_HOST can be used to change the address. See https://pkg.go.dev/github.com/moby/moby/client?tab=doc#New for more information.").Default("false").BoolVar(&f.enable)
	app.Flag("docker.container.id", "ID/name of the Postfix Docker container.").Default("postfix").StringVar(&f.containerID)
}

func (f *dockerLogSourceFactory) New(ctx context.Context) ([]LogSourceCloser, error) {
	if !f.enable {
		return nil, nil
	}

	slog.Info("Reading log events from Docker")
	c, err := client.New(client.FromEnv)
	if err != nil {
		return nil, err
	}

	logSource, err := NewDockerLogSource(ctx, c, f.containerID)
	if err != nil {
		return nil, err
	}

	return []LogSourceCloser{logSource}, nil
}

func init() {
	RegisterLogSourceFactory(&dockerLogSourceFactory{})
}
