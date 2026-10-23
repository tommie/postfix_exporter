//go:build !nodocker
// +build !nodocker

package logsource

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
)

func TestNewDockerLogSource(t *testing.T) {
	ctx := context.Background()
	c := &fakeDockerClient{}
	src, err := NewDockerLogSource(ctx, c, "acontainer")
	if err != nil {
		t.Fatalf("NewDockerLogSource failed: %v", err)
	}

	assert.Equal(t, []string{"acontainer"}, c.containerLogsCalls, "A call to ContainerLogs should be made.")

	if err := src.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	assert.Equal(t, 1, c.closeCalls, "A call to Close should be made.")
}

func TestDockerLogSource_Path(t *testing.T) {
	ctx := context.Background()
	c := &fakeDockerClient{}
	src, err := NewDockerLogSource(ctx, c, "acontainer")
	if err != nil {
		t.Fatalf("NewDockerLogSource failed: %v", err)
	}
	defer src.Close()

	assert.Equal(t, "docker:acontainer", src.Path(), "Path should be set by New.")
}

func TestDockerLogSource_Read(t *testing.T) {
	ctx := context.Background()

	c := &fakeDockerClient{
		logsReader: io.NopCloser(strings.NewReader("Feb 13 23:31:30 ahost anid[123]: aline\n")),
	}
	src, err := NewDockerLogSource(ctx, c, "acontainer")
	if err != nil {
		t.Fatalf("NewDockerLogSource failed: %v", err)
	}
	defer src.Close()

	s, err := src.Read(ctx)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	assert.Equal(t, "Feb 13 23:31:30 ahost anid[123]: aline", s, "Read should get data from the journal entry.")
}

func TestDockerLogSource_ReadReopensEndedStream(t *testing.T) {
	ctx := context.Background()

	c := &fakeDockerClient{
		logs: []fakeDockerLogs{
			{reader: io.NopCloser(strings.NewReader("first\npartial"))},
			// The container is gone: ContainerLogs fails.
			{err: errors.New("No such container: acontainer")},
			// A stopped container's stream ends immediately.
			{reader: io.NopCloser(strings.NewReader(""))},
			{reader: io.NopCloser(strings.NewReader("second\n"))},
		},
	}
	src, err := NewDockerLogSource(ctx, c, "acontainer")
	if err != nil {
		t.Fatalf("NewDockerLogSource failed: %v", err)
	}
	defer src.Close()
	var sleeps []time.Duration
	src.sleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}

	for _, want := range []string{"first", "partial", "second"} {
		got, err := src.Read(ctx)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		assert.Equal(t, want, got, "Read should continue across ended streams.")
	}

	assert.Equal(t, []string{"acontainer", "acontainer", "acontainer", "acontainer"}, c.containerLogsCalls, "Each ended stream should be reopened.")
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}, sleeps, "Reopening should back off until a line is read.")
	for _, opts := range c.containerLogsOpts[1:] {
		assert.True(t, opts.Follow, "Reopened streams should follow.")
		assert.NotEmpty(t, opts.Since, "Reopened streams should start where the old one ended.")
	}
}

func TestDockerLogSource_ReadStopsOnCanceledReopen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &fakeDockerClient{
		logs: []fakeDockerLogs{{reader: io.NopCloser(strings.NewReader(""))}},
	}
	src, err := NewDockerLogSource(ctx, c, "acontainer")
	if err != nil {
		t.Fatalf("NewDockerLogSource failed: %v", err)
	}
	defer src.Close()
	cancel()

	_, err = src.Read(ctx)
	assert.ErrorIs(t, err, context.Canceled, "Read should stop when the context is canceled while reopening.")
}

type fakeDockerLogs struct {
	reader io.ReadCloser
	err    error
}

type fakeDockerClient struct {
	logsReader io.ReadCloser
	// logs, if set, is returned in order by successive ContainerLogs calls.
	logs []fakeDockerLogs

	containerLogsCalls []string
	containerLogsOpts  []client.ContainerLogsOptions
	closeCalls         int
}

func (c *fakeDockerClient) ContainerLogs(ctx context.Context, containerID string, opts client.ContainerLogsOptions) (client.ContainerLogsResult, error) {
	c.containerLogsCalls = append(c.containerLogsCalls, containerID)
	c.containerLogsOpts = append(c.containerLogsOpts, opts)
	if len(c.logs) > 0 {
		l := c.logs[0]
		c.logs = c.logs[1:]
		return l.reader, l.err
	}
	if c.logsReader == nil {
		return io.NopCloser(strings.NewReader("")), nil
	}
	return c.logsReader, nil
}

func (c *fakeDockerClient) Close() error {
	c.closeCalls++
	return nil
}
