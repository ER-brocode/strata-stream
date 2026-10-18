package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ER-brocode/strata-stream/pkg/partition"
)

// StorageBackend abstracts object stores (S3, MinIO, GCS, local blob store).
type StorageBackend interface {
	Upload(ctx context.Context, key string, r io.Reader, size int64) error
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// LocalDiskBlobStore provides an on-disk mock/local S3 backend for high-throughput testing and isolated dev runs.
type LocalDiskBlobStore struct {
	rootDir string
	mu      sync.RWMutex
}

// NewLocalDiskBlobStore initializes a local object store.
func NewLocalDiskBlobStore(rootDir string) (*LocalDiskBlobStore, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, err
	}
	return &LocalDiskBlobStore{rootDir: rootDir}, nil
}

func (s *LocalDiskBlobStore) Upload(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	destPath := filepath.Join(s.rootDir, key)
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	return err
}

func (s *LocalDiskBlobStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	destPath := filepath.Join(s.rootDir, key)
	return os.Open(destPath)
}

func (s *LocalDiskBlobStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	destPath := filepath.Join(s.rootDir, key)
	return os.Remove(destPath)
}

func (s *LocalDiskBlobStore) Exists(ctx context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	destPath := filepath.Join(s.rootDir, key)
	_, err := os.Stat(destPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// S3CompatibleClient provides a pure standard library REST client for S3/MinIO compatible stores.
type S3CompatibleClient struct {
	endpoint   string
	bucket     string
	accessKey  string
	secretKey  string
	httpClient *http.Client
}

// NewS3CompatibleClient creates an S3 client without heavy external SDK dependencies.
func NewS3CompatibleClient(endpoint, bucket, accessKey, secretKey string) *S3CompatibleClient {
	return &S3CompatibleClient{
		endpoint:   endpoint,
		bucket:     bucket,
		accessKey:  accessKey,
		secretKey:  secretKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *S3CompatibleClient) Upload(ctx context.Context, key string, r io.Reader, size int64) error {
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("s3 upload failed (%d): %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *S3CompatibleClient) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("s3 download failed with status %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (c *S3CompatibleClient) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *S3CompatibleClient) Exists(ctx context.Context, key string) (bool, error) {
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, c.bucket, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

// TieredFlusher periodically inspects inactive sealed segments and offloads them to S3/cold storage.
type TieredFlusher struct {
	backend        StorageBackend
	partition      *partition.Partition
	topic          string
	partitionID    int32
	flushedOffsets map[int64]bool
	mu             sync.Mutex
	stopCh         chan struct{}
}

// NewTieredFlusher creates a flusher for offloading closed segments.
func NewTieredFlusher(backend StorageBackend, p *partition.Partition, topic string, partitionID int32) *TieredFlusher {
	return &TieredFlusher{
		backend:        backend,
		partition:      p,
		topic:          topic,
		partitionID:    partitionID,
		flushedOffsets: make(map[int64]bool),
		stopCh:         make(chan struct{}),
	}
}

// FlushClosedSegments scans all segments in the partition and flushes non-active sealed segments to cold storage.
func (f *TieredFlusher) FlushClosedSegments(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	activeBase := f.partition.ActiveSegmentBaseOffset()
	segments := f.partition.GetSegments()

	flushedCount := 0
	for _, seg := range segments {
		// Do not flush the active mutable segment
		if seg.BaseOffset() == activeBase {
			continue
		}

		if f.flushedOffsets[seg.BaseOffset()] {
			continue
		}

		// Upload log file
		logKey := fmt.Sprintf("strata-cold/%s/%d/%020d.log", f.topic, f.partitionID, seg.BaseOffset())
		lf, err := os.Open(seg.LogPath())
		if err != nil {
			return flushedCount, fmt.Errorf("failed to open segment for upload: %w", err)
		}
		stat, _ := lf.Stat()
		err = f.backend.Upload(ctx, logKey, lf, stat.Size())
		lf.Close()
		if err != nil {
			return flushedCount, fmt.Errorf("failed to upload segment log to cold tier: %w", err)
		}

		// Upload index file
		idxKey := fmt.Sprintf("strata-cold/%s/%d/%020d.index", f.topic, f.partitionID, seg.BaseOffset())
		idf, err := os.Open(seg.IndexPath())
		if err != nil {
			return flushedCount, fmt.Errorf("failed to open index for upload: %w", err)
		}
		istat, _ := idf.Stat()
		err = f.backend.Upload(ctx, idxKey, idf, istat.Size())
		idf.Close()
		if err != nil {
			return flushedCount, fmt.Errorf("failed to upload segment index to cold tier: %w", err)
		}

		f.flushedOffsets[seg.BaseOffset()] = true
		flushedCount++
	}

	return flushedCount, nil
}

// StartBackgroundLoop runs an automatic cold flusher every ticker interval.
func (f *TieredFlusher) StartBackgroundLoop(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, _ = f.FlushClosedSegments(ctx)
				cancel()
			case <-f.stopCh:
				return
			}
		}
	}()
}

// Stop stops the background flusher.
func (f *TieredFlusher) Stop() {
	close(f.stopCh)
}
