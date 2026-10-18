package partition

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// PartitionConfig holds operational configurations for a partition.
type PartitionConfig struct {
	Dir               string
	Topic             string
	PartitionID       int32
	MaxSegmentBytes   int64 // e.g. 10MB or 1GB
	TierRetentionTime int64 // Retention in milliseconds before candidate for cold storage
}

// Partition manages ordered log segments for a single topic-partition.
type Partition struct {
	topic       string
	partitionID int32
	dir         string
	maxSegBytes int64

	mu            sync.RWMutex
	segments      []*LogSegment
	activeSegment *LogSegment
	closed        bool
}

// OpenPartition initializes or restores a Partition from disk.
func OpenPartition(cfg PartitionConfig) (*Partition, error) {
	partDir := filepath.Join(cfg.Dir, fmt.Sprintf("%s-%d", cfg.Topic, cfg.PartitionID))
	if err := os.MkdirAll(partDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partition dir: %w", err)
	}

	p := &Partition{
		topic:       cfg.Topic,
		partitionID: cfg.PartitionID,
		dir:         partDir,
		maxSegBytes: cfg.MaxSegmentBytes,
	}

	if p.maxSegBytes <= 0 {
		p.maxSegBytes = 10 * 1024 * 1024 // Default 10MB per segment
	}

	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Partition) loadSegments() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}

	var baseOffsets []int64
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".log") {
			name := strings.TrimSuffix(entry.Name(), ".log")
			baseOffset, err := strconv.ParseInt(name, 10, 64)
			if err == nil {
				baseOffsets = append(baseOffsets, baseOffset)
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	if len(baseOffsets) == 0 {
		baseOffsets = append(baseOffsets, 0)
	}

	for _, bo := range baseOffsets {
		seg, err := NewLogSegment(p.dir, bo)
		if err != nil {
			return fmt.Errorf("failed to load segment %d: %w", bo, err)
		}
		p.segments = append(p.segments, seg)
	}

	p.activeSegment = p.segments[len(p.segments)-1]
	return nil
}

// Append writes a key/value payload to the active segment, rolling if max segment size reached.
func (p *Partition) Append(key []byte, value []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return -1, ErrClosed
	}

	if p.activeSegment.Size() >= p.maxSegBytes {
		nextOffset := p.activeSegment.NextOffset()
		newSeg, err := NewLogSegment(p.dir, nextOffset)
		if err != nil {
			return -1, fmt.Errorf("failed to roll segment: %w", err)
		}
		p.segments = append(p.segments, newSeg)
		p.activeSegment = newSeg
	}

	return p.activeSegment.Append(key, value)
}

// ReadReads reads up to maxRecords starting from startOffset.
func (p *Partition) Read(startOffset int64, maxRecords int) ([]*Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrClosed
	}

	if len(p.segments) == 0 {
		return nil, nil
	}

	var records []*Record

	for _, seg := range p.segments {
		if startOffset >= seg.NextOffset() {
			continue // Segments in the past
		}

		physPos, err := seg.FindPhysicalPosition(startOffset)
		if err != nil {
			// If before segment's earliest entry, start at 0
			if startOffset <= seg.BaseOffset() {
				physPos = 0
			} else {
				continue
			}
		}

		for len(records) < maxRecords {
			rec, bytesRead, err := seg.ReadRecordAt(physPos)
			if err != nil {
				break // end of this segment or error
			}
			physPos += bytesRead
			if rec.Offset >= startOffset {
				records = append(records, rec)
				startOffset = rec.Offset + 1
			}
		}

		if len(records) >= maxRecords {
			break
		}
	}

	return records, nil
}

// GetSegments returns snapshot copy of all segments (for cold storage tier flusher).
func (p *Partition) GetSegments() []*LogSegment {
	p.mu.RLock()
	defer p.mu.RUnlock()

	res := make([]*LogSegment, len(p.segments))
	copy(res, p.segments)
	return res
}

// ActiveSegmentBaseOffset returns base offset of the active mutable segment.
func (p *Partition) ActiveSegmentBaseOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeSegment.BaseOffset()
}

// NextOffset returns the next monotonic offset.
func (p *Partition) NextOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeSegment.NextOffset()
}

// Close closes all segments safely.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var lastErr error
	for _, seg := range p.segments {
		if err := seg.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
