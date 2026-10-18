package partition

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrCorruptRecord    = errors.New("record corrupted / CRC mismatch")
	ErrSegmentFull      = errors.New("segment full")
	ErrClosed           = errors.New("partition is closed")
)

const (
	// Record layout on disk:
	// [4 bytes: Total Length]
	// [4 bytes: CRC32 IEEE]
	// [8 bytes: Absolute Offset]
	// [8 bytes: Timestamp UnixNano]
	// [4 bytes: Key Length (-1 for nil)]
	// [Key bytes...]
	// [4 bytes: Value Length]
	// [Value bytes...]
	RecordHeaderSize = 4 + 4 + 8 + 8 + 4 + 4 // 28 bytes without key & value

	// Index entry layout:
	// [8 bytes: Relative or Absolute Offset]
	// [8 bytes: Physical Position in .log]
	IndexEntrySize = 16
)

// Record represents a single message in the stream.
type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	CRC       uint32
}

// LogSegment represents a pair of (.log, .index) files.
type LogSegment struct {
	baseOffset int64
	nextOffset int64
	dir        string
	logFile    *os.File
	idxFile    *os.File
	logSize    int64
	idxSize    int64
	mu         sync.RWMutex
	closed     bool
}

// NewLogSegment creates or opens an existing log segment for a given base offset.
func NewLogSegment(dir string, baseOffset int64) (*LogSegment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to open index file %s: %w", idxPath, err)
	}

	logStat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		idxFile.Close()
		return nil, err
	}

	idxStat, err := idxFile.Stat()
	if err != nil {
		logFile.Close()
		idxFile.Close()
		return nil, err
	}

	seg := &LogSegment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		dir:        dir,
		logFile:    logFile,
		idxFile:    idxFile,
		logSize:    logStat.Size(),
		idxSize:    idxStat.Size(),
	}

	// Recover nextOffset from index or log if already exists
	if seg.idxSize >= IndexEntrySize {
		lastEntryBuf := make([]byte, IndexEntrySize)
		_, err := idxFile.ReadAt(lastEntryBuf, seg.idxSize-IndexEntrySize)
		if err == nil {
			lastOffset := int64(binary.BigEndian.Uint64(lastEntryBuf[0:8]))
			seg.nextOffset = lastOffset + 1
		}
	}

	return seg, nil
}

// BaseOffset returns the starting offset of this segment.
func (s *LogSegment) BaseOffset() int64 {
	return s.baseOffset
}

// NextOffset returns the next offset to be written.
func (s *LogSegment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

// Size returns total on-disk bytes of log file.
func (s *LogSegment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logSize
}

// Append appends a record payload to the segment and updates the zero-copy index.
func (s *LogSegment) Append(key []byte, value []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return -1, ErrClosed
	}

	offset := s.nextOffset
	ts := time.Now().UnixNano()

	keyLen := int32(len(key))
	if key == nil {
		keyLen = -1
	}
	valLen := int32(len(value))

	// Payload calculation: [CRC (4)] + [Offset (8)] + [TS (8)] + [keyLen (4)] + [key] + [valLen (4)] + [val]
	payloadSize := 4 + 8 + 8 + 4 + max(0, int(keyLen)) + 4 + int(valLen)
	totalRecordSize := 4 + payloadSize // 4 bytes for total length prefix

	buf := make([]byte, totalRecordSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadSize))

	// Write CRC placeholder at buf[4:8]
	binary.BigEndian.PutUint64(buf[8:16], uint64(offset))
	binary.BigEndian.PutUint64(buf[16:24], uint64(ts))
	binary.BigEndian.PutUint32(buf[24:28], uint32(keyLen))

	pos := 28
	if keyLen > 0 {
		copy(buf[pos:pos+int(keyLen)], key)
		pos += int(keyLen)
	}

	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(valLen))
	pos += 4
	if valLen > 0 {
		copy(buf[pos:pos+int(valLen)], value)
		pos += int(valLen)
	}

	// Calculate CRC32 of payload (offset 8 to end)
	checksum := crc32.ChecksumIEEE(buf[8:pos])
	binary.BigEndian.PutUint32(buf[4:8], checksum)

	physPos := s.logSize

	// Write to log file
	n, err := s.logFile.Write(buf)
	if err != nil {
		return -1, fmt.Errorf("failed to write record to log: %w", err)
	}
	s.logSize += int64(n)

	// Write index entry: [8 bytes offset, 8 bytes physical position]
	idxBuf := make([]byte, IndexEntrySize)
	binary.BigEndian.PutUint64(idxBuf[0:8], uint64(offset))
	binary.BigEndian.PutUint64(idxBuf[8:16], uint64(physPos))

	if _, err := s.idxFile.Write(idxBuf); err != nil {
		return -1, fmt.Errorf("failed to write index entry: %w", err)
	}
	s.idxSize += IndexEntrySize

	s.nextOffset++
	return offset, nil
}

// ReadAt reads a single record from the log starting at physical file position.
func (s *LogSegment) ReadRecordAt(physPos int64) (*Record, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if physPos >= s.logSize {
		return nil, 0, io.EOF
	}

	// Read total length prefix
	var lenBuf [4]byte
	if _, err := s.logFile.ReadAt(lenBuf[:], physPos); err != nil {
		return nil, 0, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	totalLen := int64(4 + payloadLen)

	if physPos+totalLen > s.logSize {
		return nil, 0, ErrCorruptRecord
	}

	data := make([]byte, payloadLen)
	if _, err := s.logFile.ReadAt(data, physPos+4); err != nil {
		return nil, 0, err
	}

	expectedCRC := binary.BigEndian.Uint32(data[0:4])
	actualCRC := crc32.ChecksumIEEE(data[4:])
	if expectedCRC != actualCRC {
		return nil, 0, ErrCorruptRecord
	}

	offset := int64(binary.BigEndian.Uint64(data[4:12]))
	ts := int64(binary.BigEndian.Uint64(data[12:20]))
	keyLen := int32(binary.BigEndian.Uint32(data[20:24]))

	pos := 24
	var key []byte
	if keyLen > 0 {
		key = make([]byte, keyLen)
		copy(key, data[pos:pos+int(keyLen)])
		pos += int(keyLen)
	}

	valLen := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	var val []byte
	if valLen > 0 {
		val = make([]byte, valLen)
		copy(val, data[pos:pos+int(valLen)])
	}

	return &Record{
		Offset:    offset,
		Timestamp: ts,
		Key:       key,
		Value:     val,
		CRC:       expectedCRC,
	}, totalLen, nil
}

// FindPhysicalPosition performs binary search on the index file to locate the record position.
func (s *LogSegment) FindPhysicalPosition(targetOffset int64) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	numEntries := s.idxSize / IndexEntrySize
	if numEntries == 0 {
		return -1, ErrOffsetOutOfRange
	}

	low := int64(0)
	high := numEntries - 1
	var bestPos int64 = -1

	buf := make([]byte, IndexEntrySize)

	for low <= high {
		mid := low + (high-low)/2
		if _, err := s.idxFile.ReadAt(buf, mid*IndexEntrySize); err != nil {
			return -1, err
		}
		entryOffset := int64(binary.BigEndian.Uint64(buf[0:8]))
		entryPos := int64(binary.BigEndian.Uint64(buf[8:16]))

		if entryOffset == targetOffset {
			return entryPos, nil
		} else if entryOffset < targetOffset {
			bestPos = entryPos
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if bestPos != -1 {
		return bestPos, nil
	}
	return -1, ErrOffsetOutOfRange
}

// Sync flushes underlying file descriptors to disk.
func (s *LogSegment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.idxFile.Sync()
}

// Close closes segment file handles.
func (s *LogSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	_ = s.logFile.Sync()
	_ = s.idxFile.Sync()
	_ = s.logFile.Close()
	return s.idxFile.Close()
}

// LogPath returns the path to the data log file.
func (s *LogSegment) LogPath() string {
	return s.logFile.Name()
}

// IndexPath returns the path to the index file.
func (s *LogSegment) IndexPath() string {
	return s.idxFile.Name()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
