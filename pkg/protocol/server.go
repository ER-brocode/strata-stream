package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/ER-brocode/strata-stream/pkg/partition"
)

// BrokerServer handles concurrent client connections, parsing requests and dispatching to partition engines.
type BrokerServer struct {
	addr       string
	dataDir    string
	listener   net.Listener
	partitions map[string]*partition.Partition // key: topic:partition
	mu         sync.RWMutex
	codec      *WireCodec
	stopCh     chan struct{}
}

// NewBrokerServer creates an initialized BrokerServer instance.
func NewBrokerServer(addr string, dataDir string) *BrokerServer {
	return &BrokerServer{
		addr:       addr,
		dataDir:    dataDir,
		partitions: make(map[string]*partition.Partition),
		codec:      &WireCodec{},
		stopCh:     make(chan struct{}),
	}
}

// GetOrCreatePartition returns or instantiates a partition log for the specified topic/partition.
func (s *BrokerServer) GetOrCreatePartition(topic string, partID int32) (*partition.Partition, error) {
	key := fmt.Sprintf("%s:%d", topic, partID)

	s.mu.RLock()
	p, ok := s.partitions[key]
	s.mu.RUnlock()
	if ok {
		return p, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check
	if p, ok := s.partitions[key]; ok {
		return p, nil
	}

	cfg := partition.PartitionConfig{
		Dir:             s.dataDir,
		Topic:           topic,
		PartitionID:     partID,
		MaxSegmentBytes: 2 * 1024 * 1024, // 2MB roll for testability
	}
	newPart, err := partition.OpenPartition(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open partition: %w", err)
	}

	s.partitions[key] = newPart
	return newPart, nil
}

// Start begins listening on the configured network address.
func (s *BrokerServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln

	go s.acceptLoop()
	return nil
}

func (s *BrokerServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				continue
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *BrokerServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		frame, err := s.codec.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			return
		}

		reader := bytes.NewReader(frame)
		header, err := s.codec.DecodeRequestHeader(reader)
		if err != nil {
			return
		}

		switch header.ApiKey {
		case ApiKeyProduce:
			req, err := s.codec.DecodeProduceRequest(reader)
			if err != nil {
				return
			}

			part, err := s.GetOrCreatePartition(req.Topic, req.Partition)
			if err != nil {
				resp := &ProduceResponse{
					CorrelationId: header.CorrelationId,
					Topic:         req.Topic,
					Partition:     req.Partition,
					ErrorCode:     1, // General error
				}
				_ = s.codec.WriteFrame(conn, s.codec.EncodeProduceResponse(resp))
				continue
			}

			var firstOffset int64 = -1
			var prodErr error
			for _, m := range req.Messages {
				off, err := part.Append(m.Key, m.Value)
				if err != nil {
					prodErr = err
					break
				}
				if firstOffset == -1 {
					firstOffset = off
				}
			}

			var errCode int16 = 0
			if prodErr != nil {
				errCode = 1
			}

			resp := &ProduceResponse{
				CorrelationId: header.CorrelationId,
				Topic:         req.Topic,
				Partition:     req.Partition,
				BaseOffset:    firstOffset,
				ErrorCode:     errCode,
			}
			_ = s.codec.WriteFrame(conn, s.codec.EncodeProduceResponse(resp))

		case ApiKeyFetch:
			req, err := s.codec.DecodeFetchRequest(reader)
			if err != nil {
				return
			}

			part, err := s.GetOrCreatePartition(req.Topic, req.Partition)
			if err != nil {
				resp := &FetchResponse{
					CorrelationId: header.CorrelationId,
					Topic:         req.Topic,
					Partition:     req.Partition,
					ErrorCode:     1,
				}
				_ = s.codec.WriteFrame(conn, s.codec.EncodeFetchResponse(resp))
				continue
			}

			records, err := part.Read(req.FetchOffset, 500)
			var fetched []FetchedRecord
			for _, r := range records {
				fetched = append(fetched, FetchedRecord{
					Offset:    r.Offset,
					Timestamp: r.Timestamp,
					Key:       r.Key,
					Value:     r.Value,
				})
			}

			resp := &FetchResponse{
				CorrelationId: header.CorrelationId,
				Topic:         req.Topic,
				Partition:     req.Partition,
				ErrorCode:     0,
				HighWatermark: part.NextOffset(),
				Records:       fetched,
			}
			_ = s.codec.WriteFrame(conn, s.codec.EncodeFetchResponse(resp))

		default:
			// Unsupported API
			return
		}
	}
}

// Addr returns the bound listener address.
func (s *BrokerServer) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Stop shuts down the server and partitions.
func (s *BrokerServer) Stop() error {
	close(s.stopCh)
	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error
	for _, part := range s.partitions {
		if err := part.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
