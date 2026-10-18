package tests

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/ER-brocode/strata-stream/pkg/partition"
	"github.com/ER-brocode/strata-stream/pkg/protocol"
	"github.com/ER-brocode/strata-stream/pkg/storage"
)

func TestEndToEndPublishConsumeAndTiering(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "strata-e2e-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	brokerDataDir := filepath.Join(tempDir, "broker-data")
	coldStorageDir := filepath.Join(tempDir, "cold-s3")

	server := protocol.NewBrokerServer("127.0.0.1:0", brokerDataDir)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start broker: %v", err)
	}
	defer server.Stop()

	serverAddr := server.Addr().String()

	// Connect client
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	codec := &protocol.WireCodec{}

	topic := "telemetry-metrics"
	var partitionID int32 = 0
	numBatches := 10
	msgsPerBatch := 20
	totalExpected := numBatches * msgsPerBatch

	// Publish batches
	for b := 0; b < numBatches; b++ {
		var messages []protocol.ProduceMessage
		for m := 0; m < msgsPerBatch; m++ {
			k := []byte(fmt.Sprintf("sensor-%d", (b*msgsPerBatch)+m))
			v := []byte(fmt.Sprintf("temperature_reading_celsius=%d.5", 20+(m%15)))
			messages = append(messages, protocol.ProduceMessage{Key: k, Value: v})
		}

		reqBuf := new(bytes.Buffer)
		// Write standard Kafka Header: ApiKeyProduce (0), ApiVersion 1, CorrelationId, ClientId
		codec.EncodeProduceHeader(reqBuf, int32(b))
		codec.AppendProducePayload(reqBuf, topic, partitionID, messages)

		if err := codec.WriteFrame(conn, reqBuf.Bytes()); err != nil {
			t.Fatalf("failed to send produce frame: %v", err)
		}

		respFrame, err := codec.ReadFrame(conn)
		if err != nil {
			t.Fatalf("failed to read produce response: %v", err)
		}

		corrID, retTopic, retPart, baseOff, errCode := codec.DecodeProduceResponsePayload(respFrame)
		if errCode != 0 {
			t.Fatalf("produce returned error code: %d", errCode)
		}
		if retTopic != topic || retPart != partitionID {
			t.Fatalf("topic/part mismatch: got %s:%d, want %s:%d", retTopic, retPart, topic, partitionID)
		}
		if corrID != int32(b) {
			t.Fatalf("correlationId mismatch: got %d, want %d", corrID, b)
		}
		if baseOff != int64(b*msgsPerBatch) {
			t.Fatalf("unexpected base offset: got %d, want %d", baseOff, b*msgsPerBatch)
		}
	}

	// Consume and verify
	fetchReqBuf := new(bytes.Buffer)
	codec.EncodeFetchHeader(fetchReqBuf, 999)
	codec.AppendFetchPayload(fetchReqBuf, topic, partitionID, 0, 1048576)

	if err := codec.WriteFrame(conn, fetchReqBuf.Bytes()); err != nil {
		t.Fatalf("failed to send fetch request: %v", err)
	}

	fetchRespFrame, err := codec.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read fetch response: %v", err)
	}

	respReader := bytes.NewReader(fetchRespFrame)
	var corrID int32
	_ = codec.DecodeResponseHeader(respReader, &corrID)
	fetchResp, err := codec.DecodeFetchResponseBody(respReader)
	if err != nil {
		t.Fatalf("failed to decode fetch response body: %v", err)
	}

	if fetchResp.ErrorCode != 0 {
		t.Fatalf("fetch error: %d", fetchResp.ErrorCode)
	}
	if len(fetchResp.Records) != totalExpected {
		t.Fatalf("records mismatch: got %d, want %d", len(fetchResp.Records), totalExpected)
	}

	// Verify sequential offsets
	for i, r := range fetchResp.Records {
		if r.Offset != int64(i) {
			t.Fatalf("offset mismatch at index %d: got %d, want %d", i, r.Offset, i)
		}
		expectedKey := fmt.Sprintf("sensor-%d", i)
		if string(r.Key) != expectedKey {
			t.Fatalf("key mismatch at offset %d: got %s, want %s", r.Offset, string(r.Key), expectedKey)
		}
	}

	// Test Tiered Cold Storage Flusher
	part, err := server.GetOrCreatePartition(topic, partitionID)
	if err != nil {
		t.Fatalf("failed to get partition: %v", err)
	}

	mockS3, err := storage.NewLocalDiskBlobStore(coldStorageDir)
	if err != nil {
		t.Fatalf("failed to init cold store: %v", err)
	}

	flusher := storage.NewTieredFlusher(mockS3, part, topic, partitionID)
	// Force a roll by appending larger payload
	largePayload := make([]byte, 2*1024*1024+100) // exceed 2MB segment threshold
	_, err = part.Append([]byte("large-key"), largePayload)
	if err != nil {
		t.Fatalf("failed to write large payload: %v", err)
	}

	flushed, err := flusher.FlushClosedSegments(context.Background())
	if err != nil {
		t.Fatalf("flusher failed: %v", err)
	}
	if flushed < 1 {
		t.Fatalf("expected at least 1 flushed sealed segment to cold tier, got %d", flushed)
	}

	// Verify cold tier object exists
	coldLogKey := fmt.Sprintf("strata-cold/%s/%d/%020d.log", topic, partitionID, 0)
	exists, err := mockS3.Exists(context.Background(), coldLogKey)
	if err != nil || !exists {
		t.Fatalf("cold tier log segment was not found in S3/blob store: %v", err)
	}
}

func BenchmarkPartitionThroughput(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "strata-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := partition.PartitionConfig{
		Dir:             tempDir,
		Topic:           "bench-topic",
		PartitionID:     0,
		MaxSegmentBytes: 64 * 1024 * 1024,
	}

	part, err := partition.OpenPartition(cfg)
	if err != nil {
		b.Fatalf("failed to open partition: %v", err)
	}
	defer part.Close()

	key := []byte("bench-key-throughput")
	payload := make([]byte, 512) // 512-byte payload representative of production events

	b.SetBytes(int64(len(key) + len(payload)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := part.Append(key, payload)
			if err != nil {
				b.Errorf("append failed: %v", err)
			}
		}
	})
}
