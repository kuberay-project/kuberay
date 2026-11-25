package compression

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func compressBytesForTest(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := CompressStream(&buf, bytes.NewReader(data))
	return buf.Bytes(), err
}

func decompressBytesForTest(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := DecompressStream(&buf, bytes.NewReader(data))
	return buf.Bytes(), err
}

func TestCompressDecompressStream(t *testing.T) {
	originalData := []byte("Streaming log compression payload testing across io.Reader and io.Writer")

	var compressedBuf bytes.Buffer
	if err := CompressStream(&compressedBuf, bytes.NewReader(originalData)); err != nil {
		t.Fatalf("CompressStream failed: %v", err)
	}

	var decompressedBuf bytes.Buffer
	if err := DecompressStream(&decompressedBuf, &compressedBuf); err != nil {
		t.Fatalf("DecompressStream failed: %v", err)
	}

	if !bytes.Equal(originalData, decompressedBuf.Bytes()) {
		t.Fatalf("Stream decompressed data mismatch.\nExpected: %s\nGot: %s", originalData, decompressedBuf.Bytes())
	}
}

type mockStorageWriter struct {
	writtenData []byte
}

func (m *mockStorageWriter) WriteFile(_ string, reader io.ReadSeeker) error {
	var err error
	m.writtenData, err = io.ReadAll(reader)
	return err
}

func TestWriteCompressedFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-raw-*.txt")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	rawData := []byte("upload compression payload test")
	if _, err := tmpFile.Write(rawData); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	tmpFile.Close()

	mockWriter := &mockStorageWriter{}
	if err := WriteCompressedFile(mockWriter, "test/remote.gz", tmpFile.Name()); err != nil {
		t.Fatalf("WriteCompressedFile failed: %v", err)
	}

	decompressed, err := decompressBytesForTest(mockWriter.writtenData)
	if err != nil {
		t.Fatalf("decompressBytesForTest failed: %v", err)
	}

	if !bytes.Equal(rawData, decompressed) {
		t.Fatalf("Decompressed written data mismatch.\nExpected: %s\nGot: %s", rawData, decompressed)
	}
}

func TestWriteCompressedBytes(t *testing.T) {
	rawData := []byte("upload in-memory bytes payload test")
	mockWriter := &mockStorageWriter{}

	if err := WriteCompressedBytes(mockWriter, "test/bytes.gz", rawData); err != nil {
		t.Fatalf("WriteCompressedBytes failed: %v", err)
	}

	decompressed, err := decompressBytesForTest(mockWriter.writtenData)
	if err != nil {
		t.Fatalf("decompressBytesForTest failed: %v", err)
	}

	if !bytes.Equal(rawData, decompressed) {
		t.Fatalf("Decompressed written bytes mismatch.\nExpected: %s\nGot: %s", rawData, decompressed)
	}
}

type mockStorageReader struct {
	content []byte
}

func (m *mockStorageReader) GetContent(_, _ string) io.Reader {
	if m.content == nil {
		return nil
	}
	return bytes.NewReader(m.content)
}

func TestReadCompressedContent(t *testing.T) {
	originalText := []byte("Universal cloud storage reader decompression test")
	compressed, err := compressBytesForTest(originalText)
	if err != nil {
		t.Fatalf("compressBytesForTest failed: %v", err)
	}

	mockReader := &mockStorageReader{content: compressed}
	rc, err := ReadCompressedContent(mockReader, "cluster-1", "log.gz")
	if err != nil {
		t.Fatalf("ReadCompressedContent failed: %v", err)
	}
	defer rc.Close()

	decompressed, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("io.ReadAll failed: %v", err)
	}

	if !bytes.Equal(originalText, decompressed) {
		t.Fatalf("Decompressed read content mismatch.\nExpected: %s\nGot: %s", originalText, decompressed)
	}
}
