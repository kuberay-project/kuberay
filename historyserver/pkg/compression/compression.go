package compression

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

// CompressStream stream data in fixed chunks from src to dst through gzip,
// ensuring constant memory usage regardless of payload size.
func CompressStream(dst io.Writer, src io.Reader) error {
	w := gzip.NewWriter(dst)
	if _, err := io.Copy(w, src); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// DecompressStream stream data in fixed chunks from src to dst through gzip.
func DecompressStream(dst io.Writer, src io.Reader) error {
	r, err := gzip.NewReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			logrus.Warnf("compression: failed to close gzip reader: %v", err)
		}
	}()

	_, err = io.Copy(dst, r)
	return err
}

// StorageWriter defines the duck-typed interface for storage handlers to avoid cyclic dependencies
type StorageWriter interface {
	WriteFile(file string, reader io.ReadSeeker) error
}

// WriteCompressedFile compresses a local file on the fly and uploads it to any active cloud provider.
// It utilizes disk-based spooling to maintain constant memory usage (zero RAM expansion) regardless
// of file size.
func WriteCompressedFile(writer StorageWriter, remotePath string, localFilePath string) error {
	// 1. Opens the uncompressed local file on disk.
	rawFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("compression: failed to open local file %q: %w", localFilePath, err)
	}
	defer func() {
		if err := rawFile.Close(); err != nil {
			logrus.Warnf("compression: failed to close local file %q: %v", localFilePath, err)
		}
	}()

	// 2. Spools compressed output into an ephemeral temporary file via CompressStream.
	tmpFile, err := os.CreateTemp("", "upload-*.gz")
	if err != nil {
		return fmt.Errorf("compression: failed to create temp file: %w", err)
	}
	defer func() {
		if err := tmpFile.Close(); err != nil {
			logrus.Warnf("compression: failed to close temp file %q: %v", tmpFile.Name(), err)
		}
		if err := os.Remove(tmpFile.Name()); err != nil {
			logrus.Warnf("compression: failed to remove temp file %q: %v", tmpFile.Name(), err)
		}
	}()

	if err := CompressStream(tmpFile, rawFile); err != nil {
		return fmt.Errorf("compression: stream compression failed: %w", err)
	}

	// 3. Seeks the temp file to the beginning to satisfy io.ReadSeeker, which is required by cloud SDKs.
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("compression: failed to seek temp file: %w", err)
	}

	// 4. Delegates the upload to the duck-typed StorageWriter.
	if err := writer.WriteFile(remotePath, tmpFile); err != nil {
		return fmt.Errorf("compression: failed to upload compressed file: %w", err)
	}

	return nil
}

// WriteCompressedBytes compresses an in-memory byte slice and uploads it to any active cloud provider.
func WriteCompressedBytes(writer StorageWriter, remotePath string, data []byte) error {
	var buf bytes.Buffer
	if err := CompressStream(&buf, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("compression: in-memory compression failed: %w", err)
	}

	if err := writer.WriteFile(remotePath, bytes.NewReader(buf.Bytes())); err != nil {
		return fmt.Errorf("compression: failed to upload compressed bytes: %w", err)
	}

	return nil
}

// StorageContentReader defines the duck-typed interface for storage readers to avoid cyclic dependencies.
type StorageContentReader interface {
	GetContent(prefix string, fileName string) io.Reader
}

// gzipReadCloser wraps a *gzip.Reader and the underlying reader so both are closed properly.
type gzipReadCloser struct {
	gzipReader *gzip.Reader
	underlying io.Reader
}

func (g *gzipReadCloser) Read(p []byte) (n int, err error) {
	return g.gzipReader.Read(p)
}

func (g *gzipReadCloser) Close() error {
	var errs []error

	if err := g.gzipReader.Close(); err != nil {
		errs = append(errs, fmt.Errorf("gzip close: %w", err))
	}

	if closer, ok := g.underlying.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("source close: %w", err))
		}
	}

	return errors.Join(errs...)
}

// ReadCompressedContent reads a gzip-compressed file from cloud storage and returns an io.ReadCloser.
func ReadCompressedContent(reader StorageContentReader, prefix string, fileName string) (io.ReadCloser, error) {
	contentReader := reader.GetContent(prefix, fileName)
	if contentReader == nil {
		return nil, errors.New("compression: file not found or nil reader returned by storage")
	}

	r, err := gzip.NewReader(contentReader)
	if err != nil {
		if closer, ok := contentReader.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				logrus.Warnf("compression: failed to close underlying stream after gzip reader creation error: %v", err)
			}
		}
		return nil, fmt.Errorf("compression: failed to create gzip reader: %w", err)
	}

	return &gzipReadCloser{
		gzipReader: r,
		underlying: contentReader,
	}, nil
}
