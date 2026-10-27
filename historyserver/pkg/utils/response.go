package utils

import (
	"io"

	"github.com/sirupsen/logrus"
)

// LogResponseWriteError logs err if writing an HTTP response failed.
// A handler cannot report a failed write to the client (it usually means the
// client has already gone away), so logging it is all that is left to do.
func LogResponseWriteError(err error) {
	if err != nil {
		logrus.Errorf("Failed to write HTTP response: %v", err)
	}
}

// WriteResponse writes data to w and logs any write error.
func WriteResponse(w io.Writer, data []byte) {
	_, err := w.Write(data)
	LogResponseWriteError(err)
}
