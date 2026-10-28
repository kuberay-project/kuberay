package support

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Based on https://github.com/apache/incubator-kie-kogito-operator/blob/28b2d3dc945e48659b199cca33723568b848f72e/test/pkg/framework/logging.go

const (
	eventLastSeenKey  = "LAST_SEEN"
	eventFirstSeenKey = "FIRST_SEEN"
	eventNameKey      = "NAME"
	eventSubObjectKey = "SUBOBJECT"
	eventTypeKey      = "TYPE"
	eventReasonKey    = "REASON"
	eventMessageKey   = "MESSAGE"

	eventLogFileName = "events"
)

var eventKeys = []string{
	eventLastSeenKey,
	eventFirstSeenKey,
	eventNameKey,
	eventSubObjectKey,
	eventTypeKey,
	eventReasonKey,
	eventMessageKey,
}

func storeEvents(t Test, namespace *corev1.Namespace) {
	t.T().Helper()

	events, err := t.Client().Core().EventsV1().Events(namespace.Name).List(t.Ctx(), metav1.ListOptions{})
	require.NoError(t.T(), err)

	eventContent, err := renderEventContent(eventKeys, mapEventsToKeys(events))
	require.NoError(t.T(), err)

	WriteToOutputDir(t, eventLogFileName, Log, eventContent)
}

func mapEventsToKeys(eventList *eventsv1.EventList) []map[string]string {
	eventMaps := []map[string]string{}

	for _, event := range eventList.Items {
		eventMap := make(map[string]string)
		eventMap[eventLastSeenKey] = getDefaultEventValueIfNull(event.DeprecatedLastTimestamp.Format("2006-01-02 15:04:05"))
		eventMap[eventFirstSeenKey] = getDefaultEventValueIfNull(event.DeprecatedFirstTimestamp.Format("2006-01-02 15:04:05"))
		eventMap[eventNameKey] = getDefaultEventValueIfNull(event.GetName())
		eventMap[eventSubObjectKey] = getDefaultEventValueIfNull(event.Regarding.FieldPath)
		eventMap[eventTypeKey] = getDefaultEventValueIfNull(event.Type)
		eventMap[eventReasonKey] = getDefaultEventValueIfNull(event.Reason)
		eventMap[eventMessageKey] = getDefaultEventValueIfNull(event.Note)

		eventMaps = append(eventMaps, eventMap)
	}
	return eventMaps
}

func getDefaultEventValueIfNull(value string) string {
	if len(value) <= 0 {
		return "-"
	}
	return value
}

func renderEventContent(keys []string, dataMaps []map[string]string) ([]byte, error) {
	var content bytes.Buffer
	// Get size of strings to be written, to be able to format correctly
	maxStringSizeMap := make(map[string]int)
	for _, key := range keys {
		maxSize := len(key)
		for _, dataMap := range dataMaps {
			if len(dataMap[key]) > maxSize {
				maxSize = len(dataMap[key])
			}
		}
		maxStringSizeMap[key] = maxSize
	}

	// Write headers
	for _, header := range keys {
		if _, err := content.WriteString(header); err != nil {
			return nil, fmt.Errorf("error in writing the header: %w", err)
		}
		if _, err := content.WriteString(getWhitespaceStr(maxStringSizeMap[header] - len(header) + 1)); err != nil {
			return nil, fmt.Errorf("error in writing headers: %w", err)
		}
		if _, err := content.WriteString(" | "); err != nil {
			return nil, fmt.Errorf("error in writing headers : %w", err)
		}
	}
	if _, err := content.WriteString("\n"); err != nil {
		return nil, fmt.Errorf("error in writing headers '|': %w", err)
	}

	// Write events
	for _, dataMap := range dataMaps {
		for _, key := range keys {
			if _, err := content.WriteString(dataMap[key]); err != nil {
				return nil, fmt.Errorf("error in writing events: %w", err)
			}
			if _, err := content.WriteString(getWhitespaceStr(maxStringSizeMap[key] - len(dataMap[key]) + 1)); err != nil {
				return nil, fmt.Errorf("error in writing events: %w", err)
			}
			if _, err := content.WriteString(" | "); err != nil {
				return nil, fmt.Errorf("error in writing events: %w", err)
			}
		}
		if _, err := content.WriteString("\n"); err != nil {
			return nil, fmt.Errorf("error in writing events: %w", err)
		}
	}
	return content.Bytes(), nil
}

func getWhitespaceStr(size int) string {
	var whiteSpaceStr strings.Builder
	for range size {
		whiteSpaceStr.WriteString(" ")
	}
	return whiteSpaceStr.String()
}
