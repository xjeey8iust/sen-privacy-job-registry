package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/xjeey8iust/sen-privacy-job-registry/internal/store"
)

// storageUnavailableMessage matches the message published by GET /healthz.
const storageUnavailableMessage = "database is not available"

const (
	codeInvalidRequest     = "invalid_request"
	codeJobConflict        = "job_conflict"
	codeStorageUnavailable = "storage_unavailable"
)

// registerJobFields is the exact set of fields accepted by POST /jobs.
var registerJobFields = map[string]struct{}{
	"id":               {},
	"computation_type": {},
	"participants":     {},
	"input_refs":       {},
}

// registerJobPayload is the strictly validated result of parsing the body of
// POST /jobs.
type registerJobPayload struct {
	id              string
	computationType string
	participants    []string
	inputRefs       []string
}

func registerJobsHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		payload, ok := parseRegisterJob(c)
		if !ok {
			return
		}

		job, created, err := st.RegisterJob(c.Request.Context(), store.JobRegistration{
			ID:              payload.id,
			ComputationType: payload.computationType,
			Participants:    payload.participants,
			InputRefs:       payload.inputRefs,
		})
		if err != nil {
			if _, ok := err.(*store.JobConflictError); ok {
				respondError(c, http.StatusConflict, codeJobConflict, "job is already registered with different fields")
				return
			}
			respondStorageUnavailable(c)
			return
		}

		if created {
			c.JSON(http.StatusCreated, job)
			return
		}
		c.JSON(http.StatusOK, job)
	}
}

func listJobsHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		page, err := parsePage(c.Query("page"), 1)
		if err != nil {
			respondInvalidRequest(c)
			return
		}
		pageSize, err := parsePageSize(c.Query("page_size"), 20)
		if err != nil {
			respondInvalidRequest(c)
			return
		}
		var participant string
		if raw, exists := c.GetQuery("participant"); exists {
			if strings.TrimSpace(raw) == "" {
				respondInvalidRequest(c)
				return
			}
			participant = raw
		}

		items, total, err := st.ListJobs(c.Request.Context(), participant, page, pageSize)
		if err != nil {
			respondStorageUnavailable(c)
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"items":     items,
			"total":     total,
			"page":      page,
			"page_size": pageSize,
		})
	}
}

// parseRegisterJob enforces: a single JSON object, exactly the four known
// fields (no duplicates), non-blank strings for the first two, and arrays of
// at least one non-blank string for the last two. Any failure writes the
// standard error and returns false.
func parseRegisterJob(c *gin.Context) (registerJobPayload, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
	if err != nil {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}
	if len(bytes.TrimSpace(body)) == 0 {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}

	// The body must be exactly one JSON value: a second decode must hit EOF.
	decoder := json.NewDecoder(bytes.NewReader(body))
	var object json.RawMessage
	if err := decoder.Decode(&object); err != nil {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}

	fields, ok := decodeObjectFields(object)
	if !ok {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}

	id, ok := nonBlankString(fields["id"])
	if !ok {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}
	computationType, ok := nonBlankString(fields["computation_type"])
	if !ok {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}
	participants, ok := nonBlankStringArray(fields["participants"])
	if !ok {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}
	inputRefs, ok := nonBlankStringArray(fields["input_refs"])
	if !ok {
		respondInvalidRequest(c)
		return registerJobPayload{}, false
	}

	return registerJobPayload{
		id:              id,
		computationType: computationType,
		participants:    participants,
		inputRefs:       inputRefs,
	}, true
}

// decodeObjectFields decodes a JSON object into its raw field values. It
// rejects non-objects, duplicate keys, unknown fields and missing fields.
func decodeObjectFields(object json.RawMessage) (map[string]json.RawMessage, bool) {
	tokenizer := json.NewDecoder(bytes.NewReader(object))
	opening, err := tokenizer.Token()
	if err != nil {
		return nil, false
	}
	delim, ok := opening.(json.Delim)
	if !ok || delim != '{' {
		return nil, false
	}

	fields := make(map[string]json.RawMessage)
	for tokenizer.More() {
		keyToken, err := tokenizer.Token()
		if err != nil {
			return nil, false
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, false
		}
		if _, known := registerJobFields[key]; !known {
			return nil, false
		}
		if _, duplicated := fields[key]; duplicated {
			return nil, false
		}
		var value json.RawMessage
		if err := tokenizer.Decode(&value); err != nil {
			return nil, false
		}
		fields[key] = value
	}
	if _, err := tokenizer.Token(); err != nil {
		return nil, false
	}

	if len(fields) != len(registerJobFields) {
		return nil, false
	}
	return fields, true
}

// nonBlankString decodes a required field that must be a JSON string holding
// at least one non-whitespace character. A nil raw means the field is absent.
func nonBlankString(raw json.RawMessage) (string, bool) {
	if raw == nil {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	if strings.TrimSpace(value) == "" {
		return "", false
	}
	return value, true
}

// nonBlankStringArray decodes a required field that must be a JSON array
// containing at least one element, each a JSON string holding at least one
// non-whitespace character. Array order is preserved.
func nonBlankStringArray(raw json.RawMessage) ([]string, bool) {
	if raw == nil {
		return nil, false
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, false
	}
	if len(values) == 0 {
		return nil, false
	}
	result := make([]string, len(values))
	for i, element := range values {
		var value string
		if err := json.Unmarshal(element, &value); err != nil {
			return nil, false
		}
		if strings.TrimSpace(value) == "" {
			return nil, false
		}
		result[i] = value
	}
	return result, true
}

// parsePage parses page: an absent value yields the default, otherwise it must
// be a positive decimal integer made of ASCII digits.
func parsePage(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := parseDecimal(raw)
	if err != nil || value < 1 {
		return 0, strconv.ErrSyntax
	}
	return value, nil
}

// parsePageSize parses page_size: an absent value yields the default,
// otherwise it must be a decimal integer between 1 and 100.
func parsePageSize(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := parseDecimal(raw)
	if err != nil || value < 1 || value > 100 {
		return 0, strconv.ErrSyntax
	}
	return value, nil
}

// parseDecimal accepts a non-empty run of ASCII digits and returns its value,
// rejecting signs, spaces, leading '+' and non-integer forms.
func parseDecimal(raw string) (int, error) {
	if raw == "" {
		return 0, strconv.ErrSyntax
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, strconv.ErrSyntax
		}
	}
	return strconv.Atoi(raw)
}

func respondInvalidRequest(c *gin.Context) {
	respondError(c, http.StatusBadRequest, codeInvalidRequest, "request body or parameters are not valid")
}

func respondStorageUnavailable(c *gin.Context) {
	respondError(c, http.StatusServiceUnavailable, codeStorageUnavailable, storageUnavailableMessage)
}

func respondError(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
