package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/xjeey8iust/sen-privacy-job-registry/internal/store"
)

const invalidRequestMessage = "request does not match the published contract"

// registerRequest mirrors the registration body. Pointer fields distinguish a missing
// field from a present one, and DisallowUnknownFields rejects anything beyond these four.
type registerRequest struct {
	ID              *string   `json:"id"`
	ComputationType *string   `json:"computation_type"`
	Participants    *[]string `json:"participants"`
	InputRefs       *[]string `json:"input_refs"`
}

// toJob validates the decoded body: required fields present, strings non-blank, arrays
// holding at least one non-blank string. Values are kept verbatim, order included.
func (r *registerRequest) toJob() (store.Job, bool) {
	if r.ID == nil || r.ComputationType == nil || r.Participants == nil || r.InputRefs == nil {
		return store.Job{}, false
	}
	if strings.TrimSpace(*r.ID) == "" || strings.TrimSpace(*r.ComputationType) == "" {
		return store.Job{}, false
	}
	if len(*r.Participants) == 0 || len(*r.InputRefs) == 0 {
		return store.Job{}, false
	}
	for _, participant := range *r.Participants {
		if strings.TrimSpace(participant) == "" {
			return store.Job{}, false
		}
	}
	for _, ref := range *r.InputRefs {
		if strings.TrimSpace(ref) == "" {
			return store.Job{}, false
		}
	}
	return store.Job{
		ID:              *r.ID,
		ComputationType: *r.ComputationType,
		Participants:    *r.Participants,
		InputRefs:       *r.InputRefs,
	}, true
}

func registerJob(c *gin.Context, st *store.Store) {
	var req registerRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", invalidRequestMessage)
		return
	}
	// A second JSON value after the object means the body was not a single object.
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(c, http.StatusBadRequest, "invalid_request", invalidRequestMessage)
		return
	}
	job, ok := req.toJob()
	if !ok {
		writeError(c, http.StatusBadRequest, "invalid_request", invalidRequestMessage)
		return
	}

	stored, created, err := st.RegisterJob(job)
	if err != nil {
		var conflict *store.JobConflictError
		if errors.As(err, &conflict) {
			writeError(c, http.StatusConflict, "job_conflict", "a different job with this id is already registered")
			return
		}
		writeError(c, http.StatusServiceUnavailable, "storage_unavailable", storageUnavailableMessage)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	c.JSON(status, newJobResponse(stored))
}

func listJobs(c *gin.Context, st *store.Store) {
	page, ok := parseDecimalParam(c, "page", 1)
	if !ok || page < 1 {
		writeError(c, http.StatusBadRequest, "invalid_request", invalidRequestMessage)
		return
	}
	pageSize, ok := parseDecimalParam(c, "page_size", 20)
	if !ok || pageSize < 1 || pageSize > 100 {
		writeError(c, http.StatusBadRequest, "invalid_request", invalidRequestMessage)
		return
	}
	participant, filtered := c.GetQuery("participant")
	if filtered && strings.TrimSpace(participant) == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", invalidRequestMessage)
		return
	}

	jobs, total, err := st.ListJobs(participant, page, pageSize)
	if err != nil {
		writeError(c, http.StatusServiceUnavailable, "storage_unavailable", storageUnavailableMessage)
		return
	}

	items := make([]jobResponse, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, newJobResponse(job))
	}
	c.JSON(http.StatusOK, gin.H{
		"items":     items,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// parseDecimalParam reads an optional query parameter as a decimal integer, rejecting
// empty values, signs, decimals and anything else that is not plain digits.
func parseDecimalParam(c *gin.Context, name string, fallback int) (int, bool) {
	raw, present := c.GetQuery(name)
	if !present {
		return fallback, true
	}
	if raw == "" {
		return 0, false
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return value, true
}

type historyEntryResponse struct {
	Stage string `json:"stage"`
}

// jobResponse is the record shape shared by the registration and listing responses.
type jobResponse struct {
	ID              string                 `json:"id"`
	ComputationType string                 `json:"computation_type"`
	Participants    []string               `json:"participants"`
	InputRefs       []string               `json:"input_refs"`
	Stage           string                 `json:"stage"`
	History         []historyEntryResponse `json:"history"`
}

func newJobResponse(job store.Job) jobResponse {
	history := make([]historyEntryResponse, 0, len(job.History))
	for _, entry := range job.History {
		history = append(history, historyEntryResponse{Stage: entry.Stage})
	}
	return jobResponse{
		ID:              job.ID,
		ComputationType: job.ComputationType,
		Participants:    job.Participants,
		InputRefs:       job.InputRefs,
		Stage:           job.Stage,
		History:         history,
	}
}
