package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/xjeey8iust/sen-privacy-job-registry/internal/store"
)

func newTestRouter(t *testing.T) (*gin.Engine, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st), st
}

func serve(router *gin.Engine, method, target, body string) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, reader)
	router.ServeHTTP(recorder, request)
	return recorder
}

func errorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %v (%s)", err, recorder.Body.String())
	}
	if body.Error == nil || body.Error.Code == "" || body.Error.Message == "" {
		t.Fatalf("error shape broken: %s", recorder.Body.String())
	}
	return body.Error.Code
}

const validBody = `{"id":"job-1","computation_type":"mpc-sum","participants":["alice","bob"],"input_refs":["s3://inputs/a"]}`

const validRecordJSON = `{"id":"job-1","computation_type":"mpc-sum","participants":["alice","bob"],"input_refs":["s3://inputs/a"],"stage":"registered","history":[{"stage":"registered"}]}`

func TestHealthzReportsOK(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := serve(router, http.MethodGet, "/healthz", "")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Body.String(); got != `{"database":"ok","status":"ok"}` {
		t.Fatalf("body = %s", got)
	}
}

func TestUnknownRouteUsesPublishedErrorShape(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := serve(router, http.MethodGet, "/missing", "")

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
	if code := errorCode(t, recorder); code != "route_not_found" {
		t.Fatalf("code = %q, want route_not_found", code)
	}
}

func TestRegisterJobReturnsCreatedRecord(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := serve(router, http.MethodPost, "/jobs", validBody)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (%s)", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if got := recorder.Body.String(); got != validRecordJSON {
		t.Fatalf("body = %s, want %s", got, validRecordJSON)
	}
}

func TestRegisterIdenticalJobReturnsOKWithoutGrowingHistory(t *testing.T) {
	router, _ := newTestRouter(t)

	serve(router, http.MethodPost, "/jobs", validBody)
	recorder := serve(router, http.MethodPost, "/jobs", validBody)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Body.String(); got != validRecordJSON {
		t.Fatalf("body = %s, want %s", got, validRecordJSON)
	}
}

func TestRegisterConflictingJobReturns409(t *testing.T) {
	router, _ := newTestRouter(t)

	serve(router, http.MethodPost, "/jobs", validBody)
	recorder := serve(router, http.MethodPost, "/jobs",
		`{"id":"job-1","computation_type":"mpc-sum","participants":["bob","alice"],"input_refs":["s3://inputs/a"]}`)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	if code := errorCode(t, recorder); code != "job_conflict" {
		t.Fatalf("code = %q, want job_conflict", code)
	}

	// The original record must be untouched.
	list := serve(router, http.MethodGet, "/jobs", "")
	if !strings.Contains(list.Body.String(), `"total":1`) || !strings.Contains(list.Body.String(), `"participants":["alice","bob"]`) {
		t.Fatalf("original record changed: %s", list.Body.String())
	}
}

func TestRegisterJobRejectsInvalidBodies(t *testing.T) {
	cases := map[string]string{
		"empty body":             ``,
		"not an object":          `[1,2,3]`,
		"scalar body":            `"job-1"`,
		"trailing garbage":       `{"id":"a","computation_type":"t","participants":["p"],"input_refs":["r"]} junk`,
		"second object":          `{"id":"a","computation_type":"t","participants":["p"],"input_refs":["r"]}{"id":"b"}`,
		"missing id":             `{"computation_type":"t","participants":["p"],"input_refs":["r"]}`,
		"missing computation":    `{"id":"a","participants":["p"],"input_refs":["r"]}`,
		"missing participants":   `{"id":"a","computation_type":"t","input_refs":["r"]}`,
		"missing input_refs":     `{"id":"a","computation_type":"t","participants":["p"]}`,
		"blank id":               `{"id":"  ","computation_type":"t","participants":["p"],"input_refs":["r"]}`,
		"blank computation type": `{"id":"a","computation_type":"\t","participants":["p"],"input_refs":["r"]}`,
		"empty participants":     `{"id":"a","computation_type":"t","participants":[],"input_refs":["r"]}`,
		"empty input_refs":       `{"id":"a","computation_type":"t","participants":["p"],"input_refs":[]}`,
		"blank participant":      `{"id":"a","computation_type":"t","participants":["p"," "],"input_refs":["r"]}`,
		"blank input ref":        `{"id":"a","computation_type":"t","participants":["p"],"input_refs":[""]}`,
		"non-string participant": `{"id":"a","computation_type":"t","participants":[1],"input_refs":["r"]}`,
		"wrong field type":       `{"id":7,"computation_type":"t","participants":["p"],"input_refs":["r"]}`,
		"extra field":            `{"id":"a","computation_type":"t","participants":["p"],"input_refs":["r"],"data":"raw"}`,
		"client-supplied stage":  `{"id":"a","computation_type":"t","participants":["p"],"input_refs":["r"],"stage":"done"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			router, _ := newTestRouter(t)

			recorder := serve(router, http.MethodPost, "/jobs", body)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d (%s)", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if code := errorCode(t, recorder); code != "invalid_request" {
				t.Fatalf("code = %q, want invalid_request", code)
			}

			list := serve(router, http.MethodGet, "/jobs", "")
			if !strings.Contains(list.Body.String(), `"total":0`) {
				t.Fatalf("invalid body was stored: %s", list.Body.String())
			}
		})
	}
}

func registerJobs(t *testing.T, router *gin.Engine, bodies ...string) {
	t.Helper()
	for _, body := range bodies {
		if recorder := serve(router, http.MethodPost, "/jobs", body); recorder.Code != http.StatusCreated {
			t.Fatalf("register %s: status = %d (%s)", body, recorder.Code, recorder.Body.String())
		}
	}
}

type listResponse struct {
	Items    []json.RawMessage `json:"items"`
	Total    int               `json:"total"`
	Page     int               `json:"page"`
	PageSize int               `json:"page_size"`
}

func getJobs(t *testing.T, router *gin.Engine, target string) listResponse {
	t.Helper()
	recorder := serve(router, http.MethodGet, target, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET %s: status = %d (%s)", target, recorder.Code, recorder.Body.String())
	}
	var response listResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("GET %s: decode: %v", target, err)
	}
	return response
}

func TestListJobsPaginatesAndFilters(t *testing.T) {
	router, _ := newTestRouter(t)
	registerJobs(t, router,
		`{"id":"job-c","computation_type":"t","participants":["bob"],"input_refs":["r"]}`,
		`{"id":"job-a","computation_type":"t","participants":["alice","bob"],"input_refs":["r"]}`,
		`{"id":"job-b","computation_type":"t","participants":["carol"],"input_refs":["r"]}`,
	)

	first := getJobs(t, router, "/jobs")
	if first.Total != 3 || first.Page != 1 || first.PageSize != 20 || len(first.Items) != 3 {
		t.Fatalf("default page = %+v", first)
	}
	var ids []string
	for _, raw := range first.Items {
		var item struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			t.Fatalf("decode item: %v", err)
		}
		ids = append(ids, item.ID)
	}
	if strings.Join(ids, ",") != "job-a,job-b,job-c" {
		t.Fatalf("order = %v, want id ascending", ids)
	}
	// Items carry the same shape as the registration response.
	var firstItem struct {
		Stage   string `json:"stage"`
		History []struct {
			Stage string `json:"stage"`
		} `json:"history"`
	}
	if err := json.Unmarshal(first.Items[0], &firstItem); err != nil {
		t.Fatalf("decode item: %v", err)
	}
	if firstItem.Stage != "registered" || len(firstItem.History) != 1 || firstItem.History[0].Stage != "registered" {
		t.Fatalf("item record = %s", first.Items[0])
	}

	second := getJobs(t, router, "/jobs?page=2&page_size=2")
	if second.Total != 3 || second.Page != 2 || second.PageSize != 2 || len(second.Items) != 1 {
		t.Fatalf("page 2 = %+v", second)
	}

	beyond := getJobs(t, router, "/jobs?page=9&page_size=5")
	if beyond.Total != 3 || len(beyond.Items) != 0 {
		t.Fatalf("out-of-range page = %+v", beyond)
	}

	filtered := getJobs(t, router, "/jobs?participant=bob")
	if filtered.Total != 2 || len(filtered.Items) != 2 {
		t.Fatalf("participant=bob = %+v", filtered)
	}

	none := getJobs(t, router, "/jobs?participant=dave")
	if none.Total != 0 || len(none.Items) != 0 {
		t.Fatalf("participant=dave = %+v", none)
	}
}

func TestListJobsEmptyResultIsAnArray(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := serve(router, http.MethodGet, "/jobs", "")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), `"items":[]`) {
		t.Fatalf("items must be an empty array, got %s", recorder.Body.String())
	}
}

func TestListJobsRejectsInvalidParams(t *testing.T) {
	targets := []string{
		"/jobs?page=0",
		"/jobs?page=-1",
		"/jobs?page=abc",
		"/jobs?page=1.5",
		"/jobs?page=",
		"/jobs?page_size=0",
		"/jobs?page_size=101",
		"/jobs?page_size=-5",
		"/jobs?page_size=two",
		"/jobs?page_size=",
		"/jobs?participant=",
		"/jobs?participant=%20%20",
	}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			router, _ := newTestRouter(t)

			recorder := serve(router, http.MethodGet, target, "")

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d (%s)", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if code := errorCode(t, recorder); code != "invalid_request" {
				t.Fatalf("code = %q, want invalid_request", code)
			}
		})
	}
}

func TestStorageFailureReturns503WithHealthzMessage(t *testing.T) {
	router, st := newTestRouter(t)
	st.Close()

	for _, target := range []struct{ method, path, body string }{
		{http.MethodPost, "/jobs", validBody},
		{http.MethodGet, "/jobs", ""},
	} {
		recorder := serve(router, target.method, target.path, target.body)
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s: status = %d, want %d", target.method, target.path, recorder.Code, http.StatusServiceUnavailable)
		}
		want := `{"error":{"code":"storage_unavailable","message":"database is not available"}}`
		if got := recorder.Body.String(); got != want {
			t.Fatalf("%s %s: body = %s, want %s", target.method, target.path, got, want)
		}
	}
}
