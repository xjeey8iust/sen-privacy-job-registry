package api

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/xjeey8iust/sen-privacy-job-registry/internal/store"
)

const validJobBody = `{"id":"job-1","computation_type":"mpc","participants":["alice","bob"],"input_refs":["r1","r2"]}`

const createdJobJSON = `{"id":"job-1","computation_type":"mpc","participants":["alice","bob"],"input_refs":["r1","r2"],"stage":"registered","history":[{"stage":"registered"}]}`

func newTestRouter(t *testing.T) (*store.Store, http.Handler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, NewRouter(st)
}

func postJobs(handler http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func getJobs(handler http.Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestPostJobCreatesWith201(t *testing.T) {
	_, handler := newTestRouter(t)

	rec := postJobs(handler, validJobBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if got := rec.Body.String(); got != createdJobJSON {
		t.Fatalf("body = %s", got)
	}
}

func TestPostJobIdenticalRepeatReturns200WithoutHistoryChange(t *testing.T) {
	_, handler := newTestRouter(t)

	first := postJobs(handler, validJobBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d", first.Code)
	}
	second := postJobs(handler, validJobBody)
	if second.Code != http.StatusOK {
		t.Fatalf("repeat status = %d, want 200", second.Code)
	}
	if got := second.Body.String(); got != createdJobJSON {
		t.Fatalf("repeat body = %s", got)
	}

	list := getJobs(handler, "/jobs")
	var envelope struct {
		Total int `json:"total"`
		Items []struct {
			History []struct {
				Stage string `json:"stage"`
			} `json:"history"`
		} `json:"items"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if envelope.Total != 1 || len(envelope.Items) != 1 || len(envelope.Items[0].History) != 1 {
		t.Fatalf("repeat registration changed stored state: %s", list.Body.String())
	}
}

func TestPostJobConflictReturns409(t *testing.T) {
	_, handler := newTestRouter(t)

	if rec := postJobs(handler, validJobBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup status = %d", rec.Code)
	}
	conflict := `{"id":"job-1","computation_type":"fhe","participants":["alice","bob"],"input_refs":["r1","r2"]}`
	rec := postJobs(handler, conflict)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	assertErrorCode(t, rec, "job_conflict")

	// Existing record stays readable with the original values.
	list := getJobs(handler, "/jobs")
	var envelope struct {
		Items []struct {
			ComputationType string `json:"computation_type"`
		} `json:"items"`
	}
	json.Unmarshal(list.Body.Bytes(), &envelope)
	if len(envelope.Items) != 1 || envelope.Items[0].ComputationType != "mpc" {
		t.Fatalf("existing record overwritten: %s", list.Body.String())
	}
}

func TestPostJobInvalidBodiesReturn400AndWriteNothing(t *testing.T) {
	_, handler := newTestRouter(t)

	cases := map[string]string{
		"missing field":          `{"id":"job-1","computation_type":"mpc","participants":["alice"]}`,
		"id wrong type":          `{"id":1,"computation_type":"mpc","participants":["alice"],"input_refs":["r"]}`,
		"id blank":               `{"id":"  ","computation_type":"mpc","participants":["alice"],"input_refs":["r"]}`,
		"computation type blank": `{"id":"job-1","computation_type":"\t","participants":["alice"],"input_refs":["r"]}`,
		"participants empty":     `{"id":"job-1","computation_type":"mpc","participants":[],"input_refs":["r"]}`,
		"input refs empty":       `{"id":"job-1","computation_type":"mpc","participants":["alice"],"input_refs":[]}`,
		"participant blank item": `{"id":"job-1","computation_type":"mpc","participants":[" "],"input_refs":["r"]}`,
		"participant non-string": `{"id":"job-1","computation_type":"mpc","participants":["alice",2],"input_refs":["r"]}`,
		"participants not array": `{"id":"job-1","computation_type":"mpc","participants":"alice","input_refs":["r"]}`,
		"extra field":            `{"id":"job-1","computation_type":"mpc","participants":["alice"],"input_refs":["r"],"stage":"done"}`,
		"client stage field":     `{"id":"job-1","computation_type":"mpc","participants":["alice"],"input_refs":["r"],"history":[]}`,
		"duplicate key":          `{"id":"job-1","id":"job-2","computation_type":"mpc","participants":["alice"],"input_refs":["r"]}`,
		"empty object":           `{}`,
		"array body":             `[{"id":"job-1","computation_type":"mpc","participants":["alice"],"input_refs":["r"]}]`,
		"scalar body":            `"job-1"`,
		"null body":              `null`,
		"two objects":            `{"id":"job-1","computation_type":"mpc","participants":["alice"],"input_refs":["r"]} {"id":"job-2","computation_type":"mpc","participants":["alice"],"input_refs":["r"]}`,
		"trailing garbage":       `{"id":"job-1","computation_type":"mpc","participants":["alice"],"input_refs":["r"]} garbage`,
		"empty body":             ``,
		"unparseable":            `{not json`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rec := postJobs(handler, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", rec.Code, rec.Body.String())
			}
			assertErrorCode(t, rec, "invalid_request")
		})
	}

	list := getJobs(handler, "/jobs")
	var envelope struct {
		Total int `json:"total"`
	}
	json.Unmarshal(list.Body.Bytes(), &envelope)
	if envelope.Total != 0 {
		t.Fatalf("invalid requests wrote data: %s", list.Body.String())
	}
}

func TestGetJobsReturnsEnvelopeAndDefaults(t *testing.T) {
	_, handler := newTestRouter(t)

	rec := getJobs(handler, "/jobs")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Body.String(); got != `{"items":[],"page":1,"page_size":20,"total":0}` {
		t.Fatalf("empty envelope = %s", rec.Body.String())
	}

	if rec := postJobs(handler, validJobBody); rec.Code != http.StatusCreated {
		t.Fatalf("setup: %d", rec.Code)
	}
	rec = getJobs(handler, "/jobs")
	if got := rec.Body.String(); got != `{"items":[`+createdJobJSON+`],"page":1,"page_size":20,"total":1}` {
		t.Fatalf("envelope = %s", got)
	}
}

func TestGetJobsOrdersByUTF8Bytes(t *testing.T) {
	_, handler := newTestRouter(t)

	for _, body := range []string{
		`{"id":"z","computation_type":"c","participants":["p"],"input_refs":["r"]}`,
		`{"id":"A","computation_type":"c","participants":["p"],"input_refs":["r"]}`,
		`{"id":"中","computation_type":"c","participants":["p"],"input_refs":["r"]}`,
	} {
		if rec := postJobs(handler, body); rec.Code != http.StatusCreated {
			t.Fatalf("setup: %d %s", rec.Code, rec.Body.String())
		}
	}

	rec := getJobs(handler, "/jobs")
	var envelope struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	json.Unmarshal(rec.Body.Bytes(), &envelope)
	got := []string{envelope.Items[0].ID, envelope.Items[1].ID, envelope.Items[2].ID}
	want := []string{"A", "z", "中"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestGetJobsPaginationAndOutOfRange(t *testing.T) {
	_, handler := newTestRouter(t)

	for i := 0; i < 3; i++ {
		body := `{"id":"job-` + string(rune('a'+i)) + `","computation_type":"c","participants":["p"],"input_refs":["r"]}`
		if rec := postJobs(handler, body); rec.Code != http.StatusCreated {
			t.Fatalf("setup: %d", rec.Code)
		}
	}

	rec := getJobs(handler, "/jobs?page=2&page_size=2")
	var envelope struct {
		Total int `json:"total"`
		Page  int `json:"page"`
		Size  int `json:"page_size"`
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	json.Unmarshal(rec.Body.Bytes(), &envelope)
	if envelope.Total != 3 || envelope.Page != 2 || envelope.Size != 2 || len(envelope.Items) != 1 || envelope.Items[0].ID != "job-c" {
		t.Fatalf("page 2 = %s", rec.Body.String())
	}

	rec = getJobs(handler, "/jobs?page=99")
	json.Unmarshal(rec.Body.Bytes(), &envelope)
	if len(envelope.Items) != 0 {
		t.Fatalf("out-of-range page = %s, want empty items", rec.Body.String())
	}
}

func TestGetJobsAcceptsHugePageNumbers(t *testing.T) {
	_, handler := newTestRouter(t)

	// Empty database: a legal huge page is an empty page with total 0.
	hugePage := strconv.Itoa(math.MaxInt)
	rec := getJobs(handler, "/jobs?page="+hugePage+"&page_size=2")
	assertPageEnvelope(t, rec, math.MaxInt, 2, 0, 0)

	for i := 0; i < 3; i++ {
		body := `{"id":"job-` + string(rune('a'+i)) + `","computation_type":"c","participants":["alice"],"input_refs":["r"]}`
		if rec := postJobs(handler, body); rec.Code != http.StatusCreated {
			t.Fatalf("setup: %d %s", rec.Code, rec.Body.String())
		}
	}

	// The largest legal page number must echo back unchanged and return an
	// empty page, never wrap to page 1 or fail, for every page size.
	for _, size := range []int{1, 2, 100} {
		target := "/jobs?page=" + hugePage + "&page_size=" + strconv.Itoa(size)
		rec := getJobs(handler, target)
		assertPageEnvelope(t, rec, math.MaxInt, size, 3, 0)
		if !strings.Contains(rec.Body.String(), `"items":[]`) {
			t.Fatalf("%s: items must serialize as [], body %s", target, rec.Body.String())
		}
	}

	// The participant filter still applies to the reported total.
	rec = getJobs(handler, "/jobs?participant=alice&page="+hugePage+"&page_size=2")
	assertPageEnvelope(t, rec, math.MaxInt, 2, 3, 0)
	rec = getJobs(handler, "/jobs?participant=nobody&page="+hugePage+"&page_size=2")
	assertPageEnvelope(t, rec, math.MaxInt, 2, 0, 0)

	// The ordinary last page still holds its record and the page right after
	// it is empty.
	rec = getJobs(handler, "/jobs?page=2&page_size=2")
	assertPageEnvelope(t, rec, 2, 2, 3, 1)
	rec = getJobs(handler, "/jobs?page=3&page_size=2")
	assertPageEnvelope(t, rec, 3, 2, 3, 0)

	// Leading zeros are accepted and echoed back as the parsed value.
	rec = getJobs(handler, "/jobs?page=0002&page_size=02")
	assertPageEnvelope(t, rec, 2, 2, 3, 1)

	// None of the queries above changed stored content, stage or history.
	rec = getJobs(handler, "/jobs")
	var envelope struct {
		Total int `json:"total"`
		Items []struct {
			ID      string `json:"id"`
			Stage   string `json:"stage"`
			History []struct {
				Stage string `json:"stage"`
			} `json:"history"`
			Participants []string `json:"participants"`
			InputRefs    []string `json:"input_refs"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode final list: %v", err)
	}
	if envelope.Total != 3 || len(envelope.Items) != 3 {
		t.Fatalf("final list = %s", rec.Body.String())
	}
	for i, want := range []string{"job-a", "job-b", "job-c"} {
		item := envelope.Items[i]
		if item.ID != want || item.Stage != "registered" ||
			len(item.History) != 1 || item.History[0].Stage != "registered" ||
			len(item.Participants) != 1 || item.Participants[0] != "alice" ||
			len(item.InputRefs) != 1 || item.InputRefs[0] != "r" {
			t.Fatalf("item %d changed after paging queries: %s", i, rec.Body.String())
		}
	}
}

// assertPageEnvelope verifies a 200 GET /jobs response: the echoed page and
// page_size, the filtered total and the number of returned items.
func assertPageEnvelope(t *testing.T, rec *httptest.ResponseRecorder, page, pageSize, total, items int) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	var envelope struct {
		Total int               `json:"total"`
		Page  int               `json:"page"`
		Size  int               `json:"page_size"`
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope %q: %v", rec.Body.String(), err)
	}
	if envelope.Page != page || envelope.Size != pageSize || envelope.Total != total || len(envelope.Items) != items {
		t.Fatalf("envelope = %s, want page=%d page_size=%d total=%d items=%d",
			rec.Body.String(), page, pageSize, total, items)
	}
}

func TestGetJobsRejectsInvalidPaging(t *testing.T) {
	_, handler := newTestRouter(t)

	for _, target := range []string{
		"/jobs?page=0",
		"/jobs?page=-1",
		"/jobs?page=1.5",
		"/jobs?page=abc",
		"/jobs?page=1%20",
		"/jobs?page=%2b1",
		"/jobs?page_size=0",
		"/jobs?page_size=101",
		"/jobs?page_size=x",
		"/jobs?page_size=2.0",
	} {
		rec := getJobs(handler, target)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", target, rec.Code)
		}
		assertErrorCode(t, rec, "invalid_request")
	}
}

func TestGetJobsFiltersByExactParticipant(t *testing.T) {
	_, handler := newTestRouter(t)

	postJobs(handler, `{"id":"j1","computation_type":"c","participants":["alice","bob"],"input_refs":["r"]}`)
	postJobs(handler, `{"id":"j2","computation_type":"c","participants":["carol"],"input_refs":["r"]}`)

	rec := getJobs(handler, "/jobs?participant=alice")
	var envelope struct {
		Total int `json:"total"`
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	json.Unmarshal(rec.Body.Bytes(), &envelope)
	if envelope.Total != 1 || len(envelope.Items) != 1 || envelope.Items[0].ID != "j1" {
		t.Fatalf("filter = %s", rec.Body.String())
	}

	for _, target := range []string{"/jobs?participant=", "/jobs?participant=%20%20"} {
		rec := getJobs(handler, target)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", target, rec.Code)
		}
		assertErrorCode(t, rec, "invalid_request")
	}
}

func TestJobsEndpointsReturn503WhenStorageDown(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	post := postJobs(handler, validJobBody)
	if post.Code != http.StatusServiceUnavailable {
		t.Fatalf("post status = %d, want 503", post.Code)
	}
	assertErrorCode(t, post, "storage_unavailable")

	get := getJobs(handler, "/jobs")
	if get.Code != http.StatusServiceUnavailable {
		t.Fatalf("get status = %d, want 503", get.Code)
	}
	assertErrorCode(t, get, "storage_unavailable")
}

func TestGetJobsStorageDownAndParameterPriority(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A legal huge page is still a legal query: storage failure wins.
	rec := getJobs(handler, "/jobs?page="+strconv.Itoa(math.MaxInt)+"&page_size=2")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("huge page status = %d, want 503", rec.Code)
	}
	assertErrorCode(t, rec, "storage_unavailable")
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode 503 body: %v", err)
	}
	if envelope.Error.Message != "database is not available" {
		t.Fatalf("503 message = %q, want %q", envelope.Error.Message, "database is not available")
	}

	// Invalid parameters are rejected before the store is ever touched.
	rec = getJobs(handler, "/jobs?page=abc")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid page with storage down: status = %d, want 400", rec.Code)
	}
	assertErrorCode(t, rec, "invalid_request")
}

func TestOverlappingPostRequestsLeaveSingleRecord(t *testing.T) {
	_, handler := newTestRouter(t)

	const clients = 24
	var wg sync.WaitGroup
	statuses := make(chan int, clients)
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			statuses <- postJobs(handler, validJobBody).Code
		}()
	}
	wg.Wait()
	close(statuses)

	var created, ok int
	for status := range statuses {
		switch status {
		case http.StatusCreated:
			created++
		case http.StatusOK:
			ok++
		default:
			t.Fatalf("unexpected status %d", status)
		}
	}
	if created != 1 || created+ok != clients {
		t.Fatalf("created=%d ok=%d, want exactly one 201 and the rest 200", created, ok)
	}
}

func assertErrorCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	if envelope.Error.Code != want {
		t.Fatalf("code = %q, want %q; body %s", envelope.Error.Code, want, rec.Body.String())
	}
	if envelope.Error.Message == "" {
		t.Fatalf("error message is empty: %s", rec.Body.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil || len(raw) != 1 {
		t.Fatalf("error response must have only the error key: %s", rec.Body.String())
	}
}
