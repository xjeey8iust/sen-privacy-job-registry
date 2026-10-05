package api

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
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

func TestGetJobsAcceptsLargePageNumbers(t *testing.T) {
	_, handler := newTestRouter(t)

	bodies := []string{
		`{"id":"job-a","computation_type":"c","participants":["alice","bob"],"input_refs":["r"]}`,
		`{"id":"job-b","computation_type":"c","participants":["alice"],"input_refs":["r"]}`,
		`{"id":"job-c","computation_type":"c","participants":["carol"],"input_refs":["r"]}`,
	}
	for _, body := range bodies {
		if rec := postJobs(handler, body); rec.Code != http.StatusCreated {
			t.Fatalf("setup: %d %s", rec.Code, rec.Body.String())
		}
	}

	type envelope struct {
		Total int               `json:"total"`
		Page  int64             `json:"page"`
		Size  int               `json:"page_size"`
		Items []json.RawMessage `json:"items"`
	}
	maxPage := strconv.Itoa(math.MaxInt)
	get := func(target string) envelope {
		t.Helper()
		rec := getJobs(handler, target)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200; body %s", target, rec.Code, rec.Body.String())
		}
		var env envelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("decode %s: %v", target, err)
		}
		if env.Items == nil {
			t.Fatalf("%s items = null, want []", target)
		}
		return env
	}

	// The largest page this platform can express is a real (empty) position
	// for every page-size boundary: no truncation, no wrap to page one.
	for _, size := range []int{1, 2, 100} {
		env := get("/jobs?page=" + maxPage + "&page_size=" + strconv.Itoa(size))
		if env.Total != 3 || env.Page != int64(math.MaxInt) || env.Size != size || len(env.Items) != 0 {
			t.Fatalf("max page size %d = %+v, want empty page echoing the request with total 3", size, env)
		}
	}

	// A participant filter keeps its filtered total on the huge page.
	env := get("/jobs?participant=alice&page=" + maxPage + "&page_size=2")
	if env.Total != 2 || len(env.Items) != 0 {
		t.Fatalf("filtered max page = %+v, want total 2 with empty items", env)
	}
	env = get("/jobs?participant=nobody&page=" + maxPage)
	if env.Total != 0 || len(env.Items) != 0 {
		t.Fatalf("no-match max page = %+v, want total 0 with empty items", env)
	}

	// The ordinary last page still holds its record; the next page is empty.
	env = get("/jobs?page=2&page_size=2")
	if env.Total != 3 || len(env.Items) != 1 {
		t.Fatalf("last page = %+v, want one item with total 3", env)
	}
	var last struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(env.Items[0], &last); err != nil || last.ID != "job-c" {
		t.Fatalf("last page item = %s, want job-c", env.Items[0])
	}
	env = get("/jobs?page=3&page_size=2")
	if env.Total != 3 || len(env.Items) != 0 {
		t.Fatalf("page after last = %+v, want empty with total 3", env)
	}

	// Leading zeros parse as the numeric value and echo that value back.
	env = get("/jobs?page=0002&page_size=2")
	if env.Page != 2 || env.Total != 3 || len(env.Items) != 1 {
		t.Fatalf("leading-zero page = %+v, want page 2 with one item", env)
	}

	// None of the queries above may disturb stored content, stage or history.
	first := get("/jobs")
	want := []string{
		`{"id":"job-a","computation_type":"c","participants":["alice","bob"],"input_refs":["r"],"stage":"registered","history":[{"stage":"registered"}]}`,
		`{"id":"job-b","computation_type":"c","participants":["alice"],"input_refs":["r"],"stage":"registered","history":[{"stage":"registered"}]}`,
		`{"id":"job-c","computation_type":"c","participants":["carol"],"input_refs":["r"],"stage":"registered","history":[{"stage":"registered"}]}`,
	}
	if first.Total != 3 || len(first.Items) != 3 {
		t.Fatalf("final listing = %+v, want all three jobs", first)
	}
	for i, wantJSON := range want {
		if got := string(first.Items[i]); got != wantJSON {
			t.Fatalf("job %d changed after queries:\n got %s\nwant %s", i, got, wantJSON)
		}
	}
}

func TestGetJobsEmptyStoreServesHugePage(t *testing.T) {
	_, handler := newTestRouter(t)

	rec := getJobs(handler, "/jobs?page="+strconv.Itoa(math.MaxInt)+"&page_size=100")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	want := `{"items":[],"page":` + strconv.Itoa(math.MaxInt) + `,"page_size":100,"total":0}`
	if got := rec.Body.String(); got != want {
		t.Fatalf("empty-store max page = %s, want %s", got, want)
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
		"/jobs?page=9223372036854775808",
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
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(get.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode 503 body: %v", err)
	}
	if body.Error.Message != "database is not available" {
		t.Fatalf("503 message = %q, want %q", body.Error.Message, "database is not available")
	}

	// A legal huge page still reaches storage and reports 503, while invalid
	// parameters are rejected with 400 before storage is consulted.
	huge := getJobs(handler, "/jobs?page="+strconv.Itoa(math.MaxInt)+"&page_size=2")
	if huge.Code != http.StatusServiceUnavailable {
		t.Fatalf("huge page status = %d, want 503", huge.Code)
	}
	assertErrorCode(t, huge, "storage_unavailable")

	invalid := getJobs(handler, "/jobs?page=abc")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid page with storage down status = %d, want 400", invalid.Code)
	}
	assertErrorCode(t, invalid, "invalid_request")
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
