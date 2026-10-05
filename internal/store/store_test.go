package store

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"
)

func sampleRegistration() JobRegistration {
	return JobRegistration{
		ID:              "job-1",
		ComputationType: "mpc",
		Participants:    []string{"alice", "bob"},
		InputRefs:       []string{"ref-a", "ref-b"},
	}
}

func openStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestRegisterJobCreatesRecordWithInitialHistory(t *testing.T) {
	st := openStore(t)

	job, created, err := st.RegisterJob(context.Background(), sampleRegistration())
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if job.ID != "job-1" || job.ComputationType != "mpc" || job.Stage != StageRegistered {
		t.Fatalf("unexpected job fields: %+v", job)
	}
	if got, want := len(job.Participants), 2; got != want || job.Participants[0] != "alice" || job.Participants[1] != "bob" {
		t.Fatalf("participants = %v", job.Participants)
	}
	if len(job.InputRefs) != 2 || job.InputRefs[0] != "ref-a" || job.InputRefs[1] != "ref-b" {
		t.Fatalf("input_refs = %v", job.InputRefs)
	}
	if len(job.History) != 1 || job.History[0].Stage != StageRegistered {
		t.Fatalf("history = %+v, want only registered", job.History)
	}
}

func TestRegisterJobIdempotentForIdenticalRepeat(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	if _, _, err := st.RegisterJob(ctx, sampleRegistration()); err != nil {
		t.Fatalf("first register: %v", err)
	}
	job, created, err := st.RegisterJob(ctx, sampleRegistration())
	if err != nil {
		t.Fatalf("repeat register: %v", err)
	}
	if created {
		t.Fatal("created = true on identical repeat, want false")
	}
	if len(job.History) != 1 {
		t.Fatalf("history grew to %d entries, want 1", len(job.History))
	}

	jobs, total, err := st.ListJobs(ctx, "", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(jobs) != 1 {
		t.Fatalf("total=%d len=%d, want single record", total, len(jobs))
	}
}

func TestRegisterJobConflictLeavesExistingUntouched(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	original := sampleRegistration()
	if _, _, err := st.RegisterJob(ctx, original); err != nil {
		t.Fatalf("register: %v", err)
	}

	conflicting := original
	conflicting.ComputationType = "fhe"
	if _, _, err := st.RegisterJob(ctx, conflicting); err == nil {
		t.Fatal("want JobConflictError for changed computation_type, got nil")
	} else {
		var conflict *JobConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("err = %T, want *JobConflictError", err)
		}
	}

	// Array order differs: that is a conflict as well.
	reordered := original
	reordered.Participants = []string{"bob", "alice"}
	if _, _, err := st.RegisterJob(ctx, reordered); err == nil {
		t.Fatal("want conflict when participants order differs")
	}

	jobs, total, _ := st.ListJobs(ctx, "", 1, 20)
	if total != 1 || len(jobs) != 1 {
		t.Fatalf("want single untouched record, got total=%d len=%d", total, len(jobs))
	}
	got := jobs[0]
	if got.ComputationType != "mpc" {
		t.Fatalf("existing computation_type overwritten: %q", got.ComputationType)
	}
	if got.Participants[0] != "alice" || got.Participants[1] != "bob" {
		t.Fatalf("existing participants overwritten: %v", got.Participants)
	}
}

func TestRegisterJobSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")

	st, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, _, err := st.RegisterJob(context.Background(), sampleRegistration()); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	jobs, total, err := reopened.ListJobs(context.Background(), "", 1, 20)
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if total != 1 || len(jobs) != 1 {
		t.Fatalf("total=%d len=%d after reopen, want 1", total, len(jobs))
	}
	if jobs[0].ID != "job-1" || len(jobs[0].History) != 1 || jobs[0].History[0].Stage != StageRegistered {
		t.Fatalf("record after replay = %+v", jobs[0])
	}
}

func TestListJobsOrdersByUTF8BytesAndPaginates(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	// Byte order: ASCII uppercase sorts before lowercase; "中" (E4 B8 AD)
	// sorts after ASCII.
	ids := []string{"zebra", "Apple", "mid", "中x"}
	for _, id := range ids {
		reg := sampleRegistration()
		reg.ID = id
		if _, _, err := st.RegisterJob(ctx, reg); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	page1, total, err := st.ListJobs(ctx, "", 1, 2)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 4 {
		t.Fatalf("total = %d, want 4", total)
	}
	if len(page1) != 2 || page1[0].ID != "Apple" || page1[1].ID != "mid" {
		t.Fatalf("page 1 = %v, want Apple,mid", idsOf(page1))
	}

	page2, _, err := st.ListJobs(ctx, "", 2, 2)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(page2) != 2 || page2[0].ID != "zebra" || page2[1].ID != "中x" {
		t.Fatalf("page 2 = %v, want zebra,中x", idsOf(page2))
	}

	page3, _, err := st.ListJobs(ctx, "", 3, 2)
	if err != nil {
		t.Fatalf("list page 3: %v", err)
	}
	if len(page3) != 0 {
		t.Fatalf("out-of-range page = %v, want empty", idsOf(page3))
	}
}

func TestListJobsHugePageReturnsEmptyWithTotal(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	for _, id := range []string{"job-a", "job-b", "job-c"} {
		reg := sampleRegistration()
		reg.ID = id
		if _, _, err := st.RegisterJob(ctx, reg); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	// The largest page this platform can express must not wrap the offset
	// back onto real rows, whatever the page size.
	for _, pageSize := range []int{1, 2, 100} {
		jobs, total, err := st.ListJobs(ctx, "", math.MaxInt, pageSize)
		if err != nil {
			t.Fatalf("list max page size %d: %v", pageSize, err)
		}
		if total != 3 || len(jobs) != 0 {
			t.Fatalf("max page size %d = %v (total %d), want empty page with total 3",
				pageSize, idsOf(jobs), total)
		}
	}

	// A participant filter keeps its filtered total on the huge page.
	jobs, total, err := st.ListJobs(ctx, "alice", math.MaxInt, 2)
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if total != 3 || len(jobs) != 0 {
		t.Fatalf("filtered max page = %v (total %d), want empty with total 3", idsOf(jobs), total)
	}

	// A filter matching nothing reports total 0.
	jobs, total, err = st.ListJobs(ctx, "nobody", math.MaxInt, 2)
	if err != nil {
		t.Fatalf("no-match list: %v", err)
	}
	if total != 0 || len(jobs) != 0 {
		t.Fatalf("no-match max page = %v (total %d), want empty with total 0", idsOf(jobs), total)
	}
}

func TestListJobsFiltersByExactParticipant(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	reg1 := sampleRegistration()
	reg1.ID = "j1"
	reg1.Participants = []string{"alice", "bob"}
	reg2 := sampleRegistration()
	reg2.ID = "j2"
	reg2.Participants = []string{"carol"}
	for _, reg := range []JobRegistration{reg1, reg2} {
		if _, _, err := st.RegisterJob(ctx, reg); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	jobs, total, err := st.ListJobs(ctx, "alice", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].ID != "j1" {
		t.Fatalf("filter result = %v (total %d), want only j1", idsOf(jobs), total)
	}

	// Exact match only: "ali" must not match "alice".
	jobs, total, _ = st.ListJobs(ctx, "ali", 1, 20)
	if total != 0 || len(jobs) != 0 {
		t.Fatalf("partial match returned %v", idsOf(jobs))
	}
}

func TestConcurrentOverlappingRegistrationsLeaveSingleRecord(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()

	const goroutines = 32
	var wg sync.WaitGroup
	createdCount := make(chan bool, goroutines)
	errs := make(chan error, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, created, err := st.RegisterJob(ctx, sampleRegistration())
			if err != nil {
				errs <- err
				return
			}
			createdCount <- created
		}()
	}
	wg.Wait()
	close(createdCount)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent register: %v", err)
	}
	creates := 0
	for created := range createdCount {
		if created {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("created %d times, want exactly 1", creates)
	}

	jobs, total, err := st.ListJobs(ctx, "", 1, 20)
	if err != nil || total != 1 || len(jobs) != 1 {
		t.Fatalf("total=%d len=%d err=%v, want exactly one record", total, len(jobs), err)
	}
	if len(jobs[0].History) != 1 {
		t.Fatalf("history entries = %d, want 1", len(jobs[0].History))
	}
}

func idsOf(jobs []*Job) []string {
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}
	return ids
}
