package store

import (
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

func TestOpenCreatesUsableStore(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	if err := st.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func sampleJob() Job {
	return Job{
		ID:              "job-1",
		ComputationType: "mpc-sum",
		Participants:    []string{"alice", "bob"},
		InputRefs:       []string{"s3://inputs/a", "s3://inputs/b"},
	}
}

func TestRegisterJobStoresRecordWithInitialHistory(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	stored, created, err := st.RegisterJob(sampleJob())
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if stored.Stage != StageRegistered {
		t.Fatalf("stage = %q, want %q", stored.Stage, StageRegistered)
	}
	if len(stored.History) != 1 || stored.History[0].Stage != StageRegistered {
		t.Fatalf("history = %+v, want single registered entry", stored.History)
	}
	if !slices.Equal(stored.Participants, []string{"alice", "bob"}) {
		t.Fatalf("participants = %v, order not preserved", stored.Participants)
	}
}

func TestRegisterJobSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, _, err := st.RegisterJob(sampleJob()); err != nil {
		t.Fatalf("register: %v", err)
	}
	st.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	jobs, total, err := reopened.ListJobs("", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(jobs) != 1 {
		t.Fatalf("total = %d, len(jobs) = %d, want 1/1", total, len(jobs))
	}
	got := jobs[0]
	if got.ID != "job-1" || got.ComputationType != "mpc-sum" || got.Stage != StageRegistered {
		t.Fatalf("job = %+v", got)
	}
	if !slices.Equal(got.Participants, []string{"alice", "bob"}) ||
		!slices.Equal(got.InputRefs, []string{"s3://inputs/a", "s3://inputs/b"}) {
		t.Fatalf("arrays not preserved: %+v", got)
	}
	if len(got.History) != 1 || got.History[0].Stage != StageRegistered {
		t.Fatalf("history = %+v", got.History)
	}
}

func TestRegisterIdenticalJobIsIdempotent(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	if _, _, err := st.RegisterJob(sampleJob()); err != nil {
		t.Fatalf("register: %v", err)
	}
	again, created, err := st.RegisterJob(sampleJob())
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if created {
		t.Fatal("created = true, want false for identical replay")
	}
	if len(again.History) != 1 {
		t.Fatalf("history grew on replay: %+v", again.History)
	}
	_, total, err := st.ListJobs("", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 {
		t.Fatalf("total = %d, want 1", total)
	}
}

func TestRegisterDifferentContentConflictsAndKeepsOriginal(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	if _, _, err := st.RegisterJob(sampleJob()); err != nil {
		t.Fatalf("register: %v", err)
	}

	variants := []Job{
		{ID: "job-1", ComputationType: "other", Participants: []string{"alice", "bob"}, InputRefs: []string{"s3://inputs/a", "s3://inputs/b"}},
		{ID: "job-1", ComputationType: "mpc-sum", Participants: []string{"bob", "alice"}, InputRefs: []string{"s3://inputs/a", "s3://inputs/b"}},
		{ID: "job-1", ComputationType: "mpc-sum", Participants: []string{"alice", "bob"}, InputRefs: []string{"s3://inputs/a"}},
	}
	for _, variant := range variants {
		_, _, err := st.RegisterJob(variant)
		var conflict *JobConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("register %+v: err = %v, want JobConflictError", variant, err)
		}
	}

	jobs, total, err := st.ListJobs("", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(jobs) != 1 {
		t.Fatalf("total = %d, want 1", total)
	}
	if jobs[0].ComputationType != "mpc-sum" || len(jobs[0].History) != 1 {
		t.Fatalf("original record was overwritten: %+v", jobs[0])
	}
}

func TestConcurrentIdenticalRegistrationsLeaveOneRecord(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := st.RegisterJob(sampleJob()); err != nil {
				t.Errorf("register: %v", err)
			}
		}()
	}
	wg.Wait()

	jobs, total, err := st.ListJobs("", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(jobs) != 1 || len(jobs[0].History) != 1 {
		t.Fatalf("total = %d, jobs = %+v, want one record with one history entry", total, jobs)
	}
}

func TestListJobsOrdersByIDBytesAndPaginates(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	// UTF-8 byte order: "apple" < "banana" < "zeta" < "éclair" (0xC3 > 0x7A).
	for _, id := range []string{"zeta", "éclair", "banana", "apple"} {
		job := sampleJob()
		job.ID = id
		if _, _, err := st.RegisterJob(job); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	page1, total, err := st.ListJobs("", 1, 3)
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if total != 4 || len(page1) != 3 {
		t.Fatalf("total = %d, len = %d, want 4/3", total, len(page1))
	}
	got := []string{page1[0].ID, page1[1].ID, page1[2].ID}
	if !slices.Equal(got, []string{"apple", "banana", "zeta"}) {
		t.Fatalf("page 1 order = %v", got)
	}

	page2, _, err := st.ListJobs("", 2, 3)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(page2) != 1 || page2[0].ID != "éclair" {
		t.Fatalf("page 2 = %+v", page2)
	}

	beyond, total, err := st.ListJobs("", 5, 3)
	if err != nil {
		t.Fatalf("list page 5: %v", err)
	}
	if total != 4 || len(beyond) != 0 {
		t.Fatalf("out-of-range page: total = %d, len = %d, want 4/0", total, len(beyond))
	}
}

func TestListJobsFiltersByParticipantExactly(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	jobs := []Job{
		{ID: "job-a", ComputationType: "t", Participants: []string{"alice", "bob"}, InputRefs: []string{"r"}},
		{ID: "job-b", ComputationType: "t", Participants: []string{"bob"}, InputRefs: []string{"r"}},
		{ID: "job-c", ComputationType: "t", Participants: []string{"alicia"}, InputRefs: []string{"r"}},
	}
	for _, job := range jobs {
		if _, _, err := st.RegisterJob(job); err != nil {
			t.Fatalf("register %s: %v", job.ID, err)
		}
	}

	got, total, err := st.ListJobs("alice", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(got) != 1 || got[0].ID != "job-a" {
		t.Fatalf("participant=alice: total = %d, jobs = %+v", total, got)
	}

	got, total, err = st.ListJobs("bob", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 2 || len(got) != 2 || got[0].ID != "job-a" || got[1].ID != "job-b" {
		t.Fatalf("participant=bob: total = %d, jobs = %+v", total, got)
	}

	_, total, err = st.ListJobs("carol", 1, 20)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 0 {
		t.Fatalf("participant=carol: total = %d, want 0", total)
	}
}
