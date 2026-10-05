// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	_ "modernc.org/sqlite"
)

// StageRegistered is the only stage a job holds right after registration.
const StageRegistered = "registered"

// Job is one registered computation job together with its stage history.
type Job struct {
	ID              string
	ComputationType string
	Participants    []string
	InputRefs       []string
	Stage           string
	History         []HistoryEntry
}

// HistoryEntry is one stage transition; today it only ever carries the stage name.
type HistoryEntry struct {
	Stage string
}

// JobConflictError reports a registration whose id already exists with different content.
type JobConflictError struct {
	ID string
}

func (e *JobConflictError) Error() string {
	return fmt.Sprintf("job %q is already registered with different content", e.ID)
}

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
	mu sync.Mutex // serializes registration so overlapping requests stay single-writer
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// RegisterJob stores a new job and its initial history entry in one transaction, so the
// record and the history either both appear or both stay absent. Re-registering identical
// content returns the stored record with created=false and appends nothing; different
// content for the same id fails with *JobConflictError and leaves the stored record intact.
func (s *Store) RegisterJob(job Job) (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Job{}, false, fmt.Errorf("begin registration: %w", err)
	}
	defer tx.Rollback()

	existing, found, err := findJob(tx, job.ID)
	if err != nil {
		return Job{}, false, err
	}
	if found {
		if existing.ComputationType == job.ComputationType &&
			slices.Equal(existing.Participants, job.Participants) &&
			slices.Equal(existing.InputRefs, job.InputRefs) {
			return existing, false, nil
		}
		return Job{}, false, &JobConflictError{ID: job.ID}
	}

	participantsJSON, err := json.Marshal(job.Participants)
	if err != nil {
		return Job{}, false, fmt.Errorf("encode participants: %w", err)
	}
	inputRefsJSON, err := json.Marshal(job.InputRefs)
	if err != nil {
		return Job{}, false, fmt.Errorf("encode input refs: %w", err)
	}

	if _, err := tx.Exec(
		`INSERT INTO jobs (id, computation_type, participants, input_refs, stage) VALUES (?, ?, ?, ?, ?)`,
		job.ID, job.ComputationType, participantsJSON, inputRefsJSON, StageRegistered,
	); err != nil {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}
	for position, participant := range job.Participants {
		if _, err := tx.Exec(
			`INSERT INTO job_participants (job_id, position, participant) VALUES (?, ?, ?)`,
			job.ID, position, participant,
		); err != nil {
			return Job{}, false, fmt.Errorf("insert participant: %w", err)
		}
	}
	if _, err := tx.Exec(
		`INSERT INTO job_history (job_id, seq, stage) VALUES (?, ?, ?)`,
		job.ID, 0, StageRegistered,
	); err != nil {
		return Job{}, false, fmt.Errorf("insert history: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, fmt.Errorf("commit registration: %w", err)
	}

	job.Stage = StageRegistered
	job.History = []HistoryEntry{{Stage: StageRegistered}}
	return job, true, nil
}

// ListJobs returns one page of jobs ordered by id byte-wise (SQLite TEXT BINARY collation
// compares UTF-8 bytes directly) plus the total number of jobs matching the filter. An
// empty participant disables filtering; otherwise the match is exact.
func (s *Store) ListJobs(participant string, page, pageSize int) ([]Job, int, error) {
	conditions := ""
	filterArgs := []any{}
	if participant != "" {
		conditions = ` WHERE EXISTS (SELECT 1 FROM job_participants jp WHERE jp.job_id = jobs.id AND jp.participant = ?)`
		filterArgs = append(filterArgs, participant)
	}

	var total int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs`+conditions, filterArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	queryArgs := make([]any, 0, len(filterArgs)+2)
	queryArgs = append(queryArgs, filterArgs...)
	queryArgs = append(queryArgs, pageSize, (page-1)*pageSize)
	rows, err := s.db.Query(
		`SELECT id, computation_type, participants, input_refs, stage FROM jobs`+conditions+` ORDER BY id LIMIT ? OFFSET ?`,
		queryArgs...,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	jobs := []Job{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, 0, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	rows.Close()

	for i := range jobs {
		history, err := loadHistory(s.db, jobs[i].ID)
		if err != nil {
			return nil, 0, err
		}
		jobs[i].History = history
	}
	return jobs, total, nil
}

// queryer is satisfied by both *sql.DB and *sql.Tx.
type queryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// findJob loads one job with its history; found is false when the id is unknown.
func findJob(q queryer, id string) (Job, bool, error) {
	job, err := scanJob(q.QueryRow(
		`SELECT id, computation_type, participants, input_refs, stage FROM jobs WHERE id = ?`, id,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("read job: %w", err)
	}
	history, err := loadHistory(q, id)
	if err != nil {
		return Job{}, false, err
	}
	job.History = history
	return job, true, nil
}

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(scanner rowScanner) (Job, error) {
	var (
		job             Job
		participantsRaw string
		inputRefsRaw    string
	)
	if err := scanner.Scan(&job.ID, &job.ComputationType, &participantsRaw, &inputRefsRaw, &job.Stage); err != nil {
		return Job{}, err
	}
	if err := json.Unmarshal([]byte(participantsRaw), &job.Participants); err != nil {
		return Job{}, fmt.Errorf("decode participants: %w", err)
	}
	if err := json.Unmarshal([]byte(inputRefsRaw), &job.InputRefs); err != nil {
		return Job{}, fmt.Errorf("decode input refs: %w", err)
	}
	return job, nil
}

func loadHistory(q queryer, jobID string) ([]HistoryEntry, error) {
	rows, err := q.Query(`SELECT stage FROM job_history WHERE job_id = ? ORDER BY seq`, jobID)
	if err != nil {
		return nil, fmt.Errorf("read history: %w", err)
	}
	defer rows.Close()
	history := []HistoryEntry{}
	for rows.Next() {
		var entry HistoryEntry
		if err := rows.Scan(&entry.Stage); err != nil {
			return nil, fmt.Errorf("scan history: %w", err)
		}
		history = append(history, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read history: %w", err)
	}
	return history, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
	id               TEXT PRIMARY KEY,
	computation_type TEXT NOT NULL,
	participants     TEXT NOT NULL,
	input_refs       TEXT NOT NULL,
	stage            TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_participants (
	job_id      TEXT NOT NULL REFERENCES jobs (id),
	position    INTEGER NOT NULL,
	participant TEXT NOT NULL,
	PRIMARY KEY (job_id, position)
);
CREATE TABLE IF NOT EXISTS job_history (
	job_id TEXT NOT NULL REFERENCES jobs (id),
	seq    INTEGER NOT NULL,
	stage  TEXT NOT NULL,
	PRIMARY KEY (job_id, seq)
);
`
