// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// StageRegistered is the only stage a job has when it is first registered.
const StageRegistered = "registered"

// JobConflictError reports that a job id is already registered with fields
// that do not exactly match a repeated registration.
type JobConflictError struct {
	ID string
}

func (e *JobConflictError) Error() string {
	return fmt.Sprintf("job %q is registered with different fields", e.ID)
}

// JobRegistration carries the four client-supplied registration fields.
type JobRegistration struct {
	ID              string
	ComputationType string
	Participants    []string
	InputRefs       []string
}

// HistoryEntry is one stage transition record.
type HistoryEntry struct {
	Stage string `json:"stage"`
}

// Job is the stored record returned by the API.
type Job struct {
	ID              string         `json:"id"`
	ComputationType string         `json:"computation_type"`
	Participants    []string       `json:"participants"`
	InputRefs       []string       `json:"input_refs"`
	Stage           string         `json:"stage"`
	History         []HistoryEntry `json:"history"`
}

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB

	// writeMu serializes register transactions inside this process so that
	// overlapping requests observe single-writer semantics.
	writeMu sync.Mutex
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply %s: %w", pragma, err)
		}
	}
	for _, stmt := range schemaStatements {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	}
	return &Store{db: db}, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// RegisterJob stores a new job together with its initial history entry, or
// returns the existing record. It returns created=true only when a new row was
// inserted. A re-registration whose fields differ in value or array order fails
// with *JobConflictError; in every failure case existing content is left
// untouched.
func (s *Store) RegisterJob(ctx context.Context, reg JobRegistration) (job *Job, created bool, err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// A dedicated connection with BEGIN IMMEDIATE takes the write lock up
	// front: overlapping registrations (including other processes) queue on
	// busy_timeout and then observe the winner's committed row instead of
	// failing on a deferred-lock upgrade.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	var existingType string
	var participantsJSON, inputRefsJSON string
	lookupErr := conn.QueryRowContext(ctx,
		`SELECT computation_type, participants, input_refs FROM jobs WHERE id = ?`,
		reg.ID,
	).Scan(&existingType, &participantsJSON, &inputRefsJSON)

	if lookupErr == nil {
		var existingParticipants, existingRefs []string
		if err := json.Unmarshal([]byte(participantsJSON), &existingParticipants); err != nil {
			return nil, false, err
		}
		if err := json.Unmarshal([]byte(inputRefsJSON), &existingRefs); err != nil {
			return nil, false, err
		}
		if existingType != reg.ComputationType ||
			!equalStrings(existingParticipants, reg.Participants) ||
			!equalStrings(existingRefs, reg.InputRefs) {
			return nil, false, &JobConflictError{ID: reg.ID}
		}
		job, err := loadJob(ctx, conn, reg.ID)
		if err != nil {
			return nil, false, err
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return nil, false, err
		}
		committed = true
		return job, false, nil
	}
	if !errors.Is(lookupErr, sql.ErrNoRows) {
		return nil, false, lookupErr
	}

	encodedParticipants, err := json.Marshal(reg.Participants)
	if err != nil {
		return nil, false, err
	}
	encodedInputRefs, err := json.Marshal(reg.InputRefs)
	if err != nil {
		return nil, false, err
	}

	if _, err := conn.ExecContext(ctx,
		`INSERT INTO jobs (id, computation_type, participants, input_refs, stage)
		 VALUES (?, ?, ?, ?, ?)`,
		reg.ID, reg.ComputationType, string(encodedParticipants),
		string(encodedInputRefs), StageRegistered,
	); err != nil {
		return nil, false, err
	}
	for position, participant := range reg.Participants {
		if _, err := conn.ExecContext(ctx,
			`INSERT INTO job_participants (job_id, position, participant)
			 VALUES (?, ?, ?)`,
			reg.ID, position, participant,
		); err != nil {
			return nil, false, err
		}
	}
	if _, err := conn.ExecContext(ctx,
		`INSERT INTO job_history (job_id, seq, stage) VALUES (?, 0, ?)`,
		reg.ID, StageRegistered,
	); err != nil {
		return nil, false, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, false, err
	}
	committed = true

	return &Job{
		ID:              reg.ID,
		ComputationType: reg.ComputationType,
		Participants:    append([]string(nil), reg.Participants...),
		InputRefs:       append([]string(nil), reg.InputRefs...),
		Stage:           StageRegistered,
		History:         []HistoryEntry{{Stage: StageRegistered}},
	}, true, nil
}

// ListJobs returns one page of jobs ordered by id in ascending UTF-8 byte
// order, optionally filtered by an exact participant match, together with the
// total number of rows after filtering.
func (s *Store) ListJobs(ctx context.Context, participant string, page, pageSize int) ([]*Job, int, error) {
	filter := participant != ""

	countQuery := `SELECT count(*) FROM jobs j`
	var args []any
	if filter {
		countQuery += ` WHERE EXISTS (
			SELECT 1 FROM job_participants p
			WHERE p.job_id = j.id AND p.participant = ?)`
		args = append(args, participant)
	}

	var total int
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listQuery := `SELECT j.id, j.computation_type, j.participants, j.input_refs, j.stage
		FROM jobs j`
	if filter {
		listQuery += ` WHERE EXISTS (
			SELECT 1 FROM job_participants p
			WHERE p.job_id = j.id AND p.participant = ?)`
	}
	listQuery += ` ORDER BY j.id COLLATE BINARY LIMIT ? OFFSET ?`
	queryArgs := append(append([]any(nil), args...), pageSize, (page-1)*pageSize)

	jobs, err := s.queryJobs(ctx, listQuery, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type rowScanner interface {
	Scan(dest ...any) error
}

func loadJob(ctx context.Context, q queryer, id string) (*Job, error) {
	const query = `SELECT id, computation_type, participants, input_refs, stage
		FROM jobs WHERE id = ?`
	job, err := scanJob(q.QueryRowContext(ctx, query, id))
	if err != nil {
		return nil, err
	}
	if err := attachHistory(ctx, q, []*Job{job}); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *Store) queryJobs(ctx context.Context, query string, args ...any) ([]*Job, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := attachHistory(ctx, s.db, jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func scanJob(sc rowScanner) (*Job, error) {
	job := &Job{History: []HistoryEntry{}}
	var participantsJSON, inputRefsJSON string
	if err := sc.Scan(
		&job.ID, &job.ComputationType, &participantsJSON, &inputRefsJSON, &job.Stage,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(participantsJSON), &job.Participants); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(inputRefsJSON), &job.InputRefs); err != nil {
		return nil, err
	}
	return job, nil
}

func attachHistory(ctx context.Context, q queryer, jobs []*Job) error {
	if len(jobs) == 0 {
		return nil
	}

	var builder strings.Builder
	builder.WriteString(`SELECT job_id, stage FROM job_history WHERE job_id IN (`)
	args := make([]any, len(jobs))
	for i, job := range jobs {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString("?")
		args[i] = job.ID
	}
	builder.WriteString(`) ORDER BY job_id COLLATE BINARY, seq`)

	rows, err := q.QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	history := make(map[string][]HistoryEntry, len(jobs))
	for rows.Next() {
		var jobID, stage string
		if err := rows.Scan(&jobID, &stage); err != nil {
			return err
		}
		history[jobID] = append(history[jobID], HistoryEntry{Stage: stage})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, job := range jobs {
		if entries := history[job.ID]; len(entries) > 0 {
			job.History = entries
		} else {
			job.History = []HistoryEntry{}
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS service_metadata (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS jobs (
		id               TEXT PRIMARY KEY,
		computation_type TEXT NOT NULL,
		participants     TEXT NOT NULL,
		input_refs       TEXT NOT NULL,
		stage            TEXT NOT NULL
	);`,
	`CREATE TABLE IF NOT EXISTS job_participants (
		job_id      TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
		position    INTEGER NOT NULL,
		participant TEXT NOT NULL,
		PRIMARY KEY (job_id, position)
	);`,
	`CREATE TABLE IF NOT EXISTS job_history (
		job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
		seq    INTEGER NOT NULL,
		stage  TEXT NOT NULL,
		PRIMARY KEY (job_id, seq)
	);`,
	`CREATE INDEX IF NOT EXISTS idx_job_participants_participant
		ON job_participants(participant);`,
}
