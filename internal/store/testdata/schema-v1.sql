-- Schema version 1 exactly as shipped (stagehand e6925932). Frozen: never edit.
CREATE TABLE pipelines (
		name        TEXT PRIMARY KEY,
		spec_yaml   BLOB NOT NULL,
		spec_hash   TEXT NOT NULL,
		source_dir  TEXT NOT NULL,
		enabled     INTEGER NOT NULL DEFAULT 0,
		next_due_at TEXT,
		created_at  TEXT NOT NULL,
		updated_at  TEXT NOT NULL
	);
	CREATE TABLE runs (
		id               INTEGER PRIMARY KEY AUTOINCREMENT,
		pipeline         TEXT NOT NULL,
		trigger          TEXT NOT NULL,
		spec_yaml        BLOB NOT NULL,
		spec_hash        TEXT NOT NULL,
		source_dir       TEXT NOT NULL,
		state            TEXT NOT NULL,
		halt_stage       TEXT NOT NULL DEFAULT '',
		halt_reason      TEXT NOT NULL DEFAULT '',
		cancel_requested INTEGER NOT NULL DEFAULT 0,
		created_at       TEXT NOT NULL,
		finished_at      TEXT
	);
	CREATE INDEX runs_by_pipeline ON runs(pipeline, id);
	CREATE UNIQUE INDEX runs_one_active ON runs(pipeline) WHERE state = 'running';
	CREATE TABLE run_stages (
		run_id      INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
		stage_id    TEXT NOT NULL,
		position    INTEGER NOT NULL,
		state       TEXT NOT NULL,
		attempts    INTEGER NOT NULL DEFAULT 0,
		retry_base  INTEGER NOT NULL DEFAULT 0,
		summary     TEXT NOT NULL DEFAULT '',
		needs_json  TEXT NOT NULL DEFAULT '',
		started_at  TEXT,
		finished_at TEXT,
		PRIMARY KEY (run_id, stage_id)
	);
	CREATE TABLE stage_attempts (
		run_id        INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
		stage_id      TEXT NOT NULL,
		attempt       INTEGER NOT NULL,
		state         TEXT NOT NULL,
		exit_code     INTEGER,
		cost          REAL NOT NULL DEFAULT 0,
		input_tokens  INTEGER NOT NULL DEFAULT 0,
		output_tokens INTEGER NOT NULL DEFAULT 0,
		log_path      TEXT NOT NULL,
		resource      TEXT NOT NULL DEFAULT '',
		summary       TEXT NOT NULL DEFAULT '',
		needs_json    TEXT NOT NULL DEFAULT '',
		started_at    TEXT NOT NULL,
		finished_at   TEXT,
		PRIMARY KEY (run_id, stage_id, attempt)
	);
	CREATE INDEX stage_attempts_state ON stage_attempts(state);
