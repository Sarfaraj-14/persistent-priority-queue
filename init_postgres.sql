-- init_postgres.sql
-- Optional: only needed if you use the PostgreSQL storage backend
-- (PostgresStorage in module.py). If you just use the default file
-- storage, you do NOT need this file or a PostgreSQL server at all.
--
-- Usage:
--   psql -U <user> -d <your_database> -f init_postgres.sql
--
-- Note: module.py's PostgresStorage will also auto-create this table
-- on first use (create_table=True by default), so running this script
-- manually is optional/idempotent.

CREATE TABLE IF NOT EXISTS priority_queue_state (
    id INTEGER PRIMARY KEY,
    state JSONB NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

-- The table stores the entire priority queue state as a single JSONB
-- row (id = 1), containing:
--   {
--     "seq": <int>,
--     "entries": {
--        "<item_id>": {"priority": <num>, "value": <any>, "seq": <int>},
--        ...
--     }
--   }
