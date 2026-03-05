# Power Interconnection Queue Monitor v3.1

Tracks interconnection requests across all 7 US ISO/RTOs.
PostgreSQL database. Built for Railway deployment.

## Data Sources

| ISO/RTO | Method | Coverage |
|---------|--------|----------|
| CAISO | gridstatus library | California |
| ERCOT | gridstatus library | Texas |
| MISO | Free JSON API | Midwest (14 states) |
| PJM | Berkeley Lab dataset | Mid-Atlantic (13 states) |
| SPP | Direct CSV download | Central US (9 states) |
| NYISO | Direct Excel download | New York |
| ISO-NE | HTML table scraping | New England (6 states) |

## Features

- **PostgreSQL** with connection pooling (`psycopg2.pool.ThreadedConnectionPool`)
- **Automatic weekly sync** via background thread (configurable interval)
- **Sortable, filterable project table** — client-side: sort any column, filter by state / ISO / type / capacity / free-text
- **Dashboard** with charts (projects by ISO and type)
- **CSV export** of full project list
- **System monitoring** page with sync history and source status
- **JSON API** at `/api/projects` and `/api/stats`
- **Upserts** via `INSERT ... ON CONFLICT` — no duplicates, always fresh data

## Deploy to Railway

1. Push this repo to GitHub
2. Create a new project on [Railway](https://railway.app)
3. Connect your GitHub repo
4. **Add a PostgreSQL database** — click "New" → "Database" → "PostgreSQL"
5. Railway auto-injects `DATABASE_URL` into your service. No manual config needed.
6. Optionally set environment variables:
   - `SYNC_INTERVAL_SECONDS` — default `604800` (7 days). Set to `86400` for daily.
   - `SECRET_KEY` — for Flask sessions

The app auto-creates tables on first startup and runs an initial sync if the database is empty.

### Railway env vars (auto-set)

- `DATABASE_URL` — PostgreSQL connection string (Railway provides this automatically when you link the database)
- `PORT` — HTTP port (Railway sets this)

## Run Locally

```bash
# Start a local PostgreSQL (e.g. via Docker)
docker run -d --name pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16

# Set the connection string and run
export DATABASE_URL="postgresql://postgres:pw@localhost:5432/postgres"
pip install -r requirements.txt
python app.py
```

Visit `http://localhost:8080`.

## Project Structure

```
app.py              — Flask app + data fetchers + scheduler + DB layer
templates/
  base.html         — Sidebar layout
  index.html        — Dashboard with charts
  projects.html     — Full project table (client-side sort/filter)
  project_detail.html — Single project view
  monitoring.html   — Sync history & source status
  scan_results.html — Post-sync summary
Dockerfile          — Container build
Procfile            — Gunicorn process for Railway
requirements.txt    — Python dependencies
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/projects` | GET | All projects as JSON |
| `/api/stats` | GET | Aggregate statistics |
| `/api/sync` | POST | Trigger a sync manually |
| `/export` | GET | Download CSV |
| `/trigger` | GET | Trigger sync (web UI redirect) |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *required* | PostgreSQL connection string |
| `DATA_DIR` | `/app/data` | Working directory for cached files |
| `SYNC_INTERVAL_SECONDS` | `604800` | Seconds between automatic syncs |
| `SECRET_KEY` | `dev-key-...` | Flask secret key |
| `PORT` | `8080` | HTTP port |

## Database Schema

Three tables, all auto-created on startup:

- **projects** — one row per interconnection queue entry (upserted by `request_id`)
- **monitor_runs** — one row per sync run with timing and counts
- **sync_log** — one row per source per sync with status and error messages
