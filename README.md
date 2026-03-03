# Booking Detection Pipeline

9-pass pipeline that determines whether activity/tour provider websites have online booking systems. Reads domains from a Supabase table, runs progressively deeper detection passes, and writes structured results back.

## How it works

Each domain goes through up to 9 passes. Once a domain is resolved (booking found), it skips remaining passes:

| Pass | Method | Cost |
|------|--------|------|
| 1 | Firecrawl scrape homepage | ~$0.001 |
| 2 | LLM (GPT-4o-mini) on homepage HTML | ~$0.002 |
| 3 | Regex scan on homepage HTML | Free |
| 4 | Firecrawl crawl booking-related subpages | ~$0.01 |
| 5 | Regex scan on crawled subpages | Free |
| 6 | Firecrawl broad crawl (no path filter) | ~$0.01 |
| 7 | Regex scan on broad crawl results | Free |
| 8 | LLM on concatenated crawled markdown | ~$0.003 |
| 9 | Linkup deep search (optional) | $0.05 |

Results are checkpointed to Supabase after each pass — crashes don't lose work.

## Setup

```bash
git clone https://github.com/danieluszta/kraft-booking-detection-supabase
cd kraft-booking-detection-supabase
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Environment variables

Copy `.env.example` to `.env` and fill in your keys:

```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
DATABASE_URL=postgres://postgres.ref:password@aws-0-region.pooler.supabase.com:5432/postgres
FIRECRAWL_API_KEY=fc-...
OPENAI_API_KEY=sk-...
LINKUP_API_KEY=...          # optional, only needed with --include-linkup
```

- **SUPABASE_URL** + **SUPABASE_KEY**: Supabase Dashboard > Settings > API (use the `service_role` key)
- **DATABASE_URL**: Supabase Dashboard > Settings > Database > Connection string > Session pooler (URI)
- **FIRECRAWL_API_KEY**: [firecrawl.dev](https://firecrawl.dev)
- **OPENAI_API_KEY**: [platform.openai.com](https://platform.openai.com)
- **LINKUP_API_KEY**: [linkup.so](https://linkup.so) (optional)

### Database tables

Run the migration in Supabase SQL Editor or via psycopg2:

```sql
-- paste contents of migrations/001_create_tables.sql
```

This creates three tables:
- `booking_detection_input` — domains to process (columns: domain, status)
- `booking_detection_output` — results per domain (JSONB)
- `booking_detection_log` — audit trail of all API calls

## Usage

```bash
# Run with defaults (reads from booking_detection_input)
python3 run_pipeline.py

# Custom tables and batch size
python3 run_pipeline.py --source-table my_input --dest-table my_output --batch-size 100

# Enable Linkup deep search (pass 9, $0.05/domain)
python3 run_pipeline.py --include-linkup

# Debug logging
python3 run_pipeline.py --verbose
```

### Loading domains

Insert domains into the input table with `status = 'pending'`:

```sql
INSERT INTO booking_detection_input (domain) VALUES
  ('arcticadventures.is'),
  ('contexttravel.com'),
  ('nomadicmatt.com');
```

Or via the Supabase client library / dashboard.

## Output format

Each result row contains a JSONB `result` column:

```json
{
  "has_booking": true,
  "booking_platform": "FareHarbor",
  "reasoning": "Found FareHarbor iframe embed on /tours page",
  "source_pass": "regex_subpages"
}
```

## Tests

72 unit tests, no API keys needed:

```bash
pytest tests/ -v
```

Covers:
- Regex pattern matching for all 13+ booking platforms
- LLM response JSON parsing (clean, markdown, malformed)
- Linkup response parsing across all nested response shapes
- Supabase retry logic for transient connection errors

## Architecture

```
run_pipeline.py          — 9-pass orchestrator
lib/
  supabase_client.py     — Supabase data access (HTTP/1.1, retry on transient errors)
  firecrawl_client.py    — Firecrawl wrapper with audit logging
  firecrawl_local.py     — Raw Firecrawl API calls (scrape + async crawl with polling)
  llm_client.py          — OpenAI wrapper with audit logging
  llm_analysis_local.py  — Raw OpenAI calls + JSON response parsing
  linkup_client.py       — Linkup wrapper with audit logging
  linkup_local.py        — Raw Linkup API calls + structured output parsing
  text_scanner.py        — Regex pattern matching engine
configs/
  booking_fingerprints.yaml          — 29 regex patterns (platforms, ecommerce, payment, signals)
  prompts/
    booking_detection_html.txt       — LLM prompt for homepage HTML analysis
    booking_detection_crawled.txt    — LLM prompt for crawled page analysis
    booking_detection_own_site.md    — Linkup deep search prompt
migrations/
  001_create_tables.sql              — Supabase table definitions
tests/                               — Unit tests (pytest)
```
