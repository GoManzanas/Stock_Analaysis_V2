# Project Conventions

## Language & Runtime

- **Python 3.12+**
- No type checker enforced yet, but use type hints in function signatures

## Dependencies

- **pip** with `requirements.txt`
- Core: `click`, `rich`, `requests`, `python-dotenv`
- Testing: `pytest`

## Environment Variables

- Stored in `.env` (gitignored), loaded via `python-dotenv`
- `.env.example` is checked in as a template
- Only variable: `EODHD_API_KEY`

## Database

- **SQLite** in WAL mode, stored at `data/fund_analyst.db` (gitignored)
- No ORM — raw SQL with parameterized queries
- All dates stored as ISO 8601 TEXT
- Schema defined in `db/schema.sql`

## Testing

- **pytest** for unit tests
- Tests in `tests/` directory
- Mock external APIs (EODHD, SEC) in tests — never hit real endpoints
- Use `tmp_path` fixture for temporary databases

## Git Conventions

- Branch naming: `slicezero/<descriptive-slug>`
- Commit messages: explain _why_, not just _what_
