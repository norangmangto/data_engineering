# CMS Medicare Claims Dimensional Model (DE-SynPUF)

This project builds a star schema for synthetic Medicare claims data (DE-SynPUF 2008-2010).

## Highlights
- **Multi-grain fact tables**: Inpatient, Outpatient, Carrier, and Prescription Drug Events.
- **Bridge tables**: Many-to-many relationships for diagnoses and procedures.
- **Healthcare metrics**: length of stay, payment amounts, and chronic conditions.

## Quick Start

### 1. Install Dependencies
```bash
uv sync
```

### 2. Download Data
```bash
uv run python src/download_data.py
```

### 3. Run dbt
```bash
cd transform
uv run dbt run --profiles-dir .
uv run dbt test --profiles-dir .
```
