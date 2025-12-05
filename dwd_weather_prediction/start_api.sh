#!/bin/bash
# Start the FastAPI weather prediction server

cd "$(dirname "$0")"
uv run uvicorn src.app:app --host 0.0.0.0 --port 8000
