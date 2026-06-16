#!/usr/bin/env python3
"""Merge current benchmark run with historical data, keeping last 10 runs."""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

def main():
    # Load current run
    current_file = Path('bundle/bench-current.json')
    if not current_file.exists():
        print(f"Error: {current_file} not found", file=sys.stderr)
        sys.exit(1)
    
    current = json.loads(current_file.read_text())
    current_run_id = str(current.get('run_id', ''))
    
    # Load existing history
    bench_repo = Path('bench-repo')
    bench_repo.mkdir(exist_ok=True)
    
    history_file = bench_repo / 'history.json'
    entries = []
    
    if history_file.exists():
        try:
            payload = json.loads(history_file.read_text())
            if isinstance(payload, dict) and 'entries' in payload:
                entries = payload['entries']
            elif isinstance(payload, list):
                entries = payload
        except Exception as e:
            print(f"Warning: Could not parse history: {e}", file=sys.stderr)
    
    # Merge: add current, then previous entries not matching current run_id
    merged = [current]
    for item in entries:
        if str(item.get('run_id', '')) != current_run_id:
            merged.append(item)
    
    # Keep only last 10
    merged = merged[:10]
    
    # Write history
    output = {
        'schema_version': 1,
        'updated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'entries': merged,
    }
    
    (bench_repo / 'history.json').write_text(json.dumps(output, indent=2))
    (bench_repo / 'current.json').write_text(json.dumps(current, indent=2))
    
    # Generate README
    rows = []
    for entry in merged:
        run_num = entry.get('run_number', 'n/a')
        gen_at = entry.get('generated_at', 'n/a')
        rows.append(f"- Run {run_num}: {gen_at}")
    
    repo = "MWild67/event-sourcing-testbed"
    readme = f"""# Benchmark History (Last 10 Runs)

Public rolling benchmark data for regression tracking.

**Latest URLs:**
- Current: https://raw.githubusercontent.com/{repo}/bench-history/current.json
- History: https://raw.githubusercontent.com/{repo}/bench-history/history.json

## Recent Runs

{chr(10).join(rows)}
"""
    
    (bench_repo / 'README.md').write_text(readme)
    print(f"Updated history: {len(merged)} entries", flush=True)

if __name__ == '__main__':
    main()
