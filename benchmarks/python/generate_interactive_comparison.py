#!/usr/bin/env python3
"""
Generate interactive HTML comparison report for benchmarks.

This script creates an interactive HTML page with Plotly charts comparing
benchmark results across different versions, platforms (Linux/macOS), and
test categories (parallelism, predicate pushdown, projection pushdown).

Usage:
    python generate_interactive_comparison.py <data_dir> <output_html>

Example:
    python generate_interactive_comparison.py benchmark/data benchmark/comparison.html
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import pandas as pd
except ImportError as e:
    print(f"Error: {e}", file=sys.stderr)
    print("\nPlease install required dependencies:", file=sys.stderr)
    print("  pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)


def load_index(data_dir: Path) -> Dict[str, Any]:
    """Load the master index of all benchmark datasets."""
    index_file = data_dir / "index.json"
    if not index_file.exists():
        return {"datasets": []}

    with open(index_file) as f:
        return json.load(f)


def load_benchmark_results(result_file: Path) -> List[Dict[str, Any]]:
    """Load benchmark results from a JSON file."""
    if not result_file.exists():
        return []

    with open(result_file) as f:
        return json.load(f)


def generate_html_report(data_dir: Path, output_file: Path):
    """Generate the interactive HTML comparison report."""

    print("Loading benchmark data...")
    index = load_index(data_dir)

    # For MVP, create a simple stub HTML
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DataFusion Bio-Formats Benchmark Comparison</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }
        .controls {
            margin: 20px 0;
            padding: 15px;
            background-color: #f9f9f9;
            border-radius: 4px;
        }
        select {
            padding: 8px 12px;
            margin: 5px 10px 5px 0;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }
        .chart {
            margin: 30px 0;
        }
        .info {
            background-color: #e3f2fd;
            border-left: 4px solid #2196F3;
            padding: 15px;
            margin: 20px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 DataFusion Bio-Formats Benchmark Comparison</h1>

        <div class="info">
            <strong>Note:</strong> This is a minimal viable version of the benchmark comparison tool.
            Full interactive features (baseline/target selection, platform switching, detailed charts)
            will be implemented in future iterations.
        </div>

        <div class="controls">
            <label>
                <strong>Baseline:</strong>
                <select id="baseline-select">
                    <option>Select baseline version...</option>
                </select>
            </label>

            <label>
                <strong>Target:</strong>
                <select id="target-select">
                    <option>Select target version...</option>
                </select>
            </label>

            <label>
                <strong>Platform:</strong>
                <select id="platform-select">
                    <option value="linux">Linux</option>
                    <option value="macos">macOS</option>
                </select>
            </label>
        </div>

        <div id="charts"></div>

        <hr style="margin: 40px 0;">

        <p style="color: #666; text-align: center;">
            Generated with ❤️ by DataFusion Bio-Formats Benchmark Framework<br>
            🤖 <a href="https://github.com/biodatageeks/datafusion-bio-formats">View on GitHub</a>
        </p>
    </div>

    <script>
        // Stub implementation - will be enhanced in future iterations
        console.log('Benchmark comparison tool loaded');

        // TODO: Load and display actual benchmark data
        // TODO: Implement interactive baseline/target switching
        // TODO: Generate Plotly charts with comparison data
        // TODO: Add platform-specific filtering
    </script>
</body>
</html>
"""

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(html_content)

    print(f"✓ Report generated: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate interactive benchmark comparison report"
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="Directory containing benchmark data (with index.json)"
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="Output HTML file path"
    )

    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    generate_html_report(args.data_dir, args.output_file)


if __name__ == "__main__":
    main()
