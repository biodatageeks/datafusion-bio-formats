#!/usr/bin/env python3
"""
Generate interactive HTML benchmark comparison report with historical data selection.
Based on polars-bio's implementation - simplified dropdowns, dynamic tabs, improved styling.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


def load_index(data_dir: Path) -> Dict[str, Any]:
    """Load the master index of all benchmark datasets."""
    index_file = data_dir / "index.json"
    if not index_file.exists():
        return {"datasets": [], "tags": [], "latest_tag": None, "last_updated": ""}

    with open(index_file) as f:
        return json.load(f)


def organize_datasets_by_ref(index_data: Dict[str, Any]) -> Dict[str, Dict]:
    """
    Organize datasets by ref, grouping runners under each ref.
    For branches, each commit gets a unique entry using ref@sha as key.

    Returns:
        refs_by_type: {
            "tag": {
                "v0.1.1": {
                    "label": "v0.1.1",
                    "ref": "v0.1.1",
                    "ref_type": "tag",
                    "commit_sha": "abc123",
                    "is_latest_tag": True,
                    "runners": {
                        "linux": "tag-v0.1.1-linux",
                        "macos": "tag-v0.1.1-macos"
                    }
                }
            },
            "branch": {
                "benchmarking@abc123": {
                    "label": "benchmarking(abc123)",
                    "ref": "benchmarking",
                    "ref_type": "branch",
                    "commit_sha": "abc123",
                    "is_latest_tag": False,
                    "runners": {
                        "linux": "benchmarking@abc123@linux",
                        "macos": "benchmarking@abc123@macos"
                    }
                }
            }
        }
    """
    refs_by_type = {"tag": {}, "branch": {}}

    for dataset in index_data.get("datasets", []):
        ref = dataset["ref"]
        ref_type = dataset["ref_type"]
        runner = dataset["runner"]
        commit_sha = dataset.get("commit_sha", "unknown")

        # For branches, use ref@sha as unique key; for tags, use ref name
        if ref_type == "branch":
            unique_key = f"{ref}@{commit_sha}"
            # Use the dataset ID directly (should be ref@sha@runner format from workflow)
            dataset_id = dataset["id"]
        else:
            unique_key = ref
            dataset_id = dataset["id"]

        # Create ref entry if it doesn't exist
        if unique_key not in refs_by_type[ref_type]:
            refs_by_type[ref_type][unique_key] = {
                "label": dataset["label"],
                "ref": ref,
                "ref_type": ref_type,
                "commit_sha": commit_sha,
                "is_latest_tag": dataset.get("is_latest_tag", False),
                "runners": {},
            }

        # Add this dataset to the runners dict
        refs_by_type[ref_type][unique_key]["runners"][runner] = dataset_id

    return refs_by_type


def load_dataset_results(data_dir: Path, dataset_id: str, dataset_info: Dict) -> Dict:
    """
    Load benchmark results for a specific dataset.

    For now, returns metadata only since our benchmark results are in JSON format
    and need custom parsing. This will be extended based on actual result format.
    """
    dataset_path = data_dir / dataset_info.get("path", "")

    if not dataset_path.exists():
        return None

    # Load metadata
    metadata = {}
    for metadata_file in [dataset_path / "metadata.json", dataset_path.parent / "metadata.json"]:
        if metadata_file.exists():
            with open(metadata_file) as f:
                metadata = json.load(f)
            break

    # For now, return basic structure
    # TODO: Load actual benchmark results from JSON files
    return {
        "id": dataset_id,
        "label": dataset_info["label"],
        "ref": dataset_info["ref"],
        "runner": dataset_info.get("runner", "unknown"),
        "runner_label": dataset_info.get("runner_label", "Unknown"),
        "metadata": metadata,
        "results": {},  # Will be populated when we parse result files
    }


def generate_html_report(data_dir: Path, output_file: Path):
    """Generate interactive HTML comparison report."""

    print("Loading benchmark index...")
    index = load_index(data_dir)

    if not index.get("datasets"):
        print("Warning: No benchmark datasets found in index", file=sys.stderr)

    # Organize datasets by ref type
    refs_by_type = organize_datasets_by_ref(index)

    print(f"Found {len(index.get('datasets', []))} total datasets")
    print(f"  Tags: {len(refs_by_type['tag'])}")
    print(f"  Branches/Commits: {len(refs_by_type['branch'])}")

    # Load all dataset metadata (lightweight - just metadata for now)
    all_datasets = {}
    for dataset in index.get("datasets", []):
        dataset_data = load_dataset_results(data_dir, dataset["id"], dataset)
        if dataset_data:
            all_datasets[dataset["id"]] = dataset_data

    # Generate HTML
    html = generate_html_template(index, all_datasets, refs_by_type)

    # Write output
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html)

    print(f"\n✅ Interactive report generated: {output_file}")


def generate_html_template(index: Dict, datasets: Dict, refs_by_type: Dict) -> str:
    """Generate the complete HTML template."""

    # Embed all data as JSON
    embedded_data = {
        "index": index,
        "datasets": datasets,
        "refs_by_type": refs_by_type,
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DataFusion Bio-Formats Benchmark Comparison</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            padding: 20px;
            background-color: #f5f5f5;
        }}

        /* Selection Panel Styles */
        .selection-panel {{
            background-color: white;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .selection-panel h2 {{
            margin: 0 0 15px 0;
            color: #333;
            font-size: 18px;
            font-weight: 600;
        }}

        .selection-row {{
            display: flex;
            align-items: center;
            gap: 15px;
            margin-bottom: 15px;
        }}

        .selection-row label {{
            font-weight: 600;
            min-width: 80px;
            color: #495057;
        }}

        .selection-row select {{
            flex: 1;
            padding: 10px 15px;
            border: 1px solid #ced4da;
            border-radius: 4px;
            font-size: 14px;
            background: white;
            cursor: pointer;
        }}

        .selection-row select:focus {{
            outline: none;
            border-color: #007bff;
            box-shadow: 0 0 0 3px rgba(0,123,255,0.25);
        }}

        .vs-label {{
            font-weight: 700;
            color: #6c757d;
            font-size: 18px;
            padding: 0 10px;
        }}

        .button-group {{
            display: flex;
            gap: 10px;
            margin-top: 15px;
        }}

        button {{
            padding: 10px 20px;
            border: none;
            border-radius: 4px;
            font-size: 14px;
            cursor: pointer;
            font-weight: 500;
        }}

        .btn-primary {{
            background: #007bff;
            color: white;
        }}

        .btn-primary:hover {{
            background: #0056b3;
        }}

        .btn-secondary {{
            background: #6c757d;
            color: white;
        }}

        .btn-secondary:hover {{
            background: #545b62;
        }}

        /* Header Styles */
        .header {{
            background-color: white;
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        h1 {{
            margin: 0 0 10px 0;
            color: #333;
        }}

        .subtitle {{
            color: #666;
            font-size: 14px;
        }}

        /* Runner Tabs - More Visible */
        .runner-tabs-wrapper {{
            background-color: white;
            padding: 15px 20px 0 20px;
            margin-bottom: 0;
            border-radius: 8px 8px 0 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        .runner-tabs {{
            display: flex;
            gap: 10px;
            border-bottom: 2px solid #e9ecef;
        }}

        .runner-tab {{
            padding: 12px 24px;
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-bottom: none;
            border-radius: 6px 6px 0 0;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            color: #495057;
            transition: all 0.2s;
            margin-bottom: -2px;
        }}

        .runner-tab:hover {{
            background: #e9ecef;
        }}

        .runner-tab.active {{
            background: white;
            color: #007bff;
            border-color: #007bff;
            border-bottom-color: white;
        }}

        /* Chart Container Styles */
        .chart-container {{
            background-color: white;
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        h2 {{
            margin-top: 0;
            color: #333;
        }}

        .loading {{
            text-align: center;
            padding: 40px;
            color: #6c757d;
        }}

        .error {{
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }}

        .info {{
            background: #d1ecf1;
            border: 1px solid #bee5eb;
            color: #0c5460;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }}

        optgroup {{
            font-weight: 600;
        }}
    </style>
</head>
<body>
    <div class="selection-panel">
        <h2>📊 Select Datasets to Compare</h2>

        <div class="selection-row">
            <label for="baseline-select">Baseline:</label>
            <select id="baseline-select">
                <option value="">Loading...</option>
            </select>
        </div>

        <div class="selection-row">
            <span class="vs-label">vs</span>
        </div>

        <div class="selection-row">
            <label for="target-select">Target:</label>
            <select id="target-select">
                <option value="">Loading...</option>
            </select>
        </div>

        <div class="button-group">
            <button class="btn-primary" onclick="app.loadComparison()">Compare</button>
            <button class="btn-secondary" onclick="app.resetToDefault()">Reset to Default</button>
        </div>
    </div>

    <div id="runner-tabs-container"></div>
    <div id="charts-container"></div>

    <script>
        // Embedded data
        const DATA = {json.dumps(embedded_data, indent=2)};

        // Application state
        const app = {{
            currentBaseline: null,  // unique key (ref or ref@sha)
            currentTarget: null,    // unique key (ref or ref@sha)
            currentRunner: null,
            availableRunners: [],

            init() {{
                this.populateDropdowns();
                this.setDefaults();
            }},

            populateDropdowns() {{
                const baselineSelect = document.getElementById('baseline-select');
                const targetSelect = document.getElementById('target-select');

                baselineSelect.innerHTML = '';
                targetSelect.innerHTML = '';

                // Safety check for refs_by_type
                if (!DATA.refs_by_type) {{
                    console.error('DATA.refs_by_type is not defined');
                    return;
                }}

                // Tags
                const tags = DATA.refs_by_type.tag ? Object.entries(DATA.refs_by_type.tag).map(([key, data]) => ({{
                    key: key,
                    ...data
                }})) : [];

                if (tags.length > 0) {{
                    const tagGroup = document.createElement('optgroup');
                    tagGroup.label = 'Tags';

                    tags.forEach(ref => {{
                        const option = document.createElement('option');
                        option.value = ref.key;
                        option.textContent = ref.label + (ref.is_latest_tag ? ' ⭐ Latest' : '');
                        tagGroup.appendChild(option);
                    }});

                    baselineSelect.appendChild(tagGroup.cloneNode(true));
                    targetSelect.appendChild(tagGroup.cloneNode(true));
                }}

                // Branches (each commit gets a separate entry)
                const branches = DATA.refs_by_type.branch ? Object.entries(DATA.refs_by_type.branch).map(([key, data]) => ({{
                    key: key,
                    ...data
                }})) : [];

                if (branches.length > 0) {{
                    const branchGroup = document.createElement('optgroup');
                    branchGroup.label = 'Branches/Commits';

                    branches.forEach(ref => {{
                        const option = document.createElement('option');
                        option.value = ref.key;  // Use unique key (ref@sha)
                        option.textContent = ref.label;  // Display with commit SHA
                        branchGroup.appendChild(option);
                    }});

                    baselineSelect.appendChild(branchGroup.cloneNode(true));
                    targetSelect.appendChild(branchGroup.cloneNode(true));
                }}
            }},

            setDefaults() {{
                // Find latest tag
                const latestTagEntry = DATA.refs_by_type.tag ?
                    Object.entries(DATA.refs_by_type.tag).find(([key, ref]) => ref.is_latest_tag) : null;

                // Find first branch (most recent commit)
                const firstBranchEntry = DATA.refs_by_type.branch ?
                    Object.entries(DATA.refs_by_type.branch)[0] : null;
                const targetEntry = firstBranchEntry ||
                    (DATA.refs_by_type.tag ? Object.entries(DATA.refs_by_type.tag)[0] : null);

                if (latestTagEntry) {{
                    const [tagKey, tagData] = latestTagEntry;
                    document.getElementById('baseline-select').value = tagKey;
                    this.currentBaseline = tagKey;
                }}

                if (targetEntry) {{
                    const [targetKey, targetData] = targetEntry;
                    document.getElementById('target-select').value = targetKey;
                    this.currentTarget = targetKey;
                }}

                // Auto-load comparison if both are set
                if (this.currentBaseline && this.currentTarget) {{
                    this.loadComparison();
                }}
            }},

            resetToDefault() {{
                this.setDefaults();
                this.loadComparison();
            }},

            getRefData(refKey) {{
                // Find ref in tags or branches using unique key
                return DATA.refs_by_type.tag[refKey] || DATA.refs_by_type.branch[refKey];
            }},

            loadComparison() {{
                const baselineRef = document.getElementById('baseline-select').value;
                const targetRef = document.getElementById('target-select').value;

                if (!baselineRef || !targetRef) {{
                    alert('Please select both baseline and target datasets');
                    return;
                }}

                if (baselineRef === targetRef) {{
                    alert('Please select different datasets for comparison');
                    return;
                }}

                this.currentBaseline = baselineRef;
                this.currentTarget = targetRef;

                const baselineRefData = this.getRefData(baselineRef);
                const targetRefData = this.getRefData(targetRef);

                if (!baselineRefData || !targetRefData) {{
                    document.getElementById('charts-container').innerHTML =
                        '<div class="error">Error: Could not load dataset data</div>';
                    return;
                }}

                // Find common runners
                const baselineRunners = Object.keys(baselineRefData.runners);
                const targetRunners = Object.keys(targetRefData.runners);
                const commonRunners = baselineRunners.filter(r => targetRunners.includes(r));

                if (commonRunners.length === 0) {{
                    document.getElementById('charts-container').innerHTML =
                        '<div class="error">Error: No common runners between selected datasets</div>';
                    return;
                }}

                this.availableRunners = commonRunners;

                // Setup runner tabs
                this.setupRunnerTabs();

                // Generate charts for first runner
                this.currentRunner = commonRunners[0];
                this.generateCharts();
            }},

            setupRunnerTabs() {{
                const tabsContainer = document.getElementById('runner-tabs-container');

                if (this.availableRunners.length === 1) {{
                    // Single runner - show simple label
                    const runner = this.availableRunners[0];
                    const baselineRefData = this.getRefData(this.currentBaseline);
                    const datasetId = baselineRefData.runners[runner];
                    const dataset = DATA.datasets[datasetId];

                    tabsContainer.innerHTML = `
                        <div class="runner-tabs-wrapper">
                            <div class="runner-tabs">
                                <div class="runner-tab active">
                                    ${{dataset.runner_label}}
                                </div>
                            </div>
                        </div>
                    `;
                }} else {{
                    // Multiple runners - show clickable tabs
                    const tabs = this.availableRunners.map((runner, idx) => {{
                        const baselineRefData = this.getRefData(this.currentBaseline);
                        const datasetId = baselineRefData.runners[runner];
                        const dataset = DATA.datasets[datasetId];
                        const active = idx === 0 ? 'active' : '';

                        return `<button class="runner-tab ${{active}}" onclick="app.switchRunner('${{runner}}')">
                            ${{dataset.runner_label}}
                        </button>`;
                    }}).join('');

                    tabsContainer.innerHTML = `
                        <div class="runner-tabs-wrapper">
                            <div class="runner-tabs">
                                ${{tabs}}
                            </div>
                        </div>
                    `;
                }}
            }},

            switchRunner(runner) {{
                this.currentRunner = runner;

                // Update active tab
                document.querySelectorAll('.runner-tab').forEach(tab => {{
                    tab.classList.remove('active');
                }});
                event.target.classList.add('active');

                // Regenerate charts
                this.generateCharts();
            }},

            generateCharts() {{
                const container = document.getElementById('charts-container');
                const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';

                // Get datasets for current runner
                const baselineRefData = this.getRefData(this.currentBaseline);
                const targetRefData = this.getRefData(this.currentTarget);

                const baselineDatasetId = baselineRefData.runners[this.currentRunner];
                const targetDatasetId = targetRefData.runners[this.currentRunner];

                const baseline = DATA.datasets[baselineDatasetId];
                const target = DATA.datasets[targetDatasetId];

                if (!baseline || !target) {{
                    container.innerHTML = `
                        <div class="error">
                            <strong>Error: Dataset not found</strong><br>
                            Baseline ID: ${{baselineDatasetId}}<br>
                            Target ID: ${{targetDatasetId}}
                        </div>
                    `;
                    return;
                }}

                // Generate header
                let html = `
                    <div class="header">
                        <h1>DataFusion Bio-Formats Benchmark Comparison</h1>
                        <div class="subtitle">
                            <strong>Baseline:</strong> ${{baseline.label}} &nbsp;|&nbsp;
                            <strong>Target:</strong> ${{target.label}} &nbsp;|&nbsp;
                            <strong>Platform:</strong> ${{baseline.runner_label}} &nbsp;|&nbsp;
                            <strong>Generated:</strong> ${{timestamp}}
                        </div>
                    </div>
                `;

                // TODO: Add actual benchmark charts when result parsing is implemented
                html += `
                    <div class="info">
                        <h3>Benchmark data loaded successfully</h3>
                        <p><strong>Baseline:</strong> ${{baseline.label}} (${{baseline.ref}})</p>
                        <p><strong>Target:</strong> ${{target.label}} (${{target.ref}})</p>
                        <p><strong>Platform:</strong> ${{baseline.runner_label}}</p>
                        <br>
                        <p><em>Chart generation will be implemented when benchmark result files are available.</em></p>
                        <p><em>The framework is ready - we just need to parse the actual benchmark JSON/CSV files.</em></p>
                    </div>
                `;

                container.innerHTML = html;
            }}
        }};

        // Initialize app
        document.addEventListener('DOMContentLoaded', () => {{
            app.init();
        }});
    </script>
</body>
</html>
"""

    return html


def main():
    parser = argparse.ArgumentParser(
        description="Generate interactive benchmark comparison report"
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="Directory containing benchmark-data (with index.json)"
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="Output HTML file path"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    try:
        generate_html_report(args.data_dir, args.output_file)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
