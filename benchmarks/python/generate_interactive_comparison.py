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
from typing import Dict, List, Any, Tuple
from collections import defaultdict

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


def scan_available_datasets(data_dir: Path) -> List[Dict[str, str]]:
    """Scan data directory to find all available benchmark runs.

    Expected structure (polars-bio compatible):
    benchmark-data/
      tags/
        v0.1.0/
          {platform}/
            baseline/results/*.json
            target/results/*.json
            metadata.json
      commits/
        {short_sha}/
          {platform}/
            baseline/results/*.json
            target/results/*.json
    """
    datasets = []

    # Scan tags
    tags_dir = data_dir / "tags"
    if tags_dir.exists():
        for tag_dir in sorted(tags_dir.iterdir(), reverse=True):
            if tag_dir.is_dir() and (tag_dir / "benchmark-info.json").exists():
                datasets.append({
                    "type": "tag",
                    "name": tag_dir.name,
                    "path": str(tag_dir.relative_to(data_dir)),
                    "display": f"⭐ {tag_dir.name}"
                })

    # Scan commits
    commits_dir = data_dir / "commits"
    if commits_dir.exists():
        for commit_dir in sorted(commits_dir.iterdir(), reverse=True):
            if commit_dir.is_dir() and (commit_dir / "benchmark-info.json").exists():
                # Try to get more info from metadata
                info_file = commit_dir / "benchmark-info.json"
                try:
                    with open(info_file) as f:
                        info = json.load(f)
                        target_ref = info.get("target_ref", "")
                        commit_sha = commit_dir.name

                        # Format: branch(gitsha) or just gitsha if no branch
                        if target_ref and target_ref != commit_sha:
                            display_name = f"{target_ref}({commit_sha})"
                        else:
                            display_name = commit_sha
                except:
                    display_name = commit_dir.name

                datasets.append({
                    "type": "commit",
                    "name": commit_dir.name,
                    "path": str(commit_dir.relative_to(data_dir)),
                    "display": display_name
                })

    return datasets


def load_benchmark_results(results_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Load all benchmark JSON files from a directory, organized by platform."""
    results_by_platform = defaultdict(list)

    if not results_dir.exists():
        return results_by_platform

    # Scan for platform subdirectories
    for platform_dir in results_dir.iterdir():
        if not platform_dir.is_dir():
            continue

        platform = platform_dir.name

        # Look for JSON result files
        for json_file in platform_dir.rglob("*.json"):
            if json_file.name in ["linux.json", "macos.json"]:
                # Skip metadata files
                continue

            try:
                with open(json_file) as f:
                    result = json.load(f)
                    results_by_platform[platform].append(result)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load {json_file}: {e}", file=sys.stderr)

    return dict(results_by_platform)


def aggregate_results_by_category(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Aggregate benchmark results by category."""
    by_category = defaultdict(lambda: {"benchmarks": [], "total_time": 0.0})

    for result in results:
        category = result.get("category", "unknown")
        benchmark_name = result.get("benchmark_name", "")
        elapsed = result.get("metrics", {}).get("elapsed_seconds", 0.0)

        by_category[category]["benchmarks"].append({
            "name": benchmark_name,
            "elapsed": elapsed,
            "throughput": result.get("metrics", {}).get("throughput_records_per_sec", 0),
            "records": result.get("metrics", {}).get("total_records", 0)
        })
        by_category[category]["total_time"] += elapsed

    return dict(by_category)


def generate_html_report(data_dir: Path, output_file: Path):
    """Generate the interactive HTML comparison report."""

    print("Scanning for available benchmark datasets...")
    datasets = scan_available_datasets(data_dir)

    if not datasets:
        print("Warning: No benchmark datasets found", file=sys.stderr)

    # Convert datasets to JSON for embedding
    datasets_json = json.dumps(datasets)

    # Create data directory path mapping
    data_path_json = json.dumps(str(data_dir.resolve()))

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DataFusion Bio-Formats Benchmark Comparison</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }}
        .controls {{
            margin: 20px 0;
            padding: 20px;
            background-color: #f9f9f9;
            border-radius: 4px;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        .control-group {{
            display: flex;
            flex-direction: column;
        }}
        label {{
            font-weight: 600;
            margin-bottom: 5px;
            color: #555;
        }}
        select {{
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            background-color: white;
            cursor: pointer;
        }}
        select:hover {{
            border-color: #4CAF50;
        }}
        button {{
            padding: 10px 20px;
            background-color: #4CAF50;
            color: white;
            border: none;
            border-radius: 4px;
            font-size: 14px;
            cursor: pointer;
            margin-top: auto;
        }}
        button:hover {{
            background-color: #45a049;
        }}
        button:disabled {{
            background-color: #ccc;
            cursor: not-allowed;
        }}
        .chart {{
            margin: 30px 0;
        }}
        .info {{
            background-color: #e3f2fd;
            border-left: 4px solid #2196F3;
            padding: 15px;
            margin: 20px 0;
        }}
        .error {{
            background-color: #ffebee;
            border-left: 4px solid #f44336;
            padding: 15px;
            margin: 20px 0;
        }}
        .platform-tabs {{
            display: flex;
            gap: 10px;
            margin: 20px 0;
            border-bottom: 2px solid #ddd;
        }}
        .platform-tab {{
            padding: 10px 20px;
            cursor: pointer;
            border: none;
            background: none;
            font-size: 14px;
            color: #666;
            border-bottom: 3px solid transparent;
        }}
        .platform-tab.active {{
            color: #4CAF50;
            border-bottom-color: #4CAF50;
            font-weight: 600;
        }}
        .platform-tab:hover {{
            color: #4CAF50;
        }}
        #charts {{
            min-height: 400px;
        }}
        .loading {{
            text-align: center;
            padding: 40px;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 DataFusion Bio-Formats Benchmark Comparison</h1>

        <div class="info">
            <strong>Interactive Benchmark Comparison Tool</strong><br>
            Select a baseline version and a target version to compare performance across different platforms and benchmark categories.
        </div>

        <div class="controls">
            <div class="control-group">
                <label for="baseline-select">Baseline Version:</label>
                <select id="baseline-select">
                    <option value="">Select baseline...</option>
                </select>
            </div>

            <div class="control-group">
                <label for="target-select">Target Version:</label>
                <select id="target-select">
                    <option value="">Select target...</option>
                </select>
            </div>

            <div class="control-group">
                <label>&nbsp;</label>
                <button id="compare-btn" disabled>Generate Comparison</button>
            </div>
        </div>

        <div id="platform-tabs-container" style="display: none;">
            <div class="platform-tabs" id="platform-tabs"></div>
        </div>

        <div id="error-container"></div>
        <div id="charts"></div>

        <hr style="margin: 40px 0;">

        <p style="color: #666; text-align: center;">
            Generated with ❤️ by DataFusion Bio-Formats Benchmark Framework<br>
            🤖 <a href="https://github.com/biodatageeks/datafusion-bio-formats">View on GitHub</a>
        </p>
    </div>

    <script>
        // Embedded data
        const datasets = {datasets_json};
        const dataPath = {data_path_json};

        // State
        let currentPlatform = null;
        let baselineData = null;
        let targetData = null;
        let availablePlatforms = [];

        // Initialize dropdowns with optgroups for tags and commits
        function initializeDropdowns() {{
            const baselineSelect = document.getElementById('baseline-select');
            const targetSelect = document.getElementById('target-select');

            // Separate tags and commits
            const tags = datasets.filter(d => d.type === 'tag');
            const commits = datasets.filter(d => d.type === 'commit');

            // Add tags optgroup
            if (tags.length > 0) {{
                const baselineTagGroup = document.createElement('optgroup');
                baselineTagGroup.label = 'Tags';
                const targetTagGroup = document.createElement('optgroup');
                targetTagGroup.label = 'Tags';

                tags.forEach((dataset, index) => {{
                    const option1 = document.createElement('option');
                    option1.value = dataset.path;
                    option1.textContent = dataset.display;
                    baselineTagGroup.appendChild(option1);

                    const option2 = document.createElement('option');
                    option2.value = dataset.path;
                    option2.textContent = dataset.display;
                    targetTagGroup.appendChild(option2);

                    // Set latest tag as default baseline
                    if (index === 0) {{
                        option1.selected = true;
                    }}
                }});

                baselineSelect.appendChild(baselineTagGroup);
                targetSelect.appendChild(targetTagGroup);
            }}

            // Add commits optgroup
            if (commits.length > 0) {{
                const baselineCommitGroup = document.createElement('optgroup');
                baselineCommitGroup.label = 'Commits';
                const targetCommitGroup = document.createElement('optgroup');
                targetCommitGroup.label = 'Commits';

                commits.forEach((dataset, index) => {{
                    const option1 = document.createElement('option');
                    option1.value = dataset.path;
                    option1.textContent = dataset.display;
                    baselineCommitGroup.appendChild(option1);

                    const option2 = document.createElement('option');
                    option2.value = dataset.path;
                    option2.textContent = dataset.display;
                    targetCommitGroup.appendChild(option2);

                    // Set latest commit as default target
                    if (index === 0) {{
                        option2.selected = true;
                    }}
                }});

                baselineSelect.appendChild(baselineCommitGroup);
                targetSelect.appendChild(targetCommitGroup);
            }}

            // Enable compare button when both selections are made
            baselineSelect.addEventListener('change', validateSelections);
            targetSelect.addEventListener('change', validateSelections);

            // Initial validation
            validateSelections();
        }}

        function validateSelections() {{
            const baseline = document.getElementById('baseline-select').value;
            const target = document.getElementById('target-select').value;
            const compareBtn = document.getElementById('compare-btn');

            if (baseline && target && baseline !== target) {{
                compareBtn.disabled = false;
            }} else {{
                compareBtn.disabled = true;
            }}
        }}

        // Load benchmark data from a dataset path
        async function loadBenchmarkData(datasetPath) {{
            const baseUrl = window.location.origin + window.location.pathname.replace('/benchmark-comparison/index.html', '');
            const dataUrl = `${{baseUrl}}/benchmark-data/${{datasetPath}}`;

            // Load benchmark-info.json
            const infoResponse = await fetch(`${{dataUrl}}/benchmark-info.json`);
            if (!infoResponse.ok) {{
                throw new Error(`Failed to load benchmark info from ${{datasetPath}}`);
            }}
            const info = await infoResponse.json();

            // Discover available platforms
            const platforms = [];
            const results = {{}};

            // Try to load data from both linux and macos directories
            for (const platform of ['linux', 'macos']) {{
                try {{
                    const platformUrl = `${{dataUrl}}/${{platform}}/${{platform}}.json`;
                    const platformResponse = await fetch(platformUrl);
                    if (platformResponse.ok) {{
                        const platformInfo = await platformResponse.json();
                        platforms.push({{
                            name: platform,
                            label: platformInfo.runner || platform,
                            info: platformInfo
                        }});

                        // Load all JSON result files from baseline and target
                        const platformResults = [];
                        for (const variant of ['baseline', 'target']) {{
                            const variantUrl = `${{dataUrl}}/${{platform}}/${{variant}}/results`;
                            try {{
                                // We'll need to discover files - for now, try common patterns
                                // In production, you'd list directory contents or have an index
                                const testResponse = await fetch(`${{variantUrl}}/gff_parallelism_1threads.json`);
                                if (testResponse.ok) {{
                                    const result = await testResponse.json();
                                    result.variant = variant;
                                    platformResults.push(result);
                                }}
                            }} catch (e) {{
                                console.warn(`Could not load ${{variant}} results for ${{platform}}`, e);
                            }}
                        }}
                        results[platform] = platformResults;
                    }}
                }} catch (e) {{
                    console.warn(`Platform ${{platform}} not available`, e);
                }}
            }}

            return {{ platforms, results, info }};
        }}

        // Generate comparison charts
        async function generateComparison() {{
            const baseline = document.getElementById('baseline-select').value;
            const target = document.getElementById('target-select').value;

            if (!baseline || !target || baseline === target) {{
                return;
            }}

            const chartsDiv = document.getElementById('charts');
            const errorDiv = document.getElementById('error-container');
            errorDiv.innerHTML = '';
            chartsDiv.innerHTML = '<div class="loading">Loading benchmark data...</div>';

            try {{
                // Load both baseline and target data
                baselineData = await loadBenchmarkData(baseline);
                targetData = await loadBenchmarkData(target);

                // Find common platforms
                const baselinePlatforms = baselineData.platforms.map(p => p.name);
                const targetPlatforms = targetData.platforms.map(p => p.name);
                availablePlatforms = baselinePlatforms.filter(p => targetPlatforms.includes(p));

                if (availablePlatforms.length === 0) {{
                    errorDiv.innerHTML = `
                        <div class="error">
                            <strong>No common platforms found</strong><br>
                            Baseline has: ${{baselinePlatforms.join(', ')}}<br>
                            Target has: ${{targetPlatforms.join(', ')}}
                        </div>
                    `;
                    chartsDiv.innerHTML = '';
                    return;
                }}

                // Set up platform tabs
                const tabsContainer = document.getElementById('platform-tabs-container');
                const tabsDiv = document.getElementById('platform-tabs');
                tabsDiv.innerHTML = '';

                if (availablePlatforms.length > 1) {{
                    tabsContainer.style.display = 'block';
                    availablePlatforms.forEach((platform, index) => {{
                        const tab = document.createElement('button');
                        tab.className = 'platform-tab' + (index === 0 ? ' active' : '');
                        const platformInfo = baselineData.platforms.find(p => p.name === platform);
                        tab.textContent = platformInfo ? platformInfo.label : platform;
                        tab.onclick = () => switchPlatform(platform);
                        tabsDiv.appendChild(tab);
                    }});
                }} else if (availablePlatforms.length === 1) {{
                    tabsContainer.style.display = 'block';
                    const platformInfo = baselineData.platforms.find(p => p.name === availablePlatforms[0]);
                    tabsDiv.innerHTML = `<div style="padding: 10px; color: #666;">Platform: ${{platformInfo ? platformInfo.label : availablePlatforms[0]}}</div>`;
                }}

                // Display charts for first available platform
                currentPlatform = availablePlatforms[0];
                displayChartsForPlatform(currentPlatform);

            }} catch (error) {{
                console.error('Error loading benchmark data:', error);
                errorDiv.innerHTML = `
                    <div class="error">
                        <strong>Error loading benchmark data</strong><br>
                        ${{error.message}}<br><br>
                        This usually means benchmark data hasn't been generated yet.
                        Run the benchmark workflow from GitHub Actions to generate data.
                    </div>
                `;
                chartsDiv.innerHTML = '';
            }}
        }}

        // Switch between platforms
        function switchPlatform(platform) {{
            currentPlatform = platform;

            // Update tab styling
            document.querySelectorAll('.platform-tab').forEach(tab => {{
                tab.classList.remove('active');
                const platformInfo = baselineData.platforms.find(p => p.name === platform);
                if (tab.textContent === (platformInfo ? platformInfo.label : platform)) {{
                    tab.classList.add('active');
                }}
            }});

            displayChartsForPlatform(platform);
        }}

        // Display charts for a specific platform
        function displayChartsForPlatform(platform) {{
            const chartsDiv = document.getElementById('charts');

            const baselineResults = baselineData.results[platform] || [];
            const targetResults = targetData.results[platform] || [];

            if (baselineResults.length === 0 && targetResults.length === 0) {{
                chartsDiv.innerHTML = `
                    <div class="info">
                        <h3>No benchmark data available for ${{platform}}</h3>
                        <p>Run benchmarks on this platform to see comparison charts.</p>
                    </div>
                `;
                return;
            }}

            // Group results by category
            const categories = new Set();
            [...baselineResults, ...targetResults].forEach(r => {{
                if (r.category) categories.add(r.category);
            }});

            let html = '<div style="margin: 20px 0;">';
            html += `<h3>Benchmark Comparison</h3>`;
            html += `<p><strong>Baseline:</strong> ${{document.getElementById('baseline-select').selectedOptions[0].text}}</p>`;
            html += `<p><strong>Target:</strong> ${{document.getElementById('target-select').selectedOptions[0].text}}</p>`;
            html += '</div>';

            categories.forEach(category => {{
                html += `<div id="chart-${{category}}" class="chart"></div>`;
            }});

            chartsDiv.innerHTML = html;

            // Generate Plotly charts for each category
            categories.forEach(category => {{
                const baselineCategoryResults = baselineResults.filter(r => r.category === category);
                const targetCategoryResults = targetResults.filter(r => r.category === category);

                createComparisonChart(
                    `chart-${{category}}`,
                    category,
                    baselineCategoryResults,
                    targetCategoryResults
                );
            }});
        }}

        // Create a comparison chart using Plotly
        function createComparisonChart(divId, category, baselineResults, targetResults) {{
            const benchmarkNames = [...new Set([
                ...baselineResults.map(r => r.benchmark_name),
                ...targetResults.map(r => r.benchmark_name)
            ])];

            const baselineTimes = benchmarkNames.map(name => {{
                const result = baselineResults.find(r => r.benchmark_name === name);
                return result ? result.metrics.elapsed_seconds : 0;
            }});

            const targetTimes = benchmarkNames.map(name => {{
                const result = targetResults.find(r => r.benchmark_name === name);
                return result ? result.metrics.elapsed_seconds : 0;
            }});

            const trace1 = {{
                x: benchmarkNames,
                y: baselineTimes,
                name: 'Baseline',
                type: 'bar',
                marker: {{ color: '#636EFA' }}
            }};

            const trace2 = {{
                x: benchmarkNames,
                y: targetTimes,
                name: 'Target',
                type: 'bar',
                marker: {{ color: '#EF553B' }}
            }};

            const layout = {{
                title: `${{category}} Benchmarks`,
                xaxis: {{ title: 'Benchmark' }},
                yaxis: {{ title: 'Time (seconds)' }},
                barmode: 'group',
                height: 400
            }};

            Plotly.newPlot(divId, [trace1, trace2], layout);
        }}

        // Initialize on page load
        document.addEventListener('DOMContentLoaded', function() {{
            initializeDropdowns();

            document.getElementById('compare-btn').addEventListener('click', generateComparison);

            // Show welcome message if no datasets available
            if (datasets.length === 0) {{
                document.getElementById('charts').innerHTML = `
                    <div class="info">
                        <h3>No benchmark data available yet</h3>
                        <p>Run the benchmark workflow to generate comparison data.</p>
                        <p><strong>To generate benchmarks:</strong></p>
                        <ol>
                            <li>Go to the GitHub Actions tab</li>
                            <li>Select the "Benchmark" workflow</li>
                            <li>Click "Run workflow"</li>
                            <li>Select your options and run</li>
                        </ol>
                    </div>
                `;
            }}
        }});
    </script>
</body>
</html>
"""

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(html_content)

    print(f"✓ Report generated: {output_file}")
    print(f"  Found {len(datasets)} dataset(s)")


def main():
    parser = argparse.ArgumentParser(
        description="Generate interactive benchmark comparison report"
    )
    parser.add_argument(
        "data_dir",
        type=Path,
        help="Directory containing benchmark data (with tags/ and commits/ subdirs)"
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
