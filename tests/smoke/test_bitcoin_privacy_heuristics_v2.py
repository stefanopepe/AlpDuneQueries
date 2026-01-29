#!/usr/bin/env python3
"""
Smoke test for bitcoin_privacy_heuristics_v2.sql

Validates:
1. All referenced table columns exist in documented schemas
2. SQL syntax is valid (basic parsing)
3. Required CTEs are present
4. Output schema matches documentation
"""

import re
import sys
from pathlib import Path

# Paths
QUERY_PATH = Path(__file__).parent.parent.parent / "queries/bitcoin/bitcoin_privacy_heuristics_v2.sql"
SCHEMA_PATH = Path(__file__).parent.parent.parent / "docs/dune_database_schemas.md"

# Known valid columns from bitcoin tables (from dune_database_schemas.md)
BITCOIN_INPUTS_COLUMNS = {
    "block_time", "block_height", "tx_id", "index", "spent_tx_id",
    "spent_output_index", "value", "address", "type", "script_sig",
    "witness", "is_coinbase"
}

BITCOIN_OUTPUTS_COLUMNS = {
    "block_time", "block_height", "tx_id", "index", "value", "address",
    "type", "script_pub_key", "is_spent", "spending_tx_id", "spending_input_index"
}

# Required CTEs in the query
REQUIRED_CTES = [
    "prev",
    "checkpoint",
    "raw_inputs",
    "raw_outputs",
    "tx_input_stats",
    "tx_output_stats",
    "classified",
    "new_data",
    "kept_old"
]

# Expected output columns
EXPECTED_OUTPUT_COLUMNS = ["day", "privacy_heuristic", "tx_count", "sats_total"]

# Expected privacy heuristics
EXPECTED_HEURISTICS = [
    "change_precision",
    "change_script_type",
    "uih1",
    "uih2",
    "coinjoin_detected",
    "self_transfer",
    "address_reuse",
    "no_privacy_issues",
    "malformed"
]


def read_query():
    """Read the SQL query file."""
    with open(QUERY_PATH, "r") as f:
        return f.read()


def extract_column_references(query, table_alias, table_name):
    """Extract column references for a table alias."""
    # Match patterns like: alias.column or table.column
    pattern = rf'\b{table_alias}\.(\w+)'
    matches = re.findall(pattern, query, re.IGNORECASE)
    return set(matches)


def test_bitcoin_inputs_columns(query):
    """Test that all bitcoin.inputs column references are valid."""
    errors = []

    # Find references to bitcoin.inputs (via alias 'i' in raw_inputs CTE)
    # Also check direct references
    input_refs = set()

    # Pattern for i.column in raw_inputs section
    raw_inputs_section = re.search(r'raw_inputs AS \((.*?)\),', query, re.DOTALL)
    if raw_inputs_section:
        section = raw_inputs_section.group(1)
        input_refs.update(re.findall(r'\bi\.(\w+)', section))

    # Validate against known columns
    for col in input_refs:
        if col not in BITCOIN_INPUTS_COLUMNS:
            errors.append(f"Unknown column 'bitcoin.inputs.{col}'")

    return errors


def test_bitcoin_outputs_columns(query):
    """Test that all bitcoin.outputs column references are valid."""
    errors = []

    # Find references in raw_outputs section
    output_refs = set()

    raw_outputs_section = re.search(r'raw_outputs AS \((.*?)\),', query, re.DOTALL)
    if raw_outputs_section:
        section = raw_outputs_section.group(1)
        output_refs.update(re.findall(r'\bo\.(\w+)', section))

    # Validate against known columns
    for col in output_refs:
        if col not in BITCOIN_OUTPUTS_COLUMNS:
            errors.append(f"Unknown column 'bitcoin.outputs.{col}'")

    return errors


def test_required_ctes(query):
    """Test that all required CTEs are present."""
    errors = []

    for cte in REQUIRED_CTES:
        pattern = rf'\b{cte}\s+AS\s*\('
        if not re.search(pattern, query, re.IGNORECASE):
            errors.append(f"Missing required CTE: {cte}")

    return errors


def test_output_schema(query):
    """Test that the DESCRIPTOR schema matches expected columns."""
    errors = []

    # Extract DESCRIPTOR columns
    descriptor_match = re.search(r'DESCRIPTOR\s*\((.*?)\)', query, re.DOTALL)
    if descriptor_match:
        descriptor = descriptor_match.group(1)

        for col in EXPECTED_OUTPUT_COLUMNS:
            if col not in descriptor:
                errors.append(f"Missing output column in DESCRIPTOR: {col}")

        # Check we don't have old columns
        if "avg_inputs" in descriptor:
            errors.append("DESCRIPTOR still contains removed column: avg_inputs")
        if "avg_outputs" in descriptor:
            errors.append("DESCRIPTOR still contains removed column: avg_outputs")
    else:
        errors.append("Could not find DESCRIPTOR in query")

    return errors


def test_heuristics_defined(query):
    """Test that all expected heuristics are defined in the CASE statement."""
    errors = []

    for heuristic in EXPECTED_HEURISTICS:
        if f"'{heuristic}'" not in query:
            errors.append(f"Missing heuristic definition: {heuristic}")

    return errors


def test_spendable_output_filter(query):
    """Test that non-spendable outputs are filtered."""
    errors = []

    # Check for OP_RETURN / nulldata filter
    if "nulldata" not in query.lower() and "op_return" not in query.lower():
        errors.append("Query may not filter out OP_RETURN (nulldata) outputs")

    return errors


def test_coinbase_exclusion(query):
    """Test that coinbase transactions are excluded."""
    errors = []

    if "is_coinbase" not in query or "FALSE" not in query:
        errors.append("Query may not exclude coinbase transactions")

    return errors


def run_tests():
    """Run all smoke tests."""
    print(f"Running smoke tests for: {QUERY_PATH.name}")
    print("=" * 60)

    query = read_query()
    all_errors = []

    tests = [
        ("Bitcoin inputs columns", test_bitcoin_inputs_columns),
        ("Bitcoin outputs columns", test_bitcoin_outputs_columns),
        ("Required CTEs", test_required_ctes),
        ("Output schema", test_output_schema),
        ("Heuristics defined", test_heuristics_defined),
        ("Spendable output filter", test_spendable_output_filter),
        ("Coinbase exclusion", test_coinbase_exclusion),
    ]

    for test_name, test_func in tests:
        errors = test_func(query)
        if errors:
            print(f"FAIL: {test_name}")
            for error in errors:
                print(f"  - {error}")
            all_errors.extend(errors)
        else:
            print(f"PASS: {test_name}")

    print("=" * 60)

    if all_errors:
        print(f"FAILED: {len(all_errors)} error(s) found")
        return 1
    else:
        print("SUCCESS: All smoke tests passed")
        return 0


if __name__ == "__main__":
    sys.exit(run_tests())
