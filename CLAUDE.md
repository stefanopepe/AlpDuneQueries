# CLAUDE.md - AI Assistant Guide for DuneQueries

This document provides comprehensive guidance for AI assistants working with this repository.

## Repository Overview

**DuneQueries** is a collection of SQL queries for [Dune Analytics](https://dune.com/), a blockchain analytics platform that allows users to query on-chain data from various blockchains (Ethereum, Polygon, Arbitrum, Optimism, Solana, etc.) using SQL.

### Purpose

- Store and version-control Dune Analytics SQL queries
- Share reusable query patterns and templates
- Document blockchain data analysis techniques
- Maintain a library of tested, production-ready queries

### License

Apache License 2.0 - See LICENSE file for details.

## Directory Structure (Recommended)

When adding content to this repository, follow this structure:

```
DuneQueries/
├── CLAUDE.md              # This file - AI assistant guidelines
├── README.md              # Project overview and usage instructions
├── LICENSE                # Apache 2.0 License
├── queries/               # Main query directory
│   ├── ethereum/          # Ethereum mainnet queries
│   │   ├── defi/          # DeFi protocol queries
│   │   ├── nft/           # NFT marketplace queries
│   │   ├── tokens/        # Token analysis queries
│   │   └── wallets/       # Wallet analysis queries
│   ├── polygon/           # Polygon queries
│   ├── arbitrum/          # Arbitrum queries
│   ├── optimism/          # Optimism queries
│   ├── solana/            # Solana queries
│   └── cross-chain/       # Multi-chain queries
├── templates/             # Reusable query templates
├── spells/                # Dune Spellbook contributions
└── docs/                  # Additional documentation
```

## SQL Query Conventions

### File Naming

- Use lowercase with underscores: `token_transfer_analysis.sql`
- Include the main table or protocol: `uniswap_v3_swaps.sql`
- Be descriptive but concise: `daily_active_addresses.sql`

### Query File Structure

Each SQL file should include a header comment block:

```sql
-- ============================================================
-- Query: [Descriptive Name]
-- Description: [What the query does]
-- Author: [GitHub username or name]
-- Created: [YYYY-MM-DD]
-- Updated: [YYYY-MM-DD]
-- Dune Link: [Optional - link to live Dune query]
-- ============================================================
-- Parameters:
--   {{blockchain}} - Target blockchain (default: ethereum)
--   {{start_date}} - Analysis start date
--   {{end_date}} - Analysis end date
-- ============================================================

-- Query begins here
SELECT ...
```

### SQL Style Guide

1. **Keywords**: Use UPPERCASE for SQL keywords (`SELECT`, `FROM`, `WHERE`, `JOIN`)
2. **Identifiers**: Use lowercase for table and column names
3. **Indentation**: Use 2 or 4 spaces consistently (not tabs)
4. **Line breaks**:
   - New line for each major clause (`SELECT`, `FROM`, `WHERE`, etc.)
   - New line for each column in SELECT (for readability)
5. **Aliases**: Use meaningful aliases (`t` for transactions, `b` for blocks)
6. **Comments**: Use `--` for single-line comments

### Example Query Format

```sql
-- Daily ETH transfer volume
SELECT
    date_trunc('day', block_time) AS day,
    SUM(value / 1e18) AS eth_volume,
    COUNT(*) AS tx_count
FROM ethereum.transactions
WHERE
    block_time >= DATE '{{start_date}}'
    AND block_time < DATE '{{end_date}}'
    AND value > 0
GROUP BY 1
ORDER BY 1 DESC
```

## Dune Analytics Specifics

### Common Tables

**Ethereum:**
- `ethereum.transactions` - All transactions
- `ethereum.traces` - Internal transactions
- `ethereum.logs` - Event logs
- `tokens.erc20` - ERC20 token metadata
- `prices.usd` - Token prices

**Decoded Tables (Protocol-Specific):**
- `uniswap_v3_ethereum.Pair_evt_Swap`
- `aave_v3_ethereum.Pool_evt_Supply`
- `opensea_v2_ethereum.SeaportAdvanced_evt_OrderFulfilled`

### Dune Parameters

Use double curly braces for parameters:
- `{{blockchain}}` - Chain selector
- `{{start_date}}` - Date parameter
- `{{wallet_address}}` - Address parameter
- `{{token_symbol}}` - Token selector

### Spellbook Integration

[Dune Spellbook](https://github.com/duneanalytics/spellbook) provides curated, tested data models:
- `dex.trades` - Unified DEX trades across protocols
- `nft.trades` - Unified NFT trades
- `transfers.ethereum_erc20` - Token transfers

Prefer Spellbook tables over raw tables when available.

## Development Workflow

### Adding New Queries

1. Create the query file in the appropriate directory
2. Include the standard header comment block
3. Test the query on Dune before committing
4. Add any necessary documentation

### Testing Queries

- Always test queries on Dune Analytics before committing
- Verify results against known data points when possible
- Check query execution time and optimize if needed
- Test with different parameter values

### Optimization Tips

1. **Filter early**: Apply WHERE clauses as early as possible
2. **Limit date ranges**: Always include date filters for large tables
3. **Avoid SELECT ***: Only select columns you need
4. **Use appropriate aggregations**: Pre-aggregate when possible
5. **Index usage**: Filter on indexed columns (block_time, block_number)

## AI Assistant Guidelines

### When Writing Queries

1. Always include the header comment block
2. Use Dune SQL syntax (Trino/Presto SQL dialect)
3. Include date range parameters for time-series queries
4. Add comments for complex logic
5. Optimize for readability and performance

### When Modifying Existing Queries

1. Update the "Updated" date in the header
2. Preserve the original author information
3. Document what was changed and why
4. Test the modified query before committing

### When Reviewing Queries

Check for:
- SQL injection vulnerabilities (though Dune sanitizes parameters)
- Missing date filters on large tables
- Inefficient JOINs or subqueries
- Correct blockchain/table references
- Parameter usage for configurable values

### Common Mistakes to Avoid

1. Forgetting to divide token amounts by decimals (e.g., `/ 1e18` for ETH)
2. Using wrong table names for different chains
3. Missing NULL handling in aggregations
4. Not accounting for blockchain reorgs in recent data
5. Hardcoding values that should be parameters

## Git Workflow

### Commit Messages

Use clear, descriptive commit messages:
- `Add: uniswap v3 liquidity analysis query`
- `Fix: correct decimal handling in token transfers`
- `Update: optimize daily volume query performance`
- `Docs: add documentation for NFT queries`

### Branch Naming

- `feature/query-name` - New queries
- `fix/issue-description` - Bug fixes
- `docs/topic` - Documentation updates

## Resources

- [Dune Analytics Documentation](https://docs.dune.com/)
- [Dune Spellbook](https://github.com/duneanalytics/spellbook)
- [Trino SQL Documentation](https://trino.io/docs/current/)
- [Blockchain Data Tables Reference](https://docs.dune.com/data-tables/)

## Quick Reference

### Useful SQL Patterns

**Convert wei to ETH:**
```sql
value / 1e18 AS eth_amount
```

**Get USD value:**
```sql
SELECT
    t.value / 1e18 * p.price AS usd_value
FROM ethereum.transactions t
LEFT JOIN prices.usd p ON p.symbol = 'ETH'
    AND p.minute = date_trunc('minute', t.block_time)
```

**Address formatting:**
```sql
-- Lowercase addresses
LOWER(address) AS normalized_address

-- Checksum format (display)
'0x' || encode(address, 'hex')
```

**Time aggregations:**
```sql
date_trunc('day', block_time) AS day
date_trunc('week', block_time) AS week
date_trunc('month', block_time) AS month
```
