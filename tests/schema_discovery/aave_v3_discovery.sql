-- ============================================================
-- Query: Aave V3 Schema Discovery
-- Description: Discover available Aave V3 Pool event tables
--              and their column schemas on Dune Analytics.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Purpose: Phase 0 - Validate table existence before production queries
-- ============================================================

-- Part 1: Find all Aave V3 Ethereum Pool event tables
SELECT
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema = 'aave_v3_ethereum'
  AND table_name LIKE 'Pool%'
ORDER BY table_name
LIMIT 50
