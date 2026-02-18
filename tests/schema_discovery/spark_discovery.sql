-- ============================================================
-- Query: Spark Protocol Schema Discovery
-- Description: Discover available Spark (MakerDAO lending) tables
--              on Dune Analytics. Spark uses Aave V3 architecture.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Purpose: Phase 0 - Validate table existence before production queries
-- ============================================================

-- Part 1: Find all Spark Ethereum tables
SELECT
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema LIKE 'spark%'
  AND table_schema LIKE '%ethereum%'
ORDER BY table_schema, table_name
LIMIT 100
