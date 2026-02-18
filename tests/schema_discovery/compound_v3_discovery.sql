-- ============================================================
-- Query: Compound V3 Schema Discovery
-- Description: Discover available Compound V3 (Comet) tables
--              and their column schemas on Dune Analytics.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Purpose: Phase 0 - Validate table existence before production queries
-- ============================================================

-- Part 1: Find all Compound V3 Ethereum tables
SELECT
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema LIKE 'compound%'
  AND table_schema LIKE '%ethereum%'
ORDER BY table_schema, table_name
LIMIT 100
