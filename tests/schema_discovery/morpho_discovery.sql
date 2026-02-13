-- ============================================================
-- Query: Morpho Schema Discovery
-- Description: Discover available Morpho tables and their schemas
--              on Dune Analytics. Morpho Blue is a permissionless
--              lending primitive with market IDs.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Purpose: Phase 0 - Validate table existence before production queries
-- ============================================================

-- Part 1: Find all Morpho-related table schemas
SELECT
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema LIKE 'morpho%'
ORDER BY table_schema, table_name
LIMIT 100
