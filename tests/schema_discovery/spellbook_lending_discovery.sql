-- ============================================================
-- Query: Spellbook Lending Schema Discovery
-- Description: Check for unified lending tables in Dune Spellbook.
--              Spellbook may provide pre-aggregated lending data
--              across multiple protocols.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Purpose: Phase 0 - Check if unified tables exist before building custom
-- ============================================================

-- Part 1: Find all lending-related Spellbook tables
SELECT
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema = 'lending'
   OR table_schema LIKE 'lending%'
ORDER BY table_schema, table_name
LIMIT 100
