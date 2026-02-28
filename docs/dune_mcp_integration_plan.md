# Dune MCP Integration Plan

## Goal

Adopt Dune MCP in a consistent, secure, and repeatable way for this repository, using the official endpoint:

- `https://api.dune.com/mcp/v1`

Reference guide: <https://docs.dune.com/api-reference/agents/mcp>

## Scope

This plan covers:

- local AI client setup (Codex, Claude Code/Desktop, OpenCode)
- authentication standardization
- repository documentation updates
- rollout checks

This plan does **not** require committing local MCP client config files with secrets.

## Standards (Single Way of Working)

1. API key source:
- Use `DUNE_API_KEY` as the primary environment variable.
- Keep `DUNE_API_KEY_SPEPE` only as temporary backward compatibility in code until migration is complete.

2. MCP endpoint:
- Always use `https://api.dune.com/mcp/v1`.

3. Authentication mode:
- Prefer header auth (`x-dune-api-key`) when the client supports headers.
- Use query auth (`?api_key=...`) only for clients that require URL auth.

4. Naming:
- Use `dune_prod` as the default MCP server alias across tools.

5. Secrets handling:
- Never commit API keys, local MCP config files, or shell history snippets containing keys.
- Keep credentials in local environment only (`.env`, shell profile, or secure secret manager).

## Client Setup Baseline

### Codex

Use URL auth as documented by Dune for Codex:

```bash
codex mcp add dune_prod --url "https://api.dune.com/mcp/v1?api_key=$DUNE_API_KEY"
```

### Claude Code

Use header auth:

```bash
claude mcp add --scope user --transport http dune_prod https://api.dune.com/mcp/v1 --header "x-dune-api-key: $DUNE_API_KEY"
```

### Claude Desktop / OpenCode

Use the same endpoint and prefer header auth where supported by their config format.

## Repository Changes to Make (Planned)

1. Documentation:
- Add a `Dune MCP` section in root `README.md` with:
  - endpoint
  - auth modes
  - Codex command
  - security notes
- Add a short pointer in `CLAUDE.md` to keep AI workflows aligned with MCP usage.

2. Environment consistency:
- Keep `.env.example` focused on `DUNE_API_KEY`.
- In code, progressively migrate call sites to prefer `DUNE_API_KEY` first.

3. Optional helper:
- Add a small non-secret helper script (`scripts/setup_mcp.sh`) that validates `DUNE_API_KEY` and prints/setup commands for each client.

## Rollout Steps

1. Phase 1 (docs + baseline):
- land this plan
- update `README.md` and `CLAUDE.md`

2. Phase 2 (client enablement):
- each developer configures `dune_prod` locally in their client(s)
- run a smoke check by calling at least one discovery tool (e.g., `searchTables`)

3. Phase 3 (cleanup):
- remove legacy `DUNE_API_KEY_SPEPE` references after migration window
- keep one canonical env var (`DUNE_API_KEY`)

## Acceptance Criteria

- Documentation includes one canonical MCP endpoint and alias (`dune_prod`).
- Setup commands exist for at least Codex and Claude Code.
- No secrets are committed in repository files.
- At least one successful MCP tool invocation is verified locally per developer machine.

## Risks and Mitigations

- Client capability differences:
  - Mitigation: support header auth first, URL auth fallback where required.
- Key leakage in shell history:
  - Mitigation: use env var expansion, avoid literal key values in commands.
- Dual env vars causing confusion:
  - Mitigation: define `DUNE_API_KEY` as canonical and schedule deprecation.
