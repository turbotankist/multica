-- Add 'forge' to the runtime_profile.protocol_family CHECK constraint.
--
-- Migration 120 defined this constraint inline (no explicit name), so
-- PostgreSQL assigned the auto-generated name
-- `runtime_profile_protocol_family_check`. We drop it and re-add it with the
-- expanded value list.
--
-- Must stay in lockstep with SupportedTypes in server/pkg/agent/agent.go.

ALTER TABLE runtime_profile
    DROP CONSTRAINT IF EXISTS runtime_profile_protocol_family_check;

ALTER TABLE runtime_profile
    ADD CONSTRAINT runtime_profile_protocol_family_check
    CHECK (protocol_family IN (
        'claude',
        'codebuddy',
        'codex',
        'copilot',
        'opencode',
        'openclaw',
        'hermes',
        'gemini',
        'pi',
        'cursor',
        'kimi',
        'kiro',
        'antigravity',
        'forge'
    ));
