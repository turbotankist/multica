-- Revert: remove 'forge' from the runtime_profile.protocol_family CHECK constraint.
--
-- Any existing rows with protocol_family = 'forge' must be removed before
-- running this rollback or the ADD CONSTRAINT step will fail.

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
        'antigravity'
    ));
