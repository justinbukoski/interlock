-- Compatibility predicates for the one-shot, non-destructive v5 continuity import.
INSERT INTO predicates(
    key, cardinality, value_type, minimum_authority_rank,
    owner_confirmation_required, mandatory_bootstrap
)
VALUES
    ('legacy.fact', 'set', 'object', 5, false, false),
    ('legacy.note', 'set', 'object', 7, false, false)
ON CONFLICT (key) DO UPDATE SET
    cardinality = EXCLUDED.cardinality,
    value_type = EXCLUDED.value_type,
    minimum_authority_rank = EXCLUDED.minimum_authority_rank,
    owner_confirmation_required = EXCLUDED.owner_confirmation_required,
    mandatory_bootstrap = EXCLUDED.mandatory_bootstrap;
