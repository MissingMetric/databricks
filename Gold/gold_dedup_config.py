# Databricks notebook source
"""Client-owned dedup policy validation. Names refer to pre-prefix columns."""
DEDUP_DEFINITIONS = {
    "none": "Retains all source rows without deduplication.",
    "exact_record": "Collapses byte-equivalent normalized source records, preserving their occurrence count in the audit.",
    "same_source_identity": "Selects the latest configured revision within a source identity. Conflicting top-ranked versions stop the build.",
    "unique_business_key": "Proposes identity links using complete, normalized business keys within the configured scope.",
    "explicit_identity_mapping": "Links source identities using an explicit client-approved alias mapping. Both identities must be present.",
    "composite_fingerprint": "Flags or links records whose complete configured fingerprint fields agree. Equality does not prove business identity.",
    "external_reference": "Links a complete source reference to a unique target record in explicitly configured accounts. Automatic links require one-to-one cardinality.",
}


def validate_dedup(spec, columns):
    if not isinstance(spec, dict):
        raise ValueError("Every table requires an explicit dedup policy")
    allowed = {"identity", "scope", "strategies", "selection", "conflict_fields", "on_conflict", "allow_entity_merge"}
    if set(spec) - allowed:
        raise ValueError(f"Unknown dedup settings: {sorted(set(spec) - allowed)}")
    cols = set(columns)

    def fields(value, label, empty=False):
        if not isinstance(value, list) or (not value and not empty) or any(not isinstance(k, str) or k not in cols for k in value) or len(set(value)) != len(value):
            raise ValueError(f"{label} must contain unique existing column names")

    fields(spec.get("identity"), "dedup.identity")
    fields(spec.get("scope"), "dedup.scope")
    if not set(spec["scope"]) < set(spec["identity"]):
        raise ValueError("dedup.identity must include scope plus a source entity ID")
    fields(spec.get("conflict_fields", []), "conflict_fields", empty=True)
    if spec.get("on_conflict", "fail") not in ("fail", "retain"):
        raise ValueError("on_conflict must be fail or retain (retain keeps all candidate entities)")
    if not isinstance(spec.get("allow_entity_merge", False), bool):
        raise ValueError("allow_entity_merge must be boolean")
    selection = spec.get("selection", {})
    if not isinstance(selection, dict) or set(selection) - {"order_by", "prefer_values"}:
        raise ValueError("selection supports order_by and prefer_values")
    preferences = selection.get("prefer_values", [])
    if not isinstance(preferences, list):
        raise ValueError("prefer_values must be a list")
    for preference in preferences:
        if (not isinstance(preference, dict) or set(preference) != {"field", "values"}
                or preference["field"] not in cols or not isinstance(preference["values"], list)
                or not preference["values"] or any(not isinstance(v, str) for v in preference["values"])
                or len(set(preference["values"])) != len(preference["values"])):
            raise ValueError("prefer_values requires a field and ordered unique string values")

    def ordering(items):
        if not isinstance(items, list):
            raise ValueError("order_by must be a list")
        for item in items:
            if not isinstance(item, dict) or set(item) != {"field", "direction"} or item["field"] not in cols or item["direction"] not in ("asc", "desc"):
                raise ValueError("Each order_by requires an existing field and asc/desc direction")
    ordering(selection.get("order_by", []))
    steps = spec.get("strategies")
    if not isinstance(steps, list) or not steps:
        raise ValueError("dedup.strategies must be a nonempty list")
    ids, seen = set(), []
    for step in steps:
        if not isinstance(step, dict) or set(step) - {"id", "strategy", "params", "action"}:
            raise ValueError("Dedup steps accept id, strategy, params and action")
        name = step.get("strategy")
        if name not in DEDUP_DEFINITIONS:
            raise ValueError(f"Unknown dedup strategy: {name}")
        sid = step.get("id", name)
        if not isinstance(sid, str) or not sid or sid in ids:
            raise ValueError("Dedup step IDs must be unique")
        ids.add(sid)
        params = step.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("Dedup params must be an object")
        action = step.get("action", "flag" if name == "composite_fingerprint" else "merge")
        if action not in ("merge", "flag"):
            raise ValueError("Dedup action must be merge or flag")
        if name in ("none", "exact_record", "same_source_identity"):
            if action != "merge":
                raise ValueError("Source-record strategies do not support flag action")
            if name in seen:
                raise ValueError("Source-record strategies cannot repeat")
            if name == "same_source_identity":
                if set(params) != {"order_by"} or not params["order_by"]:
                    raise ValueError("same_source_identity requires explicit revision order_by")
                ordering(params["order_by"])
            elif params:
                raise ValueError(f"{name} accepts no parameters")
        else:
            if action == "merge" and not spec.get("allow_entity_merge", False):
                raise ValueError("Cross-identity merging requires allow_entity_merge=true; keep this false for order-line tables")
            if "same_source_identity" not in seen:
                raise ValueError("Matching strategies require same_source_identity first")
            if name in ("unique_business_key", "composite_fingerprint"):
                if set(params) != {"keys"} or not isinstance(params["keys"], list) or not params["keys"]:
                    raise ValueError("Key matching requires a nonempty keys list")
                for key in params["keys"]:
                    if not isinstance(key, dict) or set(key) - {"field", "normalize"} or key.get("field") not in cols or key.get("normalize", "exact") not in ("exact", "lower_trim", "company_name"):
                        raise ValueError("Invalid matching key or normalization")
            elif name == "external_reference":
                if set(params) != {"reference_field", "target_field", "from_scope", "to_scope"}:
                    raise ValueError("external_reference requires reference_field, target_field, from_scope and to_scope")
                if params["reference_field"] not in cols or params["target_field"] not in cols:
                    raise ValueError("External reference fields must exist")
                for scope in (params["from_scope"], params["to_scope"]):
                    if not isinstance(scope, dict) or set(scope) != set(spec["scope"]) or any(not isinstance(v, str) or not v for v in scope.values()):
                        raise ValueError("External references must explicitly bind every account scope field")
            else:
                if set(params) != {"aliases"} or not isinstance(params["aliases"], list) or not params["aliases"]:
                    raise ValueError("explicit_identity_mapping requires aliases")
                for alias in params["aliases"]:
                    if not isinstance(alias, dict) or set(alias) != {"from", "to"}:
                        raise ValueError("An alias requires from and to identity objects")
                    for identity in alias.values():
                        if not isinstance(identity, dict) or set(identity) != set(spec["identity"]) or any(not isinstance(v, str) or not v.strip() for v in identity.values()):
                            raise ValueError("Alias identities must specify all identity fields as nonempty strings")
                    if any(alias["from"][k] != alias["to"][k] for k in spec["scope"]):
                        raise ValueError("Explicit aliases cannot cross configured account scope")
        seen.append(name)
    if "none" in seen and len(seen) != 1:
        raise ValueError("none must be the only dedup strategy")
    if "none" not in seen and seen[0] != "exact_record":
        raise ValueError("Dedup pipelines start with exact_record")
    if "same_source_identity" in seen and seen.index("same_source_identity") != 1:
        raise ValueError("same_source_identity must immediately follow exact_record")
    return spec
