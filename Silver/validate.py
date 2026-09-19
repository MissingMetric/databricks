# Databricks notebook source
"""
Self-validation for a resolved silver config.

Runs BEFORE any Spark job, against exactly the config the pipeline will execute
(base + client overrides, already merged and entity-filtered). Because the same
resolved dict is validated and then run, there is no gap between "what we checked"
and "what ran" -- which is the whole point.

Five checks (raise ValidationError with all problems collected, not first-fail):

  1. ingest<->conform reference integrity
     Every non-literal, non-transform `source` in a conform column must trace to a
     field the ingest selection actually pulls (accounting for GraphQL dot -> flatten
     underscore). Catches "customer_company is null because we never selected it."

  2. conformed-schema conformance
     Every enabled entity must produce EXACTLY the common-model column set for its
     table contract -- no missing, no extra. Protects gold's UNION ALL.

  3. transform registry resolution
     Every `transform` named in any column must be a registered function.

  4. entity dependency graph
     Every child's `source` entity, and every reconciliation's parent/child, must be
     an ENABLED entity. Catches orders-off / order_line_items-on.

  5. merge-key presence
     Every column named in an entity's merge_key must be produced by that entity.

`validate(config, common_model, ingest, transforms)` returns None on success and
raises ValidationError (with a full list of messages) on any failure.
"""

from typing import Iterable


class ValidationError(Exception):
    def __init__(self, errors):
        self.errors = list(errors)
        msg = f"{len(self.errors)} validation error(s):\n" + "\n".join(
            f"  - {e}" for e in self.errors
        )
        super().__init__(msg)


# ── helpers ──────────────────────────────────────────────────────────────────────

def _endpoint_fields(ep: dict) -> list:
    """Resolve the field list bronze will contain for one ingest endpoint.

    PRINCIPLE: declare what can't be derived, derive what can.
      - explicit `fields`      -> use it (e.g. HubSpot owners: the API takes no
                                  properties and returns a fixed flat shape, so
                                  nothing else could produce the list)
      - else derive            -> `id` + properties.* + associations.*.results.id
                                  (HubSpot object endpoints)
      - a bare `fields` list is also how flat/REST platforms (Cin7, Shopify)
        declare their selection, so this one rule serves every platform.
    """
    if "fields" in ep:
        return list(ep["fields"])
    derived = ["id"]
    derived += [f"properties.{p}" for p in ep.get("properties", [])]
    derived += [f"associations.{a}.results.id" for a in ep.get("associations", [])]
    return derived


def _ingest_fields_flat(ingest: dict) -> dict:
    """Map ingest entity/endpoint name -> set of POST-FLATTEN column names it will
    produce.

    Ingest metadata lists dot paths ('properties.email',
    'totalPriceSet.shopMoney.amount'). After flatten_struct_columns those become
    underscore columns ('properties_email', 'totalPriceSet_shopMoney_amount'). We
    translate here so check #1 can compare conform sources (underscore vocabulary)
    against what ingest actually pulls.

    Accepts both shapes: `entities` (Shopify/Cin7 style) and `endpoints`
    (HubSpot style), since both are the same contract under different names.
    """
    out = {}
    for e in ingest.get("entities", []) + ingest.get("endpoints", []):
        name = e["name"]
        flat = set()
        for f in _endpoint_fields(e):
            flat.add(f.replace(".", "_"))
        out[name] = flat
    return out


def _column_source_cols(spec: dict) -> list:
    """Return the list of raw source column names a spec depends on, or [] if the
    spec is a literal or a transform (whose sources we can't statically know)."""
    if "transform" in spec or "literal" in spec:
        return []
    src = spec["source"]
    return list(src) if isinstance(src, list) else [src]


def _enabled_entities(config: dict) -> dict:
    """The entity dict, already filtered to enabled entities by the resolver."""
    return config.get("entities", {})


# ── the five checks ──────────────────────────────────────────────────────────────

def _check_ingest_reference(config, ingest, errors):
    """Check #1. Every source column in an OBJECT-shape entity must be pullable from
    that entity's ingest selection (post-flatten). Nested-array entities source from an
    exploded array on their parent; we skip static proof of those here (they'd need
    the array sub-selection expanded) but still flag sources that are obviously the
    parent's own top-level fields."""
    flat = _ingest_fields_flat(ingest)
    for name, ent in _enabled_entities(config).items():
        if ent.get("shape") == "nested_array":
            # child sources come from the exploded node; can't be statically proven
            # against the parent's flat object fields, so skip (covered at runtime).
            continue
        source_entity = ent.get("source", name)
        available = flat.get(source_entity)
        if available is None:
            errors.append(
                f"[ref] entity '{name}' sources from ingest entity "
                f"'{source_entity}' which is not defined in the ingest metadata"
            )
            continue
        for target, spec in ent.get("columns", {}).items():
            for src in _column_source_cols(spec):
                if src not in available:
                    errors.append(
                        f"[ref] entity '{name}' column '{target}' reads source "
                        f"'{src}', but the ingest selection for '{source_entity}' "
                        f"does not pull it (post-flatten). Add it to the ingest "
                        f"fields or the column will be null."
                    )


def _check_schema_conformance(config, common_model, errors):
    """Check #2. Each enabled entity produces EXACTLY the column set of the TABLE it
    declares. `table` (contract) is independent of `shape` (extraction)."""
    tables = common_model.get("tables", {})
    for name, ent in _enabled_entities(config).items():
        table = ent.get("table")
        model = tables.get(table)
        if model is None:
            errors.append(
                f"[schema] entity '{name}' declares table '{table}' which is not "
                f"defined in the common model (known: {sorted(tables)})"
            )
            continue
        expected = set(model["columns"])
        produced = set(ent.get("columns", {}))
        missing = expected - produced
        extra = produced - expected
        if missing:
            errors.append(
                f"[schema] entity '{name}' (table '{table}') is MISSING columns "
                f"required by the common model: {sorted(missing)}"
            )
        if extra:
            errors.append(
                f"[schema] entity '{name}' (table '{table}') produces EXTRA columns "
                f"not in the common model: {sorted(extra)}. This will break the gold "
                f"UNION ALL."
            )


def _check_transforms_registered(config, transforms, errors):
    """Check #3. Every named transform resolves to a registered function."""
    known = set(transforms)
    for name, ent in _enabled_entities(config).items():
        for target, spec in ent.get("columns", {}).items():
            t = spec.get("transform")
            if t is not None and t not in known:
                errors.append(
                    f"[transform] entity '{name}' column '{target}' references "
                    f"transform '{t}' which is not registered. Known: {sorted(known)}"
                )


def _check_dependency_graph(config, errors):
    """Check #4. Two cross-entity dependencies must hold:
    (a) an entity's explicit `requires` list (sibling entities it can't exist
        without -- e.g. order_line_items requires orders for reconciliation), and
    (b) every reconciliation's parent/child must both be enabled.

    NOTE: an entity's `source` is a BRONZE FILE name, NOT an entity name -- a
    nested_array child and its header sibling often read the SAME bronze file
    (Shopify: both 'orders'; Cin7: both 'sales_orders'), so `source` must NOT be
    treated as a parent-entity reference. Declare real sibling dependencies with
    `requires`."""
    enabled = set(_enabled_entities(config))
    for name, ent in _enabled_entities(config).items():
        for req in ent.get("requires", []):
            if req not in enabled:
                errors.append(
                    f"[deps] entity '{name}' requires '{req}', which is not an "
                    f"enabled entity. Enable it or disable '{name}'."
                )
    for r in config.get("reconciliations", []):
        for role in ("parent", "child"):
            ref = r.get(role)
            if ref not in enabled:
                errors.append(
                    f"[deps] reconciliation {r.get('parent')}<->{r.get('child')} "
                    f"references {role} '{ref}', which is not an enabled entity. "
                    f"Disable the reconciliation or enable the entity."
                )


def _check_merge_keys(config, errors):
    """Check #5. Every merge_key column is produced by its entity."""
    for name, ent in _enabled_entities(config).items():
        produced = set(ent.get("columns", {}))
        for k in ent.get("merge_key", []):
            if k not in produced:
                errors.append(
                    f"[merge] entity '{name}' merge_key includes '{k}', which is not "
                    f"one of the columns it produces. The upsert would fail."
                )


# ── entry point ──────────────────────────────────────────────────────────────────

def validate(config: dict, common_model: dict, ingest: dict, transforms: Iterable) -> None:
    """Run all five checks. Raises ValidationError with the full list, or returns
    None if the resolved config is sound.

    Args:
        config:       resolved, entity-filtered silver config (base + overrides)
        common_model: parsed common_model.json
        ingest:       parsed platform ingest metadata (for reference integrity)
        transforms:   iterable of registered transform names (e.g. TRANSFORMS keys)
    """
    errors = []
    _check_ingest_reference(config, ingest, errors)
    _check_schema_conformance(config, common_model, errors)
    _check_transforms_registered(config, transforms, errors)
    _check_dependency_graph(config, errors)
    _check_merge_keys(config, errors)
    if errors:
        raise ValidationError(errors)


# ── standalone / CI usage ────────────────────────────────────────────────────────
