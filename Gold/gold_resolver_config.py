# Databricks notebook source
"""Pure configuration contract shared by validation, execution and cataloging."""
from copy import deepcopy


def resolver_steps(spec):
    if not isinstance(spec, dict) or set(spec) - {"strategies", "by_platform"}:
        raise ValueError("Resolver requires strategies and optional by_platform; legacy strategy is not supported")
    chain = spec.get("strategies")
    if not isinstance(chain, list) or not chain:
        raise ValueError("strategies must be a nonempty ordered list")
    steps, ids = [], set()
    for entry in chain:
        step = {"strategy": entry} if isinstance(entry, str) else deepcopy(entry)
        if not isinstance(step, dict) or set(step) - {"id", "strategy", "params"}:
            raise ValueError("A strategy step accepts only id, strategy and params")
        name = step.get("strategy")
        if not isinstance(name, str) or not name:
            raise ValueError("A strategy step needs a name")
        step.setdefault("id", name)
        step.setdefault("params", {})
        if not isinstance(step["id"], str) or not step["id"] or step["id"] in ids:
            raise ValueError("Step IDs must be nonempty and unique within a chain")
        if not isinstance(step["params"], dict):
            raise ValueError("Step params must be an object")
        ids.add(step["id"])
        steps.append(step)
    return steps


def resolver_variants(spec):
    result = {None: resolver_steps(spec)}
    variants = spec.get("by_platform", {})
    if not isinstance(variants, dict):
        raise ValueError("by_platform must map platform names to complete chains")
    for platform, variant in variants.items():
        if not isinstance(platform, str) or not platform or not isinstance(variant, dict) or "by_platform" in variant:
            raise ValueError("Platform variants must be named, non-nested chains")
        result[platform] = resolver_steps(variant)
    return result


def resolver_order(resolvers, registry, available):
    """Strategy-declared row inputs determine dependencies, including variants."""
    dependencies = {}
    for target, spec in resolvers.items():
        required = set()
        for steps in resolver_variants(spec).values():
            for step in steps:
                if step["strategy"] not in registry:
                    raise ValueError(f"Unknown resolver strategy: {step['strategy']}")
                fn = registry[step["strategy"]]
                defaults = fn.row_inputs
                if set(step["params"]) - set(defaults):
                    raise ValueError(f"Unknown parameters for {step['strategy']}")
                inputs = {**defaults, **step["params"]}
                if any(not isinstance(v, str) or not v for v in inputs.values()):
                    raise ValueError("Resolver input parameters must be column names")
                required.update(inputs.values())
        missing = required - set(available) - set(resolvers)
        if missing:
            raise ValueError(f"{target}: missing resolver inputs {sorted(missing)}")
        dependencies[target] = required & set(resolvers)
    ordered = []
    while dependencies:
        ready = [target for target, deps in dependencies.items() if not deps]
        if not ready:
            raise ValueError(f"Cyclic resolver dependencies: {sorted(dependencies)}")
        ordered.extend(ready)
        for target in ready:
            del dependencies[target]
        for deps in dependencies.values():
            deps.difference_update(ready)
    return ordered


def client_resolver_tables(tables, config):
    """Exact replacement, never an overlay. Every table must be acknowledged."""
    if not isinstance(config, dict) or set(config) != {"schema_version", "tables"} or config["schema_version"] != 1:
        raise ValueError("resolution_config requires schema_version=1 and tables")
    client_tables = config["tables"]
    if not isinstance(client_tables, dict) or set(client_tables) != set(tables):
        raise ValueError("Client resolver configuration must explicitly cover every gold table (use resolve: {} when empty)")
    result = deepcopy(tables)
    for name, entry in client_tables.items():
        if not isinstance(entry, dict) or "resolve" not in entry or set(entry) - {"resolve", "dedup"} or not isinstance(entry["resolve"], dict):
            raise ValueError(f"{name}: expected a resolve object")
        for spec in entry["resolve"].values():
            resolver_variants(spec)
        result[name]["resolve"] = deepcopy(entry["resolve"])
        if "dedup" in entry:
            result[name]["dedup"] = deepcopy(entry["dedup"])
    return result


def validate_dedup_steps(spec, columns, matchers, selectors):
    """Structure here; strategy-owned parameter validation stays on functions."""
    if not isinstance(spec, dict) or not {"identity", "strategies"} <= set(spec) or set(spec) - {"identity", "strategies", "allow_identity_merge"}:
        raise ValueError("dedup requires identity and strategies")
    if not isinstance(spec.get("allow_identity_merge", False), bool):
        raise ValueError("allow_identity_merge must be a boolean")
    identity = spec["identity"]
    if not isinstance(identity, list) or not identity or any(not isinstance(k, str) or k not in columns for k in identity) or len(set(identity)) != len(identity):
        raise ValueError("dedup identity must be nonempty distinct existing columns")
    steps = spec["strategies"]
    if not isinstance(steps, list) or not steps:
        raise ValueError("dedup strategies must be a nonempty list")
    ids = set()
    mapping_grain = None
    for step in steps:
        if not isinstance(step, dict) or set(step) - {"id", "strategy", "params", "selection", "on_ambiguity"}:
            raise ValueError("Invalid dedup step")
        if not isinstance(step.get("id"), str) or not step["id"] or step["id"] in ids:
            raise ValueError("Dedup step IDs must be unique and nonempty")
        ids.add(step["id"])
        if step.get("on_ambiguity", "fail") != "fail":
            raise ValueError("Only fail-on-ambiguity is currently supported")
        selection = step.get("selection")
        if not isinstance(selection, dict) or set(selection) - {"strategy", "params"}:
            raise ValueError("A dedup step requires selection")
        for item, registry in ((step, matchers), (selection, selectors)):
            fn = registry.get(item.get("strategy"))
            if fn is None or not hasattr(fn, "validate_params"):
                raise ValueError(f"Unregistered dedup/selection implementation: {item.get('strategy')}")
            params = item.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("Strategy params must be an object")
            fn.validate_params(params, columns, identity)
        if selection["strategy"] not in matchers[step["strategy"]].selectors:
            raise ValueError("Matcher and selector are incompatible")
        if hasattr(matchers[step["strategy"]], "identity_mapping") and not spec.get("allow_identity_merge", False):
            raise ValueError("Cross-ID matching requires allow_identity_merge=true after auditing downstream references")
        if hasattr(matchers[step["strategy"]], "identity_mapping"):
            grain = tuple(matchers[step["strategy"]].identity_mapping(step.get("params", {}), identity))
            if mapping_grain is not None and mapping_grain != grain:
                raise ValueError("Cross-ID steps must use the same identity grain to preserve alias chains")
            mapping_grain = grain
