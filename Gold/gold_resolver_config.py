# Databricks notebook source
"""Pure configuration contract shared by validation, execution and cataloging."""
from copy import deepcopy

# COMMAND ----------
# MAGIC %run ./gold_dedup_config
# COMMAND ----------


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
    if not isinstance(config, dict) or set(config) != {"schema_version", "tables"} or config["schema_version"] != 2:
        raise ValueError("resolution_config requires schema_version=2 and tables")
    client_tables = config["tables"]
    if not isinstance(client_tables, dict) or set(client_tables) != set(tables):
        raise ValueError("Client resolver configuration must explicitly cover every gold table (use resolve: {} when empty)")
    result = deepcopy(tables)
    for name, entry in client_tables.items():
        if not isinstance(entry, dict) or not {"resolve", "dedup"} <= set(entry) or set(entry) - {"resolve", "dedup", "identity_links"} or not isinstance(entry["resolve"], dict):
            raise ValueError(f"{name}: expected resolve and dedup objects, optional identity_links")
        for spec in entry["resolve"].values():
            resolver_variants(spec)
        validate_dedup(entry["dedup"], tables[name].get("base_columns", []) + ([tables[name]["grain_pk"]] if tables[name].get("grain_pk") else []))
        result[name]["resolve"] = deepcopy(entry["resolve"])
        result[name]["dedup"] = deepcopy(entry["dedup"])
        result[name]["identity_links"] = deepcopy(entry.get("identity_links", []))
    return result
