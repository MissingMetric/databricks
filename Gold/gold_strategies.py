# Databricks notebook source
# Gold strategies -- registered into the four-stage engine
# ─────────────────────────────────────────────────────────────────────────────
# Ports the existing resolver logic into the stage registries, and adds the
# metric strategies that reproduce the current fact window block. Every function
# is registered via a decorator from gold_engine; the engine dispatches by name
# from the table metadata.

# COMMAND ----------

# MAGIC %run ./gold_engine

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lit, when, lower, trim, regexp_replace, coalesce,
    sum as spark_sum, min as spark_min, max as spark_max, count as spark_count,
    countDistinct, first, date_trunc, datediff, current_date, size, collect_set,
    date_format, row_number, struct, to_json, dense_rank,
)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS (unchanged from the current resolvers)
# ══════════════════════════════════════════════════════════════════════════════

def company_match_key(c):
    k = lower(trim(c))
    k = regexp_replace(k, r"[.,]", "")
    k = regexp_replace(k, r"\b(llc|inc|incorporated|corp|co|ltd)\b", "")
    k = regexp_replace(k, r"\s+", " ")
    return trim(k)


# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1 -- DEDUP strategies
# ══════════════════════════════════════════════════════════════════════════════

def _dd_fields(keys, columns):
    if not isinstance(keys, list) or not keys or any(not isinstance(k, str) or k not in columns for k in keys):
        raise ValueError("Expected nonempty list of existing fields")


def _dd_annotate(df, group, member, role, inputs):
    return (df.withColumn("__dd_group", group).withColumn("__dd_member", member)
        .withColumn("__dd_role", role).withColumn("__dd_inputs", inputs))


@dedup_strategy("same_source_identity",
    description="Groups versions of the same configured scoped source identity.", outcomes={})
def match_same_source_identity(df, params, ctx):
    return _dd_annotate(df, col("__dd_identity"), F.sha2(col("__dd_record"), 256), lit("version"),
                        col("__dd_identity"))


def _validate_same(params, columns, identity):
    if params:
        raise ValueError("same_source_identity accepts no parameters")


match_same_source_identity.validate_params = _validate_same
match_same_source_identity.selectors = {"ordered"}


@dedup_strategy("normalized_key",
    description="Matches complete normalized keys within a configured scope; protected-field disagreements stop the build. This is a client-approved equivalence rule, not fuzzy matching.", outcomes={})
def match_normalized_key(df, params, ctx):
    dedup_require(col("count") > 1, df.groupBy("__dd_identity").count(),
                  "Resolve source versions before matching business keys")
    expressions = [col(k).alias(k) for k in params["scope_by"]]
    valid = lit(True)
    for key in params["scope_by"]:
        valid = valid & col(key).isNotNull() & (F.trim(col(key).cast("string")) != "")
    for index, key in enumerate(params["keys"]):
        value = col(key["field"])
        norm = key.get("normalize", "exact")
        if norm == "lower_trim":
            value = F.lower(F.trim(value))
        elif norm == "company_name":
            value = company_match_key(value)
        valid = valid & value.isNotNull() & (F.trim(value.cast("string")) != "")
        expressions.append(value.alias(f"key_{index}"))
    group = F.when(valid, F.to_json(F.struct(*expressions), {"ignoreNullFields": "false"}))
    result = _dd_annotate(df, group, col("__dd_identity"), lit("candidate"),
                         dedup_json(list(dict.fromkeys(params["scope_by"] + [k["field"] for k in params["keys"]] + params.get("must_agree", [])))))
    for field in params.get("must_agree", []):
        conflicts = result.filter(col("__dd_group").isNotNull()).groupBy("__dd_group").agg(
            F.countDistinct(dedup_json([field])).alias("n"))
        dedup_require(col("n") > 1, conflicts, f"Conflicting protected field: {field}")
    return result


def _validate_normalized(params, columns, identity):
    if set(params) - {"scope_by", "keys", "require_nonempty", "must_agree"} or params.get("require_nonempty", True) is not True:
        raise ValueError("normalized_key requires nonempty matching keys")
    _dd_fields(params.get("scope_by"), columns)
    if not isinstance(params.get("keys"), list) or not params["keys"]:
        raise ValueError("normalized_key requires keys")
    for key in params["keys"]:
        if not isinstance(key, dict) or set(key) - {"field", "normalize"} or key.get("field") not in columns or key.get("normalize", "exact") not in {"exact", "lower_trim", "company_name"}:
            raise ValueError("Invalid normalized matching key")
    if not isinstance(params.get("must_agree", []), list):
        raise ValueError("must_agree must be a list of fields")
    if params.get("must_agree"):
        _dd_fields(params["must_agree"], columns)


match_normalized_key.validate_params = _validate_normalized
match_normalized_key.selectors = {"ordered"}
match_normalized_key.identity_mapping = lambda params, identity: identity


@dedup_strategy("external_reference",
    description="Matches one referencing unit to one existing referenced unit in explicit source scopes. All lines of each unit remain together; incomplete or conflicting links stop the build.", outcomes={})
def match_external_reference(df, params, ctx):
    unit = params["unit_by"]
    source, target = params["from"], params["to"]
    def scope(where):
        condition = lit(True)
        for key, value in where.items():
            condition = condition & col(key).eqNullSafe(lit(value))
        return condition
    left_scope, right_scope = scope(source["where"]), scope(target["where"])
    dedup_require(left_scope & right_scope, df, "External-reference scopes overlap")
    for key in unit:
        dedup_require((left_scope | right_scope) & (col(key).isNull() | (F.trim(col(key).cast("string")) == "")), df, "Missing external unit identity")
    ref, target_key = source["reference_field"], target["identity_field"]
    base = df.withColumn("__dd_unit", dedup_json(unit))
    # Every line of a referencing unit must agree, including null versus value.
    consistent = base.filter(left_scope).groupBy("__dd_unit").agg(F.countDistinct(dedup_json([ref])).alias("n"))
    dedup_require(col("n") > 1, consistent, "Conflicting references within unit")
    sources = base.filter(left_scope & col(ref).isNotNull() & (F.trim(col(ref).cast("string")) != "")).select(
        col("__dd_unit").alias("__dd_from"), col(ref).cast("string").alias("__dd_ref")).distinct()
    targets = base.filter(right_scope & col(target_key).isNotNull()).select(
        col("__dd_unit").alias("__dd_to"), col(target_key).cast("string").alias("__dd_target")).distinct()
    links = sources.join(targets, col("__dd_ref") == col("__dd_target"), "inner")
    for field in ("__dd_from", "__dd_to"):
        dedup_require(col("count") > 1, links.groupBy(field).count(), "External reference is not one-to-one")
    members = links.select(col("__dd_from").alias("__dd_lookup"), col("__dd_to").alias("__dd_link"), lit("referencing").alias("__dd_kind"), col("__dd_ref").alias("__dd_link_input")).unionByName(
        links.select(col("__dd_to").alias("__dd_lookup"), col("__dd_to").alias("__dd_link"), lit("referenced").alias("__dd_kind"), col("__dd_ref").alias("__dd_link_input")))
    joined = base.join(members, col("__dd_unit") == col("__dd_lookup"), "left")
    return _dd_annotate(joined, col("__dd_link"), col("__dd_unit"), F.coalesce(col("__dd_kind"), lit("unmatched")),
        F.to_json(F.struct(col("__dd_link_input").alias("reference"), col("__dd_unit").alias("unit")), {"ignoreNullFields": "false"})).drop(
            "__dd_unit", "__dd_lookup", "__dd_link", "__dd_kind", "__dd_link_input")


def _validate_external(params, columns, identity):
    if set(params) != {"unit_by", "from", "to", "cardinality"} or params["cardinality"] != "one_to_one":
        raise ValueError("external_reference requires explicit units/scopes and one_to_one cardinality")
    _dd_fields(params["unit_by"], columns)
    if not set(params["unit_by"]) <= set(identity):
        raise ValueError("Unit fields must be included in the row identity")
    for side, key in (("from", "reference_field"), ("to", "identity_field")):
        part = params[side]
        if not isinstance(part, dict) or set(part) != {"where", key} or part[key] not in columns or not isinstance(part["where"], dict) or not part["where"]:
            raise ValueError("Invalid external-reference endpoint")
        _dd_fields(list(part["where"]), columns)
        if not set(part["where"]) <= set(params["unit_by"]):
            raise ValueError("Endpoint scope fields must be part of unit identity")
        if any(v is None or isinstance(v, (dict, list)) for v in part["where"].values()):
            raise ValueError("Source scopes require non-null scalar values")
    if set(params["from"]["where"]) != set(params["to"]["where"]) or params["from"]["where"] == params["to"]["where"]:
        raise ValueError("Endpoint scopes must bind the same fields to different values")


match_external_reference.validate_params = _validate_external
match_external_reference.selectors = {"referenced_record"}
match_external_reference.identity_mapping = lambda params, identity: params["unit_by"]


@selection_strategy("ordered",
    description="Chooses by declared ordering and source preferences. Differing candidates tied at the top stop the build.", outcomes={})
def select_ordered(df, params, ctx):
    order = []
    fields = []
    for entry in params["order_by"]:
        field = entry["field"]
        fields.append(field)
        value = col(field)
        if "prefer" in entry:
            value = lit(len(entry["prefer"]))
            for index, preferred in reversed(list(enumerate(entry["prefer"]))):
                value = F.when(col(field).eqNullSafe(lit(preferred)), index).otherwise(value)
            order.append(value.asc())
        else:
            order.append(getattr(value, entry.get("direction", "asc") + "_nulls_" + entry.get("nulls", "last"))())
    # Unmatched members get individual partitions, never compete with one another.
    partition = F.coalesce(col("__dd_group"), col("__dd_member"))
    window = Window.partitionBy(partition).orderBy(*order)
    ranked = df.withColumn("__dd_rank", F.dense_rank().over(window))
    ties = ranked.filter(col("__dd_rank") == 1).groupBy(partition.alias("partition")).agg(F.countDistinct("__dd_member").alias("n"))
    dedup_require(col("n") > 1, ties, "Selection tied: configure an explicit tie-breaker")
    full = window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return (ranked.withColumn("__dd_winner", F.first("__dd_member").over(full))
        .withColumn("__dd_selection", dedup_json(list(dict.fromkeys(fields)))).drop("__dd_rank"))


def _validate_ordered(params, columns, identity):
    if set(params) - {"order_by", "on_tie"} or params.get("on_tie", "fail") != "fail" or not isinstance(params.get("order_by"), list) or not params["order_by"]:
        raise ValueError("ordered requires ordering; only on_tie=fail is supported")
    for entry in params["order_by"]:
        if not isinstance(entry, dict) or entry.get("field") not in columns:
            raise ValueError("Invalid ordering field")
        if "prefer" in entry:
            if set(entry) - {"field", "prefer", "unlisted"} or not isinstance(entry["prefer"], list) or not entry["prefer"] or entry.get("unlisted", "last") != "last":
                raise ValueError("Invalid preference ordering")
            if any(not isinstance(v, str) for v in entry["prefer"]) or len(set(entry["prefer"])) != len(entry["prefer"]):
                raise ValueError("Preferences must be distinct strings")
        elif set(entry) - {"field", "direction", "nulls"} or entry.get("direction", "asc") not in {"asc", "desc"} or entry.get("nulls", "last") not in {"first", "last"}:
            raise ValueError("Invalid ordering direction/null policy")


select_ordered.validate_params = _validate_ordered


@selection_strategy("referenced_record",
    description="Keeps the existing referenced unit, including its entire line set, rather than its integration copy. Does not infer completeness.", outcomes={})
def select_referenced_record(df, params, ctx):
    winner = F.max(F.when(col("__dd_role") == "referenced", col("__dd_member"))).over(Window.partitionBy("__dd_group"))
    return (df.withColumn("__dd_winner", F.when(col("__dd_group").isNull(), col("__dd_member")).otherwise(winner))
        .withColumn("__dd_selection", F.to_json(F.struct(col("__dd_role").alias("role")))))


select_referenced_record.validate_params = _validate_same

@dedup_strategy("none",
    description="Keeps all incoming rows; no deduplication is applied.",
    outcomes={"retained":"This row was retained without deduplication."})
def dedup_none(df, spec, ctx):
    """No dedup -- table is already at its own grain."""
    return df


@dedup_strategy("keep_first",
    description="Keeps one row per identity, ordered descending by the configured ordering field. Ties have no explicit preference.",
    outcomes={"retained":"This is the retained row. Discarded candidates are not stored in this evidence."})
def dedup_keep_first(df, spec, ctx):
    """One row per `on` key, deterministic pick by optional `order_by` (desc)."""
    on = spec["on"]
    order_col = spec.get("order_by", on)
    w = Window.partitionBy(on).orderBy(col(order_col).desc())
    return (df.withColumn("_rn", row_number().over(w))
              .filter(col("_rn") == 1).drop("_rn"))

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2 -- RESOLVE strategies (ported from the current resolvers)
# ══════════════════════════════════════════════════════════════════════════════

def _candidate(df, value, reason, inputs, ambiguous=None):
    usable = value.isNotNull() & (trim(value.cast("string")) != "")
    status = when(usable, lit("resolved")).otherwise(lit("no_match"))
    if ambiguous is not None:
        status = when(ambiguous, lit("ambiguous")).otherwise(status)
    return df.withColumn("__mm_candidate", struct(
        when(usable, value.cast("string")).alias("value"),
        status.alias("status"),
        when(status == "ambiguous", lit("ambiguous_lookup"))
            .when(usable, lit(reason)).otherwise(lit("missing_input_or_match")).alias("reason"),
        to_json(struct(*[v.alias(k) for k, v in inputs.items()]),
                {"ignoreNullFields": "false"}).alias("inputs")))


def _unique_lookup(df, key, fields):
    """One lookup row per key. Conflicting payloads are flagged, never picked."""
    values = struct(*[col(field).alias(field) for field in fields])
    return (df.filter(col(key).isNotNull()).groupBy(key)
        .agg(collect_set(values).alias("__mm_matches"))
        .withColumn("__mm_ambiguous", size(col("__mm_matches")) > 1)
        .select(col(key).alias("__mm_lookup_key"), "__mm_ambiguous",
            *[when(~col("__mm_ambiguous"), col("__mm_matches")[0][field])
                .alias("__mm_lookup_" + field) for field in fields]))


@resolve_strategy("company_order_string",
    row_inputs={"field": "orders_e_customer_company"},
    description="Uses normalized company text from the configured row field as the company identity. No directory lookup is attempted.",
    outcomes={"order_string": "Normalized order company text supplied the identity.",
              "missing_input_or_match": "No usable company text was supplied."})
def r_company_order_string(df, params, ctx):
    value = company_match_key(col(params["field"]))
    return _candidate(df, value, "order_string",
        {"order_company": col(params["field"]), "normalized_name": value})


@resolve_strategy("company_normalized_name_match",
    row_inputs={"field": "orders_e_customer_company"},
    description="Matches normalized row company text to the companies directory. Normalization removes case, punctuation, selected suffixes and repeated spaces. Conflicting matches stop the build.",
    outcomes={"company_name_match": "A unique normalized-name match supplied the company identity.",
              "missing_input_or_match": "No usable company name or directory match was found."})
def r_company_normalized_name_match(df, params, ctx):
    lookup = _unique_lookup(ctx["dims"]["companies"].withColumn(
        "__mm_name", company_match_key(col("company_e_name"))),
        "__mm_name", ["company_e_id", "company_e_name", "source_platform"])
    key = company_match_key(col(params["field"]))
    joined = df.join(lookup, (key != "") & (key == col("__mm_lookup_key")), "left")
    result = _candidate(joined, col("__mm_lookup_company_e_id"), "company_name_match", {
        "order_company": col(params["field"]), "normalized_name": key,
        "matched_company_id": col("__mm_lookup_company_e_id"),
        "matched_company_name": col("__mm_lookup_company_e_name"),
        "matched_platform": col("__mm_lookup_source_platform")},
        col("__mm_ambiguous"))
    return result.drop(*lookup.columns)


@resolve_strategy("sales_rep_order_native", version=2,
    row_inputs={"field": "orders_e_sales_rep_email"},
    description="Uses the rep email recorded in the configured row field. Missing or blank values leave the row unresolved for the next configured step.",
    outcomes={"order_native": "The order's recorded sales rep supplied the email.",
              "missing_input_or_match": "The order had no usable sales rep email."})
def r_rep_order_native(df, params, ctx):
    return _candidate(df, col(params["field"]), "order_native",
        {"order_rep": col(params["field"])})


@resolve_strategy("sales_rep_company_owner",
    row_inputs={"company_field": "company_e_id"},
    description="Looks up the resolved company in the companies directory, then looks up its owner in the rep directory. It does not read the order's native rep.",
    outcomes={"company_owner": "The company's owner supplied the sales rep email.",
              "missing_input_or_match": "The company, owner ID or owner email could not be matched."})
def r_rep_company_owner(df, params, ctx):
    companies = _unique_lookup(ctx["dims"]["companies"], "company_e_id", ["company_e_owner_id"])
    joined = df.join(companies, col(params["company_field"]) == col("__mm_lookup_key"), "left")
    joined = (joined.withColumnRenamed("__mm_lookup_key", "__mm_company_key")
        .withColumnRenamed("__mm_ambiguous", "__mm_company_ambiguous"))
    reps = _unique_lookup(ctx["dims"]["sales_reps"], "sales_rep_e_id", ["sales_rep_e_email"])
    joined = joined.join(reps, col("__mm_lookup_company_e_owner_id") == col("__mm_lookup_key"), "left")
    inputs = {"company_id": col(params["company_field"]), "matched_company_id": col("__mm_company_key"),
        "owner_id": col("__mm_lookup_company_e_owner_id"), "matched_rep_id": col("__mm_lookup_key"),
        "owner_email": col("__mm_lookup_sales_rep_e_email")}
    upstream = params["company_field"] + "_evidence"
    if upstream in df.columns:
        inputs["company_decision"] = col(upstream)
    result = _candidate(joined, col("__mm_lookup_sales_rep_e_email"), "company_owner", inputs,
        coalesce(col("__mm_company_ambiguous"), lit(False)) | coalesce(col("__mm_ambiguous"), lit(False)))
    return result.drop("__mm_company_key", "__mm_company_ambiguous",
        "__mm_lookup_company_e_owner_id", *reps.columns)


@resolve_strategy("owner_id_to_rep_email", version=2,
    row_inputs={"owner_field": "company_e_owner_id"},
    description="Looks up the configured row owner ID in the rep directory and returns its unique email.",
    outcomes={"owner_lookup": "An email was returned by the owner-ID lookup.",
              "missing_input_or_match": "No usable owner ID or matching rep email was found."})
def r_owner_id_to_rep_email(df, params, ctx):
    reps = _unique_lookup(ctx["dims"]["sales_reps"], "sales_rep_e_id", ["sales_rep_e_email"])
    joined = df.join(reps, col(params["owner_field"]) == col("__mm_lookup_key"), "left")
    result = _candidate(joined, col("__mm_lookup_sales_rep_e_email"), "owner_lookup",
        {"owner_id": col(params["owner_field"]), "matched_rep_id": col("__mm_lookup_key"),
         "matched_email": col("__mm_lookup_sales_rep_e_email")}, col("__mm_ambiguous"))
    return result.drop(*reps.columns)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 3 -- METRIC strategies
# Declarative aggregates + windowed metrics. Each returns ONE Column, partitioned
# over the metric's `over` (an identity column). Reproduces the current window block.
# ══════════════════════════════════════════════════════════════════════════════

def _win(m):
    return Window.partitionBy(m["over"])

def _win_ordered(m):
    return (Window.partitionBy(m["over"])
            .orderBy(col(m.get("order_by", "order_date")).asc())
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

@metric_strategy("sum",
    description="Adds the input values within the configured identity partition.",
    outcomes={})
def m_sum(df, m, ctx):
    return spark_sum(m["of"]).over(_win(m))

@metric_strategy("min",
    description="Finds the lowest input value within the configured identity partition.",
    outcomes={})
def m_min(df, m, ctx):
    return spark_min(m["of"]).over(_win(m))

@metric_strategy("max",
    description="Finds the highest input value within the configured identity partition.",
    outcomes={})
def m_max(df, m, ctx):
    return spark_max(m["of"]).over(_win(m))

@metric_strategy("count_distinct",
    description="Counts distinct non-null input values within the configured identity partition.",
    outcomes={})
def m_count_distinct(df, m, ctx):
    # size(collect_set(...)) over a window -- distinct count without collapsing rows
    return size(collect_set(m["of"]).over(_win(m)))

@metric_strategy("first_ordered",
    description="Takes the input value from the earliest ordered row in the identity partition. Ordering ties have no explicit preference.",
    outcomes={})
def m_first_ordered(df, m, ctx):
    return first(m["of"]).over(_win_ordered(m))

@metric_strategy("cohort_month",
    description="Formats the earliest input date in the identity partition as year-month.",
    outcomes={})
def m_cohort_month(df, m, ctx):
    # yyyy-MM of the earliest order_date in the partition
    return date_format(spark_min(m["of"]).over(_win(m)), "yyyy-MM")

@metric_strategy("days_since",
    description="Calculates days from the latest input date in the identity partition to the pipeline processing date, not the viewing date.",
    outcomes={})
def m_days_since(df, m, ctx):
    return datediff(current_date(), spark_max(m["of"]).over(_win(m)))

@metric_strategy("is_current_month_of",
    description="Checks whether the configured row date, truncated to month, equals the configured comparison value.",
    outcomes={})
def m_is_current_month(df, m, ctx):
    # true if this row's order_date month == the cohort month column named in `of`
    return date_trunc("month", col(m["order_date"])) == col(m["of"])

@metric_strategy(
    "sequence",
    description=(
        "Assigns a consecutive sequence within the configured identity "
        "partition, ordered by the configured field and direction. "
        "The entity key breaks ordering ties. Repeated rows with the same "
        "ordering value and entity key share a sequence number. "
        "Rows missing a required input receive null and do not affect "
        "the sequence of valid rows."
    ),
    outcomes={}
)
def m_sequence(df, m, ctx):
    """
    Required:
      over:     Partition identity, e.g. customer ID.
      of:       Entity identity, e.g. globally unique order ID.
      order_by: Ordering field, e.g. order timestamp.

    Optional:
      direction: "asc" (default) or "desc".

    All rows for an entity must share the same partition identity
    and ordering value.
    """
    partition_key = m["over"]
    entity_key = m["of"]
    ordering_key = m["order_by"]
    direction = m.get("direction", "asc").lower()

    if direction not in ("asc", "desc"):
        raise ValueError("sequence.direction must be 'asc' or 'desc'")

    for key in (partition_key, entity_key, ordering_key):
        if not isinstance(key, str) or key not in df.columns:
            raise ValueError(
                f"sequence requires an existing column name; received {key!r}"
            )

    valid = (
        col(partition_key).isNotNull()
        & col(entity_key).isNotNull()
        & col(ordering_key).isNotNull()
    )

    ordering = (
        col(ordering_key).asc()
        if direction == "asc"
        else col(ordering_key).desc()
    )

    # Separate invalid rows so they cannot shift valid sequence numbers.
    window = (
        Window.partitionBy(col(partition_key), valid)
        .orderBy(ordering, col(entity_key).asc())
    )

    return when(
        valid,
        dense_rank().over(window)
    ).otherwise(lit(None).cast("int"))

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4 -- ENRICH strategies
# ══════════════════════════════════════════════════════════════════════════════

@enrich_strategy("left_join_bring",
    description="Brings fields from a source table's public surface by key using a left join. Source rows are reduced to one per join key without an explicit tie preference.",
    outcomes={"matched":"A source row matched the join key. The brought value itself may be null.","unmatched":"No source row matched the join key."})
def e_left_join_bring(df, source_surface, spec, ctx):
    """Left-join a source table's public surface on `on`, bring renamed columns.
    `bring` is {source_col: target_col}. Collisions avoided by explicit renaming."""
    return _enrich_with_evidence(df, source_surface, spec, ctx, "left", e_left_join_bring.strategy_ref)
    
@enrich_strategy("full_join_bring",
    description="Brings fields by key using a full outer join, preserving unmatched rows from either side. Source rows are reduced to one per join key without an explicit tie preference.",
    outcomes={"matched":"A source row was present for this enrichment.","unmatched":"No source row was present for this enrichment."})
def e_full_join_bring(df, source_surface, spec, ctx):
    """Full-join a source table's public surface on `on`, 
    bring renamed columns."""
    return _enrich_with_evidence(df, source_surface, spec, ctx, "full_outer", e_full_join_bring.strategy_ref)


def _enrich_with_evidence(df, source_surface, spec, ctx, how, ref):
    on, bring = spec["on"], spec["bring"]
    src_on = spec.get("source_on", on)
    join_key = "__mm_enrich_key"
    selects = [col(src_on).alias(join_key)]
    scratch = [join_key]
    for index, (source, target) in enumerate(bring.items()):
        selects.append(col(source).alias(target))
        upstream = f"__mm_upstream_{index}"
        scratch.append(upstream)
        selects.append((col(source + "_evidence") if source + "_evidence" in source_surface.columns
                        else lit(None).cast("string")).alias(upstream))
    src = source_surface.select(*selects).dropDuplicates([join_key])
    result = df.join(src, df[on] == src[join_key], how)
    for index, (source, target) in enumerate(bring.items()):
        result = record_evidence(result, target, ref,
            when(col(join_key).isNotNull(), lit("matched")).otherwise(lit("unmatched")),
            {"join_value": col(on), "matched_key": col(join_key),
             "source_table": lit(spec["from"]), "source_field": lit(source),
             "upstream_decision": col(f"__mm_upstream_{index}")}, ctx)
    return result.drop(*scratch)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4 tail -- DERIVE strategies (post-enrich per-row expressions)
# May reference any column on the row (native/resolved/metric/enriched). Terminal.
# ══════════════════════════════════════════════════════════════════════════════

@derive_strategy("coalesce",
    description="Uses the first non-null value from the configured inputs, in their declared order.",
    outcomes={})
def d_coalesce(df, d, ctx):
    """First non-null across `of` (a list of column names). Display-name fallback:
    coalesce(hubspot_name, order_string_name)."""
    return coalesce(*[col(c) for c in d["of"]])


@derive_strategy("is_not_null",
    description="Returns true when the configured input is non-null; false otherwise.",
    outcomes={})
def d_is_not_null(df, d, ctx):
    """Boolean flag: true when `of` (a column) is non-null. The in_hubspot flag."""
    return col(d["of"]).isNotNull()

@derive_strategy(
    "new_or_returning",
    description=(
        "Classifies an order from its customer lifetime order sequence. "
        "Sequence 1 is New; sequences greater than 1 are Returning. "
        "Missing or invalid sequences are Unknown. "
        "New means the customer's first observed order in the available "
        "history, not necessarily their first-ever purchase."
    ),
    outcomes={}
)
def d_new_or_returning(df, d, ctx):
    """Classify an order using the sequence column named in `of`."""
    sequence = col(d["of"])

    valid = (
        sequence.isNotNull()
        & (sequence >= 1)
        & (sequence == sequence.cast("long"))
    )

    return (
        when(~valid | sequence.isNull(), lit("Unknown"))
        .when(sequence == 1, lit("New"))
        .otherwise(lit("Returning"))
    )

# COMMAND ----------

print("Gold strategies loaded and registered.")
print(f"  dedup:   {sorted(DEDUP)}")
print(f"  resolve: {sorted(RESOLVE)}")
print(f"  metric:  {sorted(METRIC)}")
print(f"  enrich:  {sorted(ENRICH)}")
print(f"  derive:  {sorted(DERIVE)}")
