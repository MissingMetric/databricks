import ast
import copy
import importlib.util
import json
from pathlib import Path
import unittest
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("catalog", ROOT / "Gold/gold_catalog.py")
catalog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(catalog)
config_spec = importlib.util.spec_from_file_location("resolver_config", ROOT / "Gold/gold_resolver_config.py")
config = importlib.util.module_from_spec(config_spec)
config_spec.loader.exec_module(config)
catalog.resolver_variants = config.resolver_variants


def definitions():
    # Read the real decorator definitions without importing Spark or running a job.
    out = {}
    tree = ast.parse((ROOT / "Gold/gold_strategies.py").read_text(encoding="utf-8"))
    for fn in tree.body:
        if not isinstance(fn, ast.FunctionDef):
            continue
        for decorator in fn.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Name):
                continue
            stage = decorator.func.id.removesuffix("_strategy")
            name = ast.literal_eval(decorator.args[0])
            info = {kw.arg: ast.literal_eval(kw.value) for kw in decorator.keywords}
            row_inputs = info.pop("row_inputs", {})
            if row_inputs:
                info["inputs"] = {k: {"origin": "row", "default_field": v} for k, v in row_inputs.items()}
            version = info.pop("version", 1)
            ref = f"{stage}.{name}@{version}"
            out[ref] = {"id": ref, "stage": stage, "name": name, "version": version, **info}
    return out


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.meta = json.loads((ROOT / "tests/fixtures/gold_tables.json").read_text())
        self.meta = {k: v for k, v in self.meta.items() if not k.startswith("_")}
        client = json.loads((ROOT / "config/development.resolution_config.json").read_text())
        self.meta = config.client_resolver_tables(self.meta, client)
        self.registry = definitions()

    def build(self):
        return catalog.build_full_catalog(self.meta, self.registry, run_id="test-run")

    def test_actual_platform_dispatch_overrides_authored_label(self):
        doc = self.build()
        field = next(f for f in doc["tables"]["orders"]["fields"] if f["key"] == "sales_rep_e_email")
        self.assertEqual(field["provenance"]["resolution"]["strategies"][0]["strategy"], "resolve.sales_rep_order_native@2")
        self.assertEqual(field["provenance"]["resolution"]["by_platform"]["shopify"][1]["strategy"], "resolve.sales_rep_company_owner@1")
        self.assertNotIn("enrich.full_join_bring@1", doc["strategies"])
        self.assertEqual(doc["run_id"], "test-run")

    def test_client_override_and_arbitrary_target_names(self):
        self.meta["orders"]["resolve"]["custom_assignment"] = self.meta["orders"]["resolve"].pop("sales_rep_e_email")
        self.meta["orders"]["catalog"]["fields"]["custom_assignment"] = {"label": "Assignment", "dataType": "string"}
        self.meta["orders"]["resolve"]["custom_assignment"]["strategies"] = ["sales_rep_company_owner"]
        field = next(f for f in self.build()["tables"]["orders"]["fields"] if f["key"] == "custom_assignment")
        self.assertEqual(field["provenance"]["evidence"]["column"], "custom_assignment_evidence")

    def test_enrichment_retains_origin_and_metric_inputs(self):
        field = next(f for f in self.build()["tables"]["companies"]["fields"] if f["key"] == "company_e_ltv")
        origin = field["provenance"]["origin"]
        self.assertEqual(origin["provenance"]["calculation"]["of"], "orders_e_line_revenue")
        self.assertEqual(origin["provenance"]["calculation"]["over"], "company_e_id")

    def test_identity_available_without_exposing_all_hidden_fields(self):
        table = self.build()["tables"]["orders"]
        self.assertIn("orders_e_order_id", table["evidence_query_fields"])
        self.assertNotIn("company_e_mismatch", table["evidence_query_fields"])

    def test_missing_definition_fails_closed(self):
        self.registry.pop("resolve.sales_rep_order_native@2")
        with self.assertRaises(ValueError):
            self.build()

    def test_missing_exported_evidence_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "Missing generated evidence"):
            catalog.build_full_catalog(self.meta, self.registry, schema_columns={"orders": []})

    def test_gold_files_are_valid_python(self):
        for path in (ROOT / "Gold").glob("*.py"):
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def test_development_config_passes_complete_metadata_validation(self):
        module_spec = importlib.util.spec_from_file_location("validator", ROOT / "Gold/gold_validate.py")
        validator = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(validator)
        validator.resolver_order = config.resolver_order
        registries = {stage: {} for stage in ("dedup", "resolve", "metric", "enrich", "derive")}
        for definition in self.registry.values():
            inputs = {k: v["default_field"] for k, v in definition.get("inputs", {}).items()}
            registries[definition["stage"]][definition["name"]] = SimpleNamespace(row_inputs=inputs)
        validator.validate_gold(self.meta, *registries.values())


if __name__ == "__main__":
    unittest.main()
