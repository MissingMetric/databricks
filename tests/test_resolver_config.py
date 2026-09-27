import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

spec = importlib.util.spec_from_file_location('config', Path(__file__).resolve().parents[1] / 'Gold/gold_resolver_config.py')
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


class ResolverConfigTests(unittest.TestCase):
    def test_precedence_and_platform_replacement(self):
        variants = config.resolver_variants({'strategies': ['a', 'b'], 'by_platform': {'shopify': {'strategies': ['b']}}})
        self.assertEqual([s['strategy'] for s in variants[None]], ['a', 'b'])
        self.assertEqual([s['strategy'] for s in variants['shopify']], ['b'])

    def test_legacy_empty_and_duplicate_steps_rejected(self):
        for bad in ({'strategy': 'a'}, {'strategies': []}, {'strategies': ['a', 'a']}, {'strategies': ['a'], 'by_platform': {'x': 'a'}}):
            with self.assertRaises(ValueError):
                config.resolver_variants(bad)

    def test_dependencies_not_declaration_order(self):
        registry = {'owner': SimpleNamespace(row_inputs={'company_field': 'company'}), 'name': SimpleNamespace(row_inputs={'field': 'name'})}
        resolvers = {'rep': {'strategies': ['owner']}, 'company': {'strategies': ['name']}}
        self.assertEqual(config.resolver_order(resolvers, registry, ['name']), ['company', 'rep'])
        registry['name'].row_inputs['field'] = 'rep'
        with self.assertRaisesRegex(ValueError, 'Cyclic'):
            config.resolver_order(resolvers, registry, ['name'])

    def test_missing_input_unknown_strategy_and_params(self):
        registry = {'a': SimpleNamespace(row_inputs={'field': 'native'})}
        for chain in (['unknown'], ['a'], [{'strategy': 'a', 'params': {'oops': 'native'}}]):
            with self.assertRaises(ValueError):
                config.resolver_order({'target': {'strategies': chain}}, registry, [])

    def test_client_config_replaces_never_merges(self):
        tables = {'orders': {'resolve': {'old': {'strategy': 'legacy'}}, 'base_columns': ['x']}}
        result = config.client_resolver_tables(tables, {'schema_version': 1, 'tables': {'orders': {'resolve': {}}}})
        self.assertEqual(result['orders']['resolve'], {})
        self.assertIn('old', tables['orders']['resolve'])
        self.assertEqual(result['orders']['base_columns'], ['x'])
        for bad in ({}, {'schema_version': 1, 'tables': {}}):
            with self.assertRaises(ValueError):
                config.client_resolver_tables(tables, bad)


if __name__ == '__main__':
    unittest.main()
