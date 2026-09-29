# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Unit tests of check.py's selector checks, on inline scrapes and schemas."""
import contextlib
import io
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check  # noqa: E402

SCOPE = "exporter.series_parquet"
SCHEMA = {
    "metric_sets": [
        {
            "name": SCOPE,
            "attributes": {"core.id": {"UInt": 0}},
            "metrics": [
                {
                    "name": "block.write_failures",
                    "unit": "{block}",
                    "attributes": {"error.type": {"String": value}},
                    "value": 0,
                }
                for value in ("deadline", "permanent_storage")
            ],
        }
    ]
}
SCRAPE = (
    'up{job="series-parquet"} 1\n'
    'flush_failures_total{otel_scope_name="processor.durable_buffer"} 0\n'
    'loki_process_truncated_fields_total{field="line"} 0\n'
)


def known():
    result = check.Known()
    result.add_schema(SCHEMA)
    result.add_scrape(SCRAPE)
    return result


def errors(expr, absent=()):
    return check.check_expression("rule", expr, known(), check.parse_absent(absent))


class SelectorChecks(unittest.TestCase):
    # Scenario: a counter that never moved, selected with a label value the
    # engine registers, known only from the schema.
    # Guarantees: the selector passes without a scrape line or --absent.
    def test_schema_covers_a_counter_that_never_moved(self):
        found, accepted = errors(
            f'sum by (pod, error_type) (block_write_failures_total{{otel_scope_name="{SCOPE}", error_type="deadline"}})'
        )
        self.assertEqual((found, accepted), ([], []))

    # Scenario: selectors naming a label value, a regex alternative and a
    # label key the engine does not register.
    # Guarantees: each is reported, naming the metric and what is wrong.
    def test_unknown_label_values_and_keys_are_reported(self):
        for expr, fragment in [
            (f'block_write_failures_total{{otel_scope_name="{SCOPE}", error_type="timeout"}}', "error_type='timeout'"),
            (f'block_write_failures_total{{otel_scope_name="{SCOPE}", error_type=~"deadline|gone"}}', "error_type='gone'"),
            (f'block_write_failures_total{{otel_scope_name="{SCOPE}", reason="x"}}', "no label reason"),
            (f'sum by (outcome) (block_write_failures_total{{otel_scope_name="{SCOPE}"}})', "groups by outcome"),
        ]:
            with self.subTest(expr=expr):
                found, _ = errors(expr)
                self.assertEqual(len(found), 1, found)
                self.assertIn(fragment, found[0])

    # Scenario: a metric name selected under the scope of a set that has no
    # such instrument, although another set exposes that name.
    # Guarantees: it is reported; a bare --absent does not accept a scoped
    # selector, a scoped --absent does.
    def test_absent_is_scope_aware(self):
        expr = f'flush_failures_total{{otel_scope_name="{SCOPE}"}}'
        found, _ = errors(expr, absent=["flush_failures_total"])
        self.assertEqual(len(found), 1, found)
        found, accepted = errors(expr, absent=[f'flush_failures_total{{otel_scope_name="{SCOPE}"}}'])
        self.assertEqual(found, [])
        self.assertEqual(accepted, [f"flush_failures_total (otel_scope_name={SCOPE})"])

    # Scenario: an Alloy metric without a scope, in a scrape, and one missing
    # from every file and accepted by name.
    # Guarantees: the first passes, the second is accepted, and neither has
    # its labels compared with a schema.
    def test_unscoped_metrics_use_the_scrape_or_absent(self):
        found, _ = errors('loki_process_truncated_fields_total{job="a", field="line"}')
        self.assertEqual(found, [])
        found, accepted = errors(
            'otelcol_exporter_send_failed_log_records_total{job="a"}',
            absent=["otelcol_exporter_send_failed_log_records_total"],
        )
        self.assertEqual((found, accepted), ([], ["otelcol_exporter_send_failed_log_records_total"]))

    # Scenario: a label_replace adds the `altered` label that an outer
    # aggregation groups by.
    # Guarantees: the added label counts as carried.
    def test_label_replace_adds_a_grouping_label(self):
        found, _ = errors(
            f'sum by (altered) (label_replace(block_write_failures_total{{otel_scope_name="{SCOPE}"}}, "altered", "x", "", ""))'
        )
        self.assertEqual(found, [])


class RuleFile(unittest.TestCase):
    # Scenario: the PrometheusRule --write-crd generates from the rules file,
    # with and without the site's `release` label.
    # Guarantees: it reads back as the rules file's groups, and only its
    # labels differ between the two.
    def test_generated_prometheusrule_carries_the_rules_file(self):
        groups = check.yaml.safe_load(check.RULES.read_text())["groups"]
        for labels in (None, {"release": "x"}):
            with self.subTest(labels=labels):
                crd = check.yaml.safe_load(check.crd_text(groups, labels))
                self.assertEqual(crd, check.crd_for(groups, labels))
                self.assertEqual(check.crd_errors("generated", crd, groups), [])

    # Scenario: --write-crd to a file, then --crd on that file, on a copy
    # with a changed rule, and on a copy with other metadata, as an applied
    # PrometheusRule has.
    # Guarantees: the written file passes, the changed rule is reported, the
    # metadata is not compared.
    def test_crd_file_must_carry_the_rules_file(self):
        groups = check.yaml.safe_load(check.RULES.read_text())["groups"]
        with tempfile.TemporaryDirectory() as directory:
            written = pathlib.Path(directory) / "prometheusrule.yaml"
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(check.main(["--write-crd", str(written)]), 0)
                self.assertEqual(check.main(["--crd", str(written)]), 0)
            crd = check.yaml.safe_load(written.read_text())
            crd["metadata"] = {"name": "series-parquet", "namespace": "monitoring", "uid": "1"}
            self.assertEqual(check.crd_errors("applied", crd, groups), [])
            crd["spec"]["groups"][0]["rules"][0]["for"] = "1h"
            changed = pathlib.Path(directory) / "changed.yaml"
            changed.write_text(check.yaml.safe_dump(crd))
            report = io.StringIO()
            with contextlib.redirect_stdout(report):
                self.assertEqual(check.main(["--crd", str(changed)]), 1)
            self.assertIn("changed.yaml differs from series-parquet.rules.yaml", report.getvalue())


if __name__ == "__main__":
    unittest.main()
