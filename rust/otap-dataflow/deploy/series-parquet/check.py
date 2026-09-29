#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Checks the alert rules and the dashboard of this directory.

Usage: check.py [--write-crd FILE [--release NAME]] [--crd FILE ...]
                [--schema JSON ...]
                [--absent NAME | --absent 'NAME{otel_scope_name="SET"}' ...]
                [SCRAPE ...]

--write-crd generates the prometheus-operator PrometheusRule of
alerts/series-parquet.rules.yaml into FILE (`-` for standard output), with
the `release` label NAME when --release is given; the generated text must
read back as exactly the rules file's groups. Each --crd FILE, a generated
or an applied PrometheusRule (`kubectl get prometheusrule series-parquet -o
yaml`), must carry exactly those groups; its metadata is the site's and is
not compared.

Every metric the rules and the dashboard select, by name and by its
`otel_scope_name` matcher, must be in one of the SCRAPE files (Prometheus
text from /api/v1/metrics of the engine or /metrics of Alloy) or in one of
the --schema files, except the ones given with --absent. A --schema file is
the engine's /api/v1/telemetry/metrics?format=json&keep_all_zeroes=true: it
lists every registered metric with every value of its enumerated labels, so
it also covers counters that never moved. For a metric the schema knows,
every label a matcher or a `by (...)` names must be one of its labels, and
every value an `=` or `=~` matcher names must be one of that label's values.
Without SCRAPE and --schema files only the PrometheusRule is checked.
"""
import argparse
import json
import pathlib
import re
import sys

import yaml

HERE = pathlib.Path(__file__).resolve().parent
RULES = HERE / "alerts" / "series-parquet.rules.yaml"
DASHBOARD = HERE / "dashboard" / "series-parquet.json"
SELECTOR = re.compile(r"\b([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}")
MATCHER = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"')
GROUPING = re.compile(r"\b(?:by|without)\s*\(([^)]*)\)")
# The destination label of a label_replace: its last four arguments.
LABEL_REPLACE = re.compile(r',\s*"([a-zA-Z_][a-zA-Z0-9_]*)"\s*,\s*"[^"]*"\s*,\s*"[^"]*"\s*,\s*"[^"]*"\s*\)')
SCOPE_ABSENT = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{otel_scope_name="([^"]*)"\}$')
WORDS = re.compile(r"^[a-zA-Z0-9_.:-]+(\|[a-zA-Z0-9_.:-]+)*$")
# Suffixes the engine's Prometheus names add to an instrument's name: its
# unit, `_total` for a counter, the parts of a distribution.
NAME_SUFFIX = re.compile(r"^(_[a-z]+)?(_total|_sum|_count|_min|_max|_bucket)?$")
# Series Prometheus adds to every scrape target itself.
SYNTHETIC = {"up"}
# Series the restart rules read from kube-state-metrics, outside any scrape
# of this deployment.
EXTERNAL = {
    "kube_pod_container_status_restarts_total",
    "kube_pod_container_status_last_terminated_reason",
}
# Labels the scrape configuration adds to every series of a target.
TARGET_LABELS = {"job", "namespace", "pod", "instance", "container", "service", "endpoint"}
CRD_HEADER = """\
# Generated from alerts/series-parquet.rules.yaml by check.py --write-crd.
# kube-prometheus-stack loads rules whose labels match its ruleSelector,
# by default `release: <its Helm release>`: generate with
# `check.py --write-crd FILE --release <release>`, or match your selector.
"""


def crd_for(groups, labels=None):
    metadata = {"name": "series-parquet"}
    if labels:
        metadata["labels"] = labels
    return {
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PrometheusRule",
        "metadata": metadata,
        "spec": {"groups": groups},
    }


def crd_errors(name, crd, groups):
    """Errors of a PrometheusRule `crd` that does not carry exactly `groups`;
    its metadata is not compared."""
    if not isinstance(crd, dict) or (crd.get("apiVersion"), crd.get("kind")) != (
        "monitoring.coreos.com/v1",
        "PrometheusRule",
    ):
        return [f"{name} is not a monitoring.coreos.com/v1 PrometheusRule"]
    if (crd.get("spec") or {}).get("groups") != groups:
        return [f"{name} differs from {RULES.name}; generate it with check.py --write-crd"]
    return []


def crd_text(groups, labels=None):
    """The PrometheusRule of `groups` as the YAML --write-crd writes."""
    return CRD_HEADER + yaml.dump(
        crd_for(groups, labels), Dumper=BlockDumper, sort_keys=False, width=1000
    )


class BlockDumper(yaml.SafeDumper):
    """Writes multi-line strings as literal blocks, as the rules file does."""


BlockDumper.add_representer(
    str,
    lambda dumper, value: dumper.represent_scalar(
        "tag:yaml.org,2002:str", value, style="|" if "\n" in value else None
    ),
)


def prom_label(key):
    """The Prometheus label key the engine writes for attribute `key`."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", key)


def matchers_of(text):
    """(key, operator, value) of every matcher in a selector's braces."""
    return MATCHER.findall(text)


def scope_of(matchers):
    for key, op, value in matchers:
        if key == "otel_scope_name" and op == "=":
            return value
    return None


def expressions(groups, dashboard):
    """(where, PromQL) of every expression the rules and the dashboard hold."""
    out = [(f"rule {rule['alert']}", rule["expr"]) for group in groups for rule in group["rules"]]
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            out.append((f"panel {panel['title']!r}", target["expr"]))
    for var in dashboard["templating"]["list"]:
        if "definition" in var:
            out.append((f"variable {var['name']}", var["definition"]))
    return out


def selectors(expr):
    """(name, scope, matchers) of every `name{...}` in `expr`."""
    out = []
    for name, body in SELECTOR.findall(expr):
        matchers = matchers_of(body)
        out.append((name, scope_of(matchers), matchers))
    return out


def text_value(value):
    """The string form of an attribute value of the JSON metrics API."""
    if isinstance(value, dict) and len(value) == 1:
        (inner,) = value.values()
        return text_value(inner)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class Known:
    """What the scrapes and the schemas say a metric carries.

    `metrics[(name, scope)]` maps each label key to the set of its values;
    `schema[scope]` lists (base name, labels) of every instrument of a
    metric set the schema registers, base being its Prometheus name without
    unit and type suffixes.
    """

    def __init__(self):
        self.metrics = {}
        self.schema = {}

    def add_scrape(self, text):
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            name = re.split(r"[{ ]", line, maxsplit=1)[0]
            body = line[len(name) + 1: line.rfind("}")] if line[len(name):].startswith("{") else ""
            matchers = matchers_of(body)
            labels = {key: {value} for key, _, value in matchers}
            for scope in {None, scope_of(matchers)}:
                entry = self.metrics.setdefault((name, scope), {})
                for key, values in labels.items():
                    entry.setdefault(key, set()).update(values)

    def add_schema(self, document):
        for metric_set in document["metric_sets"]:
            scope = metric_set["name"]
            set_labels = {
                "otel_scope_" + prom_label(key): {text_value(value)}
                for key, value in metric_set.get("attributes", {}).items()
            }
            instruments = self.schema.setdefault(scope, {})
            for metric in metric_set["metrics"]:
                base = prom_label(metric["name"])
                labels = instruments.setdefault(base, {})
                for key, values in set_labels.items():
                    labels.setdefault(key, set()).update(values)
                for key, value in metric.get("attributes", {}).items():
                    labels.setdefault(prom_label(key), set()).add(text_value(value))

    def schema_labels(self, name, scope):
        """Labels of the schema instrument `name` names in `scope`, or None."""
        best = None
        for base, labels in self.schema.get(scope, {}).items():
            if name.startswith(base) and NAME_SUFFIX.match(name[len(base):]):
                if best is None or len(base) > len(best[0]):
                    best = (base, labels)
        return None if best is None else best[1]

    def labels(self, name, scope):
        """Every label key and value known for `name` in `scope`, and whether
        the schema knows the metric; None when nothing knows it."""
        schema = self.schema_labels(name, scope) if scope else None
        scraped = self.metrics.get((name, scope))
        if schema is None and scraped is None:
            return None
        merged = {}
        for source in (schema or {}, scraped or {}):
            for key, values in source.items():
                merged.setdefault(key, set()).update(values)
        return merged, schema is not None


def check_values(where, name, key, op, pattern, values):
    """Errors for an `=` or `=~` matcher naming a value `values` lacks."""
    if op == "=":
        wanted = [pattern]
    elif op == "=~" and WORDS.match(pattern):
        wanted = pattern.split("|")
    elif op == "=~":
        if any(re.fullmatch(pattern, value) for value in values):
            return []
        return [f"{where}: {name} {key}=~{pattern!r} matches no value of {sorted(values)}"]
    else:
        return []
    return [
        f"{where}: {name} has no {key}={value!r} (values: {', '.join(sorted(values))})"
        for value in wanted
        if value not in values
    ]


def check_expression(where, expr, known, absent):
    """Errors of one expression: unknown metrics, labels and label values."""
    errors, accepted = [], []
    grouping_keys = set(TARGET_LABELS) | set(LABEL_REPLACE.findall(expr))
    schema_complete = True
    for name, scope, matchers in selectors(expr):
        if name in SYNTHETIC or name in EXTERNAL:
            schema_complete = False
            continue
        found = known.labels(name, scope)
        if found is None:
            if (name, scope) in absent:
                accepted.append(describe((name, scope)))
            else:
                errors.append(f"{where}: not in any scrape or schema: {describe((name, scope))}")
            schema_complete = False
            continue
        labels, from_schema = found
        grouping_keys |= set(labels)
        if not from_schema:
            schema_complete = False
            continue
        for key, op, value in matchers:
            if key in TARGET_LABELS or key == "otel_scope_name":
                continue
            if key not in labels:
                errors.append(f"{where}: {name} (otel_scope_name={scope}) has no label {key}")
                continue
            errors += check_values(where, name, key, op, value, labels[key])
    if schema_complete:
        for group in GROUPING.findall(expr):
            for key in (k.strip() for k in group.split(",")):
                if key and key not in grouping_keys:
                    errors.append(f"{where}: groups by {key}, which no selected metric carries")
    return errors, accepted


def describe(key):
    name, scope = key
    return f"{name} (otel_scope_name={scope})" if scope else name


def parse_absent(values):
    """--absent values as (name, scope) keys; a bare name has no scope."""
    keys = set()
    for value in values:
        scoped = SCOPE_ABSENT.match(value)
        keys.add((scoped.group(1), scoped.group(2)) if scoped else (value, None))
    return keys


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-crd", metavar="FILE")
    parser.add_argument("--release")
    parser.add_argument("--crd", action="append", default=[])
    parser.add_argument("--absent", action="append", default=[])
    parser.add_argument("--schema", action="append", default=[])
    parser.add_argument("scrape", nargs="*")
    args = parser.parse_args(argv)

    groups = yaml.safe_load(RULES.read_text())["groups"]
    labels = {"release": args.release} if args.release else None
    text = crd_text(groups, labels)
    errors = crd_errors("the generated PrometheusRule", yaml.safe_load(text), groups)
    # With the PrometheusRule on standard output, the report goes to standard error.
    report = sys.stdout
    if args.write_crd == "-":
        sys.stdout.write(text)
        report = sys.stderr
    elif args.write_crd:
        pathlib.Path(args.write_crd).write_text(text)
    for path in args.crd:
        errors += crd_errors(path, yaml.safe_load(pathlib.Path(path).read_text()), groups)

    if args.scrape or args.schema:
        known = Known()
        for path in args.scrape:
            known.add_scrape(pathlib.Path(path).read_text())
        for path in args.schema:
            known.add_schema(json.loads(pathlib.Path(path).read_text()))
        absent = parse_absent(args.absent)
        accepted = set()
        exprs = expressions(groups, json.loads(DASHBOARD.read_text()))
        for where, expr in exprs:
            found, ok = check_expression(where, expr, known, absent)
            errors += found
            accepted.update(ok)
        for key in sorted(accepted):
            print(f"absent, accepted: {key}", file=report)
        print(f"{len(exprs)} expressions checked", file=report)
    for error in errors:
        print(error, file=report)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
