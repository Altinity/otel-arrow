#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Independent implementations of the series lake format's golden vectors.

Written from docs/FORMAT.md, not from the Rust code:

- canonical_v1.json: canonical identity encoding v1 and series ids (section 1).
- schema_fingerprint_v1.json: the schema rendering and schema_fingerprint of
  every dataset plus one denormalized schema (section 3).
- render_v1.json: the attribute-map and log-body strings render_v1 produces
  (section 2), including the non-finite double and base64 bytes spellings.
- cbor_v1.json: raw CBOR `ser` cells, the value each decodes to or the
  refusal, and the identity of a log series carrying that value (section 1,
  "CBOR decoding").
- identity_config_v1.json: the identity_config file metadata value and its
  hash for several allow-lists (section 5).

Usage: gen_golden.py <golden directory>   (normally tests/golden)
Requires: pip install --require-hashes -r tools/requirements.lock.txt
"""
import base64, decimal, json, math, os, struct, sys
import xxhash

TAG = dict(str=1, bytes=2, int=3, double=4, bool=5, null=6, array=7, kvlist=8)
CANONICAL_NAN = 0x7FF8000000000000
NEGATIVE_ZERO = 0x8000000000000000


def put(tag, payload):
    return bytes([tag]) + struct.pack(">I", len(payload)) + payload


def enc_str(s):
    return put(TAG["str"], s.encode("utf-8"))


def enc_value(v):
    t = v["type"]
    if t == "null":
        return put(TAG["null"], b"")
    if t == "str":
        return enc_str(v["value"])
    if t == "bytes":
        return put(TAG["bytes"], bytes.fromhex(v["value"]))
    if t == "int":
        return put(TAG["int"], struct.pack(">q", v["value"]))
    if t == "double":
        bits = v["bits"] if "bits" in v else struct.unpack(">Q", struct.pack(">d", v["value"]))[0]
        exp = bits >> 52 & 0x7FF
        mant = bits & ((1 << 52) - 1)
        if exp == 0x7FF and mant != 0:
            # Every NaN collapses to the canonical quiet NaN.
            bits = CANONICAL_NAN
        elif bits == NEGATIVE_ZERO:
            # -0.0 collapses to +0.0: OTAP cannot carry the sign of a zero, so
            # an identity must not depend on it (FORMAT.md section 1).
            bits = 0
        return put(TAG["double"], struct.pack(">Q", bits))
    if t == "bool":
        return put(TAG["bool"], bytes([1 if v["value"] else 0]))
    if t == "array":
        payload = struct.pack(">I", len(v["items"])) + b"".join(enc_value(i) for i in v["items"])
        return put(TAG["array"], payload)
    if t == "kvlist":
        return enc_kvlist(v["entries"])
    raise ValueError(t)


def enc_kvlist(entries):
    entries = sorted(entries, key=lambda e: e["key"].encode("utf-8"))
    payload = struct.pack(">I", len(entries))
    for e in entries:
        payload += enc_str(e["key"]) + enc_value(e["value"])
    return put(TAG["kvlist"], payload)


def canonical(d):
    out = enc_str("OTEL-SERIES/1") + enc_str(d["signal"])
    out += enc_kvlist(d.get("resource_attrs", []))
    out += enc_str(d.get("resource_schema_url", ""))
    out += enc_str(d.get("scope_name", ""))
    out += enc_str(d.get("scope_version", ""))
    out += enc_str(d.get("scope_schema_url", ""))
    out += enc_kvlist(d.get("scope_attrs", []))
    has_metric = d["signal"] == "metrics"
    assert has_metric == ("metric" in d and d["metric"] is not None), \
        "metric must be present iff signal is metrics"
    if has_metric:
        m = d["metric"]
        out += enc_str(m["name"]) + enc_str(m.get("unit", "")) + enc_str(m["kind"])
        out += enc_str(m.get("temporality", "")) + put(TAG["bool"], bytes([1 if m.get("is_monotonic") else 0]))
    out += enc_kvlist(d.get("attrs", []))
    return out


def kv(key, value):
    return {"key": key, "value": value}


S = lambda s: {"type": "str", "value": s}
I = lambda i: {"type": "int", "value": i}
# D takes a finite Python float; non-finite doubles must use DB(bits) so that
# json.dump(allow_nan=False) never emits bare Infinity/NaN, which serde_json rejects.
D = lambda f: {"type": "double", "value": f}
DB = lambda bits: {"type": "double", "bits": bits}
B = lambda hexs: {"type": "bytes", "value": hexs}
BOOL = lambda b: {"type": "bool", "value": b}
NULL = {"type": "null"}
ARR = lambda *items: {"type": "array", "items": list(items)}
KV = lambda *entries: {"type": "kvlist", "entries": list(entries)}

def allow_listed(base, series_attributes, record_attrs):
    """A logs descriptor whose identity attributes are the record's attributes
    named in `series_attributes` (FORMAT.md section 1): a configured key the
    record lacks is omitted, a present key keeps its value, unset included."""
    wanted = set(series_attributes)
    return {**base, "series_attributes": series_attributes, "record_attrs": record_attrs,
            "attrs": [e for e in record_attrs if e["key"] in wanted]}


base_logs = dict(signal="logs", resource_attrs=[kv("host.id", S("a1"))], scope_name="lib", scope_version="1")
base_metrics = dict(signal="metrics", resource_attrs=[kv("host.id", S("a1"))],
                    metric=dict(name="cpu.usage", unit="s", kind="gauge", temporality="", is_monotonic=False))

cases = [
    ("logs_minimal", base_logs),
    ("empty_string_attr", {**base_logs, "attrs": [kv("k", S(""))]}),
    ("missing_scope", {**base_logs, "scope_name": "", "scope_version": ""}),
    ("int_zero", {**base_logs, "attrs": [kv("k", I(0))]}),
    ("neg_zero_double", {**base_logs, "attrs": [kv("k", DB(0x8000000000000000))]}),
    # Same canonical bytes and series id as neg_zero_double: golden.rs asserts it.
    ("pos_zero_double", {**base_logs, "attrs": [kv("k", DB(0x0000000000000000))]}),
    ("int_min", {**base_logs, "attrs": [kv("k", I(-2**63))]}),
    ("int_max", {**base_logs, "attrs": [kv("k", I(2**63 - 1))]}),
    ("int_42", {**base_logs, "attrs": [kv("k", I(42))]}),
    ("str_42", {**base_logs, "attrs": [kv("k", S("42"))]}),
    ("nan_quiet", {**base_logs, "attrs": [kv("k", DB(0x7FF8000000000000))]}),
    ("nan_payload", {**base_logs, "attrs": [kv("k", DB(0x7FF8000000000001))]}),
    ("nan_negative", {**base_logs, "attrs": [kv("k", DB(0xFFF8000000000000))]}),
    # pos_inf / neg_inf: DB(...) bit patterns, not D(math.inf) / D(-math.inf),
    # so json.dump(..., allow_nan=False) never has to serialize a bare float
    # infinity (serde_json rejects bare Infinity/NaN in JSON text).
    ("pos_inf", {**base_logs, "attrs": [kv("k", DB(0x7FF0000000000000))]}),
    ("neg_inf", {**base_logs, "attrs": [kv("k", DB(0xFFF0000000000000))]}),
    ("unicode_bmp", {**base_logs, "attrs": [kv("k", S("\u00e9\u4e2d"))]}),
    ("unicode_supplementary", {**base_logs, "attrs": [kv("k", S("\U0001F600"))]}),
    ("embedded_nul", {**base_logs, "attrs": [kv("k", S("a\u0000b"))]}),
    ("bytes", {**base_logs, "attrs": [kv("k", B("00ff10"))]}),
    ("null_value", {**base_logs, "attrs": [kv("k", NULL)]}),
    ("bool_true", {**base_logs, "attrs": [kv("k", BOOL(True))]}),
    ("nested_array", {**base_logs, "attrs": [kv("k", ARR(I(1), S("x"), ARR(NULL)))]}),
    ("nested_kvlist", {**base_logs, "attrs": [kv("k", KV(kv("b", I(2)), kv("a", KV(kv("z", BOOL(False))))))]}),
    ("key_order_prefix", {**base_logs, "attrs": [kv("ab", I(1)), kv("a", I(2)), kv("a\u00e9", I(3)), kv("b", I(4))]}),
    ("metrics_gauge", base_metrics),
    ("metrics_sum_delta_monotonic", {**base_metrics, "metric": dict(name="req", unit="1", kind="sum", temporality="delta", is_monotonic=True)}),
    ("metrics_histogram_cumulative", {**base_metrics, "metric": dict(name="lat", unit="ms", kind="histogram", temporality="cumulative", is_monotonic=False)}),
    ("metrics_exp_histogram_delta", {**base_metrics, "metric": dict(name="lat", unit="ms", kind="exp_histogram", temporality="delta", is_monotonic=False)}),
    ("metrics_summary", {**base_metrics, "metric": dict(name="lat", unit="ms", kind="summary", temporality="", is_monotonic=False)}),
    ("metrics_dp_attrs", {**base_metrics, "attrs": [kv("cpu", I(3)), kv("mode", S("user"))]}),
    ("metrics_scope_attrs", {**base_metrics, "scope_attrs": [kv("s", S("v"))], "scope_name": "m", "scope_version": "2"}),
    ("producer_a", {**base_logs, "resource_attrs": [kv("host.id", S("a"))]}),
    ("producer_b", {**base_logs, "resource_attrs": [kv("host.id", S("b"))]}),
    ("schema_urls", {**base_logs, "resource_schema_url": "https://r", "scope_schema_url": "https://s"}),
    # Only the configured keys the record carries are in the identity: "gone"
    # is configured but absent (omitted, not null), "extra" is carried but not
    # configured, and "empty" is present with an unset value (null).
    ("logs_allow_list_missing_key", allow_listed(
        base_logs, ["gone", "user", "empty"],
        [kv("user", S("u1")), kv("extra", I(1)), kv("empty", NULL)])),
]


# Schema fingerprint (FORMAT.md section 3). Each dataset is listed as
# (name, type, nullable) straight from the column tables of section 2, with the
# type already spelled in the series-lake-schema/1 vocabulary.
SCHEMA_HEADER = "series-lake-schema/1"
TS = "Ts<us,UTC>"
MAP = "Map<Str!,Str?>"
SERIES_COMMON = [
    ("series_id", "FSB<16>", False),
    ("identity_bytes", "Bin", False),
    ("emitted_at", TS, False),
    ("resource_schema_url", "Str", False),
    ("resource_attrs", MAP, False),
    ("scope_name", "Str", False),
    ("scope_version", "Str", False),
    ("scope_schema_url", "Str", False),
    ("scope_attrs", MAP, False),
    ("attrs", MAP, False),
]
DATASETS = {
    "logs/series": SERIES_COMMON,
    "logs/values": [
        ("series_id", "FSB<16>", False),
        ("producer_id", "Str", False),
        ("time", TS, True),
        ("time_unix_nano", "I64", True),
        ("observed_time", TS, True),
        ("observed_time_unix_nano", "I64", True),
        ("severity_number", "I32", False),
        ("severity_text", "Str", False),
        ("body", "Str", True),
        ("event_name", "Str", False),
        ("trace_id", "FSB<16>", True),
        ("span_id", "FSB<8>", True),
        ("flags", "I32", False),
        ("attrs", MAP, False),
    ],
    "metrics/series": SERIES_COMMON + [
        ("metric_name", "Str", False),
        ("unit", "Str", False),
        ("metric_type", "Str", False),
        ("temporality", "Str", False),
        ("is_monotonic", "Bol", False),
        ("description", "Str", False),
    ],
    "metrics/values": [
        ("series_id", "FSB<16>", False),
        ("producer_id", "Str", False),
        ("metric_name", "Str", False),
        ("time", TS, True),
        ("time_unix_nano", "I64", True),
        ("start_time", TS, True),
        ("start_time_unix_nano", "I64", True),
        ("flags", "I32", False),
        ("value_int", "I64", True),
        ("value_double", "F64", True),
        ("count", "I64", True),
        ("sum", "F64", True),
        ("min", "F64", True),
        ("max", "F64", True),
        ("bucket_counts", "[I64!]", True),
        ("explicit_bounds", "[F64!]", True),
    ],
}
# Denormalized columns are appended after the intrinsic ones, always nullable.
DENORM_TYPE = {"string": "Str", "int64": "I64", "double": "F64", "bool": "Bol"}
DENORMALIZED = [
    {"path": "resource.service.name", "column": "service_name", "type": "string"},
    {"path": "attrs.http.status_code", "column": "http_status_code", "type": "int64"},
    {"path": "attrs.latency", "column": "latency", "type": "double"},
    {"path": "attrs.cached", "column": "cached", "type": "bool"},
]


def schema_rendering(fields):
    out = SCHEMA_HEADER + "\n"
    for name, ty, nullable in fields:
        ty = ty + ("?" if nullable else "!")
        out += "%d:%s%d:%s\n" % (len(name.encode("utf-8")), name, len(ty.encode("utf-8")), ty)
    return out


schemas = []
for dataset, fields in DATASETS.items():
    schemas.append({"name": dataset, "dataset": dataset, "logs_denormalize": [], "fields": fields})
schemas.append({
    "name": "logs/values+denormalized", "dataset": "logs/values",
    "logs_denormalize": DENORMALIZED,
    # Every path is outside the series identity except the resource one, but
    # a values dataset carries every denormalized column (section 3).
    "fields": DATASETS["logs/values"] + [(d["column"], DENORM_TYPE[d["type"]], True) for d in DENORMALIZED],
})
schema_vectors = []
for sch in schemas:
    rendering = schema_rendering(sch["fields"])
    schema_vectors.append({
        "name": sch["name"], "dataset": sch["dataset"],
        "logs_denormalize": sch["logs_denormalize"],
        "rendering": rendering,
        "fingerprint": "%016x" % xxhash.xxh3_64_intdigest(rendering.encode("utf-8")),
    })


# render_v1 (FORMAT.md section 2): the spellings of the workspace OTLP JSON
# encoder for non-finite doubles and bytes.
def double_of(v):
    if "bits" in v:
        return struct.unpack(">d", struct.pack(">Q", v["bits"]))[0]
    return v["value"]


class JsonNumber:
    """A JSON number already spelled as FORMAT.md section 2 requires."""

    def __init__(self, text):
        self.text = text


def spell_double(d):
    """A finite double as FORMAT.md section 2 spells it.

    Shortest round-trip digits (Python's repr picks the same digits), laid out
    in fixed notation when the decimal exponent of the first digit is in
    -5..=15 and in scientific notation otherwise, with an explicit exponent
    sign and no exponent zero padding: 1e+16, 1e-7, 0.00001, 3.0.
    """
    if d == 0:
        return "-0.0" if math.copysign(1.0, d) < 0 else "0.0"
    sign = "-" if d < 0 else ""
    t = decimal.Decimal(repr(abs(d))).normalize().as_tuple()
    digits = "".join(str(x) for x in t.digits)
    exp = len(digits) - 1 + t.exponent  # decimal exponent of the first digit
    if -5 <= exp <= 15:
        point = exp + 1  # digits before the decimal point
        if point <= 0:
            body = "0." + "0" * (-point) + digits
        elif point >= len(digits):
            body = digits + "0" * (point - len(digits)) + ".0"
        else:
            body = digits[:point] + "." + digits[point:]
    else:
        body = digits[0] + ("." + digits[1:] if len(digits) > 1 else "")
        body += "e" + ("+" if exp >= 0 else "-") + str(abs(exp))
    return sign + body


def render_v1(v):
    t = v["type"]
    if t == "null":
        return None
    if t == "str":
        return v["value"]
    if t == "bytes":
        return base64.standard_b64encode(bytes.fromhex(v["value"])).decode("ascii")
    if t == "int":
        return v["value"]
    if t == "double":
        d = double_of(v)
        if math.isnan(d):
            return "NaN"
        if d == math.inf:
            return "Infinity"
        if d == -math.inf:
            return "-Infinity"
        return JsonNumber(spell_double(d))
    if t == "bool":
        return v["value"]
    if t == "array":
        return [render_v1(i) for i in v["items"]]
    if t == "kvlist":
        entries = sorted(v["entries"], key=lambda e: e["key"].encode("utf-8"))
        return [(e["key"], render_v1(e["value"])) for e in entries]
    raise ValueError(t)


def compact_json(x):
    """Compact JSON text, spelling doubles with spell_double."""
    if isinstance(x, JsonNumber):
        return x.text
    if isinstance(x, list) and all(isinstance(e, tuple) for e in x) and x:
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + compact_json(v) for k, v in x) + "}"
    if isinstance(x, list):
        return "[" + ",".join(compact_json(e) for e in x) + "]"
    return json.dumps(x, ensure_ascii=False)


def map_value(v):
    """Attribute-map cell and log body: raw string, SQL null, or compact JSON."""
    if v["type"] == "null":
        return None
    if v["type"] == "str":
        return v["value"]
    return compact_json(render_v1(v))


render_cases = [
    ("str_raw", S("hello")),
    ("str_with_quote_is_raw", S('a"b')),
    ("null_is_sql_null", NULL),
    ("int_42", I(42)),
    ("int_min", I(-2**63)),
    ("double_finite", D(1.5)),
    ("double_integral_keeps_fraction", D(3.0)),
    # Finite double layout: fixed notation for decimal exponents -5..=15,
    # scientific outside it with an explicit sign and no zero padding.
    ("double_large_scientific", D(1e16)),
    ("double_large_negative_scientific", D(-1e16)),
    ("double_large_fixed_limit", D(1e15)),
    ("double_many_digits_scientific", D(1.2345678901234568e16)),
    ("double_max", D(1.7976931348623157e308)),
    ("double_small_scientific", D(1e-7)),
    ("double_small_digits_scientific", D(1.23e-6)),
    ("double_small_fixed_limit", D(1e-5)),
    ("double_small_digits_fixed", D(1.23e-5)),
    ("double_min_subnormal", D(5e-324)),
    ("double_neg_zero", DB(0x8000000000000000)),
    ("double_nan_quiet", DB(0x7FF8000000000000)),
    ("double_nan_payload", DB(0x7FF8000000000001)),
    ("double_pos_inf", DB(0x7FF0000000000000)),
    ("double_neg_inf", DB(0xFFF0000000000000)),
    ("bool_true", BOOL(True)),
    ("bytes_empty", B("")),
    ("bytes_one_padded_twice", B("ab")),
    ("bytes_two_padded_once", B("ab12")),
    ("bytes_three_unpadded", B("00ff10")),
    ("bytes_standard_alphabet", B("fbff")),
    ("array_nested_spellings", ARR(DB(0x7FF0000000000000), DB(0xFFF0000000000000), DB(0x7FF8000000000000),
                                   B("ff"), NULL, S("x\ny"), I(1))),
    ("kvlist_sorted_keys", KV(kv("b", B("0102")), kv("a", KV(kv("z", BOOL(False)))), kv("\u00e9", S("\u4e2d")))),
]
render_vectors = [{"name": n, "value": v, "map_value": map_value(v)} for n, v in render_cases]

# CBOR decoding of the OTAP `ser` column (FORMAT.md section 1, "CBOR
# decoding"): a decoder written from those rules, not a CBOR library, so that
# every mapping and refusal is pinned by this file.
I64_MAX = 2**63 - 1


class Refused(Exception):
    """The cell is refused; `kind` is "invalid" or "too_deep"."""

    def __init__(self, kind):
        super().__init__(kind)
        self.kind = kind


class CborReader:
    def __init__(self, data, max_depth):
        self.data, self.pos, self.max_depth = data, 0, max_depth

    def byte(self):
        if self.pos >= len(self.data):
            raise Refused("invalid")
        self.pos += 1
        return self.data[self.pos - 1]

    def take(self, n):
        if self.pos + n > len(self.data):
            raise Refused("invalid")
        self.pos += n
        return self.data[self.pos - n:self.pos]

    def head(self):
        """(major type, additional info, argument or None when indefinite)."""
        b = self.byte()
        major, info = b >> 5, b & 0x1F
        if info < 24:
            return major, info, info
        if info in (24, 25, 26, 27):
            return major, info, int.from_bytes(self.take(1 << (info - 24)), "big")
        if info == 31:
            return major, info, None
        raise Refused("invalid")

    def string(self, major, arg):
        """Bytes of a definite or indefinite byte or text string."""
        if arg is not None:
            return self.checked(major, self.take(arg))
        out = b""
        while True:
            if self.data[self.pos:self.pos + 1] == b"\xff":
                self.pos += 1
                return out
            m, _, n = self.head()
            if m != major:
                raise Refused("invalid")
            # A nested indefinite chunk is flattened into the outer string.
            out += self.string(major, n)

    @staticmethod
    def checked(major, raw):
        if major == 3:
            try:
                raw.decode("utf-8")
            except UnicodeDecodeError:
                raise Refused("invalid")
        return raw

    def items(self, arg, read):
        """Read a definite count, or items up to a break."""
        if arg is not None:
            return [read() for _ in range(arg)]
        out = []
        while self.data[self.pos:self.pos + 1] != b"\xff":
            out.append(read())
        self.pos += 1
        return out

    def key(self):
        major, info, arg = self.head()
        if major == 3:
            return self.string(3, arg).decode("utf-8")
        if major == 7 and info in (22, 23):
            return ""  # null and undefined keys are the empty key
        raise Refused("invalid")

    def item(self, depth_left):
        major, info, arg = self.head()
        if major == 0:
            if arg > I64_MAX:
                raise Refused("invalid")
            return I(arg)
        if major == 1:
            if arg > I64_MAX:
                raise Refused("invalid")
            return I(-1 - arg)
        if major == 2:
            return B(self.string(2, arg).hex())
        if major == 3:
            return S(self.string(3, arg).decode("utf-8"))
        if major in (4, 5):
            if depth_left == 0:
                raise Refused("too_deep")
            if major == 4:
                return ARR(*self.items(arg, lambda: self.item(depth_left - 1)))
            entries = self.items(arg, lambda: kv(self.key(), self.item(depth_left - 1)))
            keys = [e["key"].encode("utf-8") for e in entries]
            if len(set(keys)) != len(keys):
                raise Refused("invalid")
            return KV(*sorted(entries, key=lambda e: e["key"].encode("utf-8")))
        if major == 6:
            m, _, n = self.head()
            if arg not in (2, 3) or m != 2 or n is None or n > 16:
                raise Refused("invalid")
            magnitude = int.from_bytes(self.take(n), "big")
            if magnitude > I64_MAX:
                raise Refused("invalid")
            return I(magnitude if arg == 2 else -1 - magnitude)
        # Major type 7.
        if info == 20:
            return BOOL(False)
        if info == 21:
            return BOOL(True)
        if info in (22, 23):
            return NULL
        if info in (25, 26, 27):
            fmt = {25: ">e", 26: ">f", 27: ">d"}[info]
            raw = arg.to_bytes(1 << (info - 24), "big")
            # Widened exactly to a binary64 double.
            return DB(struct.unpack(">Q", struct.pack(">d", struct.unpack(fmt, raw)[0]))[0])
        raise Refused("invalid")


def decode_cell(hexs, max_depth):
    # Only the first item is read; bytes after it are ignored.
    return CborReader(bytes.fromhex(hexs), max_depth).item(max_depth)


CBOR_DEPTH = 4
cbor_cases = [
    ("uint", "182a"),
    ("uint_above_int64", "1b8000000000000000"),
    ("negint", "3863"),
    ("negint_min", "3b7fffffffffffffff"),
    ("negint_below_int64", "3b8000000000000000"),
    ("float16_one", "f93c00"),
    ("float16_neg_zero", "f98000"),
    ("float16_nan", "f97e00"),
    ("float16_inf", "f97c00"),
    ("float32", "fa3fc00000"),
    ("float64", "fb3ff8000000000000"),
    ("false", "f4"),
    ("true", "f5"),
    ("null", "f6"),
    ("undefined_is_null", "f7"),
    ("simple_other", "f0"),
    ("break_alone", "ff"),
    ("text", "6461c3a962"),
    ("text_not_utf8", "62c328"),
    ("text_indefinite", "7f6161626263ff"),
    ("text_indefinite_nested_chunk", "7f61617f6162ffff"),
    ("text_indefinite_bytes_chunk", "7f4161ff"),
    ("bytes", "4300ff10"),
    ("bytes_indefinite", "5f4101420203ff"),
    ("array", "8301613af6"),
    ("array_indefinite", "9f0102ff"),
    ("map_sorted", "a2616202616101"),
    ("map_indefinite", "bf616202616101ff"),
    ("map_null_key", "a1f601"),
    ("map_undefined_key", "a1f701"),
    ("map_int_key", "a10102"),
    ("map_duplicate_key", "a2616101616102"),
    ("map_null_and_empty_key", "a2f6016002"),
    ("nested_duplicate_key", "a16178a2616101616102"),
    ("bignum", "c2420100"),
    ("bignum_negative", "c3420100"),
    ("bignum_int64_max", "c2487fffffffffffffff"),
    ("bignum_negative_int64_min", "c3487fffffffffffffff"),
    ("bignum_above_int64", "c2488000000000000000"),
    ("bignum_16_bytes", "c250" + "00" * 15 + "2a"),
    ("bignum_17_bytes", "c251" + "00" * 16 + "2a"),
    ("bignum_indefinite", "c25f4101ff"),
    ("tag_other", "c11a5f5e1000"),
    ("depth_at_limit", "81" * CBOR_DEPTH + "01"),
    ("depth_over_limit", "81" * (CBOR_DEPTH + 1) + "01"),
    ("map_depth_over_limit", "a16161" * (CBOR_DEPTH + 1) + "01"),
    ("trailing_bytes_ignored", "0102"),
    ("truncated", "6361"),
]
cbor_vectors = []
for name, hexs in cbor_cases:
    vector = {"name": name, "cbor_hex": hexs, "max_depth": CBOR_DEPTH}
    try:
        value = decode_cell(hexs, CBOR_DEPTH)
    except Refused as refusal:
        vector["refused"] = refusal.kind
    else:
        # The identity of a log series whose attribute `k` holds the value.
        identity = canonical({**base_logs, "attrs": [kv("k", value)]})
        vector.update(value=value, canonical_hex=identity.hex(),
                      series_id_hex=xxhash.xxh3_128_hexdigest(identity))
    cbor_vectors.append(vector)


# identity_config (FORMAT.md section 5): the identity settings of each signal
# as compact JSON, and its xxh3_64.
def identity_config(signal, series_attributes):
    if signal == "metrics":
        return "{}"
    keys = sorted(set(series_attributes), key=lambda k: k.encode("utf-8"))
    return json.dumps({"series_attributes": keys}, ensure_ascii=False, separators=(",", ":"))


identity_cases = [
    ("logs_default", "logs", []),
    ("logs_sorted_and_deduplicated", "logs", ["user", "b\"q", "user", "a"]),
    ("logs_unicode", "logs", ["\u00e9", "z"]),
    ("logs_control_characters", "logs", ["a\tb", "c\u0001d", "e\\f"]),
    ("metrics", "metrics", []),
]
identity_vectors = []
for name, signal, attrs in identity_cases:
    text = identity_config(signal, attrs)
    identity_vectors.append({
        "name": name, "signal": signal, "series_attributes": attrs,
        "identity_config": text,
        "identity_config_hash": "%016x" % xxhash.xxh3_64_intdigest(text.encode("utf-8")),
    })


def dump(name, doc, ensure_ascii):
    with open(os.path.join(sys.argv[1], name), "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=ensure_ascii, allow_nan=False)
        fh.write("\n")


vectors = []
for name, d in cases:
    b = canonical(d)
    vectors.append({"name": name, "descriptor": d, "canonical_hex": b.hex(),
                    "series_id_hex": xxhash.xxh3_128_hexdigest(b)})
# canonical_v1.json keeps its original non-escaped spelling of the Unicode
# vectors, so regenerating it leaves the file byte-identical.
dump("canonical_v1.json", {"format": "canonical_v1", "vectors": vectors}, ensure_ascii=False)
dump("schema_fingerprint_v1.json",
     {"format": "series-lake-schema/1", "vectors": schema_vectors}, ensure_ascii=True)
dump("render_v1.json", {"format": "render_v1", "vectors": render_vectors}, ensure_ascii=True)
dump("cbor_v1.json", {"format": "cbor_v1", "vectors": cbor_vectors}, ensure_ascii=True)
dump("identity_config_v1.json", {"format": "identity_config_v1", "vectors": identity_vectors},
     ensure_ascii=True)
print(f"wrote {len(vectors)} canonical vectors, {len(schema_vectors)} schema vectors, "
      f"{len(render_vectors)} render vectors, {len(cbor_vectors)} cbor vectors, "
      f"{len(identity_vectors)} identity_config vectors")
