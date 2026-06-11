import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class FakeExpr:
    def alias(self, *_args, **_kwargs):
        return self

    def over(self, *_args, **_kwargs):
        return self

    def isNotNull(self):
        return self

    def isNull(self):
        return self

    def __and__(self, _other):
        return self

    def __or__(self, _other):
        return self

    def __invert__(self):
        return self

    def cast(self, *_args, **_kwargs):
        return self

    def __gt__(self, _other):
        return self

    def __ge__(self, _other):
        return self

    def __lt__(self, _other):
        return self

    def __le__(self, _other):
        return self

    def __eq__(self, _other):  # type: ignore[override]
        return self

    def __ne__(self, _other):  # type: ignore[override]
        return self


class FakeWhenExpr(FakeExpr):
    def otherwise(self, _value):
        return self


class FakeWindowSpec:
    def orderBy(self, *_args, **_kwargs):
        return self

    def rowsBetween(self, *_args, **_kwargs):
        return self


class FakeWindow:
    unboundedPreceding = object()

    @staticmethod
    def partitionBy(*_args, **_kwargs):
        return FakeWindowSpec()


@dataclass
class StructField:
    name: str
    dataType: object
    nullable: bool = True
    metadata: dict | None = None


class StructType:
    def __init__(self, fields=None):
        self.fields = list(fields or [])

    def __iter__(self):
        return iter(self.fields)


class BooleanType:
    pass


class DecimalType:
    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale


class IntegerType:
    pass


class LongType:
    pass


class StringType:
    pass


class TimestampType:
    pass


class FakeWrite:
    def __init__(self, frame):
        self.frame = frame
        self.format_name = None
        self.mode_name = None
        self.saved_table = None

    def format(self, name):
        self.format_name = name
        return self

    def mode(self, name):
        self.mode_name = name
        return self

    def saveAsTable(self, table):
        self.saved_table = table
        self.frame.saved_table = table


class FakeDataFrame:
    def __init__(self, data=None, schema=None, rows=None):
        self.data = list(data or [])
        self.schema = schema
        self.rows = list(rows or self.data)
        self.write = FakeWrite(self)
        self.temp_view_name = None
        self.saved_table = None
        self.selected_columns = None
        self.with_columns = []
        self.join_calls = []
        self.filter_calls = []
        self.cached = False
        self.unpersisted = False

    def collect(self):
        return self.rows

    def take(self, n):
        return self.rows[:n]

    def createOrReplaceTempView(self, name):
        self.temp_view_name = name

    def select(self, *cols, **_kwargs):
        self.selected_columns = cols
        return self

    def withColumn(self, name, expr):
        self.with_columns.append((name, expr))
        return self

    def join(self, other, on=None, how=None):
        self.join_calls.append((other, on, how))
        return self

    def filter(self, condition):
        self.filter_calls.append(condition)
        return self

    def agg(self, *exprs):
        input_records = len(self.rows)
        valid_records = sum(1 for row in self.rows if row.get("error_reason") is None)
        quarantine_records = sum(1 for row in self.rows if row.get("error_reason") is not None)
        duplicate_records = sum(1 for row in self.rows if row.get("is_duplicate"))
        return FakeDataFrame(
            rows=[
                {
                    "input_records": input_records,
                    "valid_records": valid_records,
                    "quarantine_records": quarantine_records,
                    "duplicate_records": duplicate_records,
                }
            ]
        )

    def cache(self):
        self.cached = True
        return self

    def unpersist(self):
        self.unpersisted = True
        return self


class FakeSparkSession:
    def __init__(self):
        self.conf = types.SimpleNamespace(values={}, set=self._set_conf)
        self.sql_queries = []
        self.created_dataframes = []
        self.sql_results = []

    def _set_conf(self, key, value):
        self.conf.values[key] = value

    def sql(self, query):
        self.sql_queries.append(query)
        if self.sql_results:
            return self.sql_results.pop(0)
        return FakeDataFrame(rows=[])

    def createDataFrame(self, data, schema=None):
        frame = FakeDataFrame(data=data, schema=schema)
        self.created_dataframes.append(frame)
        return frame


def _install_pyspark_stub() -> None:
    pyspark_mod = types.ModuleType("pyspark")
    pyspark_mod.__path__ = []

    sql_mod = types.ModuleType("pyspark.sql")
    sql_mod.__path__ = []

    functions_mod = types.ModuleType("pyspark.sql.functions")
    window_mod = types.ModuleType("pyspark.sql.window")
    types_mod = types.ModuleType("pyspark.sql.types")

    def _expr(*_args, **_kwargs):
        return FakeExpr()

    def _when(*_args, **_kwargs):
        return FakeWhenExpr()

    for name in [
        "expr",
        "coalesce",
        "collect_set",
        "col",
        "lit",
        "max",
        "sum",
        "size",
        "array_contains",
        "count",
    ]:
        setattr(functions_mod, name, _expr)

    functions_mod.when = _when

    types_mod.StructField = StructField
    types_mod.StructType = StructType
    types_mod.BooleanType = BooleanType
    types_mod.DecimalType = DecimalType
    types_mod.IntegerType = IntegerType
    types_mod.LongType = LongType
    types_mod.StringType = StringType
    types_mod.TimestampType = TimestampType

    window_mod.Window = FakeWindow

    fake_session = FakeSparkSession()

    class _SparkSessionClass:
        builder = types.SimpleNamespace(getOrCreate=lambda: fake_session)

        @staticmethod
        def getActiveSession():
            return None

    sql_mod.SparkSession = _SparkSessionClass
    sql_mod.functions = functions_mod
    sql_mod.window = window_mod
    sql_mod.types = types_mod
    pyspark_mod.sql = sql_mod

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = sql_mod
    sys.modules["pyspark.sql.functions"] = functions_mod
    sys.modules["pyspark.sql.window"] = window_mod
    sys.modules["pyspark.sql.types"] = types_mod


def _install_jsonschema_stub() -> None:
    jsonschema_mod = types.ModuleType("jsonschema")

    class Draft7Validator:
        def __init__(self, schema):
            self.schema = schema

        def iter_errors(self, _instance):
            return []

    jsonschema_mod.Draft7Validator = Draft7Validator
    sys.modules["jsonschema"] = jsonschema_mod


def _install_pycountry_stub() -> None:
    pycountry_mod = types.ModuleType("pycountry")
    pycountry_mod.countries = [
        types.SimpleNamespace(alpha_2="US"),
        types.SimpleNamespace(alpha_2="SE"),
        types.SimpleNamespace(alpha_2="DE"),
    ]
    sys.modules["pycountry"] = pycountry_mod


if importlib.util.find_spec("pyspark") is None:
    _install_pyspark_stub()

if importlib.util.find_spec("jsonschema") is None:
    _install_jsonschema_stub()

if importlib.util.find_spec("pycountry") is None:
    _install_pycountry_stub()


@pytest.fixture
def fake_spark_session():
    session = FakeSparkSession()
    return session
