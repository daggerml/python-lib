from __future__ import annotations

from daggerml import Uri
from daggerml.contrib import codecs


def test_literal_codecs_skip_missing_optional_dependencies(monkeypatch):
    monkeypatch.setattr(codecs, "_import_optional", lambda module_name: None)

    assert codecs.literal_codecs() == []


def test_literal_codecs_include_pandas_when_installed(monkeypatch):
    class FakeStore:
        def put(self, data=None, filepath=None, *, suffix=""):
            assert filepath is None
            assert data == b"pandas-parquet"
            assert suffix == ".parquet"
            return Uri("s3://bucket/pandas.parquet")

    class FakePandasFrame:
        def to_parquet(self, buf):
            buf.write(b"pandas-parquet")

    class FakePandasModule:
        DataFrame = FakePandasFrame

    monkeypatch.setattr(codecs, "S3Store", FakeStore)
    monkeypatch.setattr(
        codecs,
        "_import_optional",
        lambda module_name: FakePandasModule if module_name == "pandas" else None,
    )

    loaded = codecs.literal_codecs()

    assert len(loaded) == 1
    assert loaded[0].can_encode(FakePandasFrame()) is True
    assert loaded[0].encode(FakePandasFrame(), ctx=None) == Uri("s3://bucket/pandas.parquet")


def test_literal_codecs_include_polars_when_installed(monkeypatch):
    class FakeStore:
        def put(self, data=None, filepath=None, *, suffix=""):
            assert filepath is None
            assert data == b"polars-parquet"
            assert suffix == ".parquet"
            return Uri("s3://bucket/polars.parquet")

    class FakePolarsFrame:
        def write_parquet(self, buf):
            buf.write(b"polars-parquet")

    class FakePolarsModule:
        DataFrame = FakePolarsFrame

    monkeypatch.setattr(codecs, "S3Store", FakeStore)
    monkeypatch.setattr(
        codecs,
        "_import_optional",
        lambda module_name: FakePolarsModule if module_name == "polars" else None,
    )

    loaded = codecs.literal_codecs()

    assert len(loaded) == 1
    assert loaded[0].can_encode(FakePolarsFrame()) is True
    assert loaded[0].encode(FakePolarsFrame(), ctx=None) == Uri("s3://bucket/polars.parquet")


def test_literal_codecs_include_all_installed_dataframe_backends(monkeypatch):
    class FakePandasFrame:
        def to_parquet(self, buf):
            buf.write(b"pandas-parquet")

    class FakePolarsFrame:
        def write_parquet(self, buf):
            buf.write(b"polars-parquet")

    class FakePandasModule:
        DataFrame = FakePandasFrame

    class FakePolarsModule:
        DataFrame = FakePolarsFrame

    modules = {
        "pandas": FakePandasModule,
        "polars": FakePolarsModule,
    }
    monkeypatch.setattr(codecs, "_import_optional", lambda module_name: modules.get(module_name))

    loaded = codecs.literal_codecs()

    assert [codec.__class__.__name__ for codec in loaded] == ["PandasDataFrameCodec", "PolarsDataFrameCodec"]
