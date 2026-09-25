"""Search decision-tree regressors for airline arrival delay in minutes.

Run from an initialized DaggerML project with ``remote.root`` configured. The
Docker image must have DaggerML, polars, and scikit-learn installed. The default
input is Vega's 2,000-flight airline-delay sample (``delay`` is the response).
Only the authoring process downloads the input; workers read its persisted URI.
"""

# Dagclass compiles method calls into node calls; sklearn lives in the worker image.
# pyright: reportMissingImports=false, reportCallIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import argparse
from typing import Any
from urllib.request import urlopen

import daggerml as dml
from daggerml import Node, Uri
from daggerml.contrib import api
from daggerml.contrib.s3 import S3Store

FLIGHTS_URL = "https://raw.githubusercontent.com/vega/vega-datasets/main/data/flights-2k.json"


def _read_cut(cut):
    """Read a codec-staged Parquet URI, honoring a local S3-compatible endpoint."""
    import os

    import polars as pl

    options = None
    if endpoint := os.environ.get("AWS_ENDPOINT_URL"):
        options = {"aws_endpoint_url": endpoint, "allow_http": "true", "max_retries": 1}
    return pl.read_parquet(cut.value().uri, storage_options=options)


@api.dagclass
class AirlineDelaySearch:
    image: str
    flags: list[str]

    @api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("flags"))
    @api.funkify
    def prepare(self, source):
        import polars as pl

        from daggerml.contrib.s3 import S3Store

        store = S3Store()
        flights = pl.read_json(store.get(source.value()))
        flights = (
            flights.select(
                pl.col("distance").cast(pl.Float64),
                pl.col("date").str.to_datetime("%Y/%m/%d %H:%M").dt.hour().cast(pl.Float64).alias("hour"),
                pl.col("date").str.to_datetime("%Y/%m/%d %H:%M").dt.weekday().cast(pl.Float64).alias("weekday"),
                pl.col("delay").cast(pl.Float64),
            )
            .drop_nulls()
            .sample(fraction=1.0, shuffle=True, seed=42)
        )
        if flights.height < 8:
            raise ValueError("at least eight complete flights are required")
        boundary = int(flights.height * 0.75)
        return {
            "insample": flights.head(boundary),
            "out_of_sample": flights.tail(flights.height - boundary),
        }

    @api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("flags"))
    @api.funkify(extra_objs=(_read_cut,))
    def train(self, cut, params):
        import pickle

        from sklearn.tree import DecisionTreeRegressor

        from daggerml.contrib.s3 import S3Store

        store = S3Store()
        flights = _read_cut(cut)
        tree = DecisionTreeRegressor(random_state=42, **params.value())
        tree.fit(flights.drop("delay").to_numpy(), flights["delay"].to_numpy())
        return store.put(data=pickle.dumps(tree), suffix=".pkl")

    @api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("flags"))
    @api.funkify(extra_objs=(_read_cut,))
    def predict(self, model_uri, cut):
        import pickle

        from daggerml.contrib.s3 import S3Store

        store = S3Store()
        # Unpickle only models produced by the trusted train step above.
        tree = pickle.loads(store.get(model_uri.value()))
        flights = _read_cut(cut)
        return [float(value) for value in tree.predict(flights.drop("delay").to_numpy())]

    @api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("flags"))
    @api.funkify(extra_objs=(_read_cut,))
    def metrics(self, predictions, cut):
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        flights = _read_cut(cut)
        actual = flights["delay"].to_list()
        predicted = predictions.value()
        return {
            "r2": float(r2_score(actual, predicted)),
            "mse": float(mean_squared_error(actual, predicted)),
            "mae": float(mean_absolute_error(actual, predicted)),
        }

    def pipeline(self, params, cuts):
        model = self.train(cuts["insample"], params)
        scores = {}
        for cut in ("insample", "out_of_sample"):
            predicted = self.predict(model, cuts[cut])
            scores[cut] = self.metrics(predicted, cuts[cut])
        self.scores = scores
        return scores["out_of_sample"]["r2"]

    def search(self, cuts):
        trials = []
        paramset = [{"max_depth": depth, "min_samples_leaf": leaf} for depth in (3, 7) for leaf in (5, 20)]
        for index, params in enumerate(paramset):
            param_node = self.put(params, name=f"params-{index}")
            objective = self.pipeline(param_node, cuts)
            trials.append({"params": param_node, "objective": objective})
        self.trials = trials
        return max(trials, key=lambda trial: trial["objective"].value())

    def main(self, source):
        cuts = self.prepare(source)
        best = self.search(cuts)
        print("Best tree params:", best["params"].value())
        print("Out-of-sample R²:", best["objective"].value())
        return best


def run(
    image: str, source: Uri | Node | None = None, *, flags: list[str] | None = None, name: str = "airline-delay-search"
) -> Any:
    """Stage a flight dataset, run the dagclass, and return its committed best trial."""
    if source is None:
        with urlopen(FLIGHTS_URL, timeout=30) as response:
            flights = response.read()
        with dml.new(f"{name}-input", message="stage Vega airline delays") as dag:
            source = dag.put(S3Store().put(data=flights, suffix=".json"), name="flights")
            dag.commit(source)
    api.run(AirlineDelaySearch(image=image, flags=flags or []), source, name=name)
    return dml.load(name).result.value()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", help="Docker image with daggerml, polars and scikit-learn")
    run(parser.parse_args().image)
