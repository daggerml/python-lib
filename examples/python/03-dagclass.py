"""Author a nested funk pipeline with ``@api.dagclass``.

Run this from an initialized project with a configured remote. The dagclass
stages its member funks in dependency order, so methods can call other members
without accepting each nested funk as an explicit argument.
"""

from __future__ import annotations

import argparse

import daggerml as dml
from daggerml.contrib import api


@api.funkify
def parse_numbers(dag, raw):
    print("parse_numbers: parsing inputs")
    return [int(value) for value in raw.value()]


@api.funkify
def normalize(dag, values):
    print("normalize: scaling values")
    values = values.value()
    largest = max(values)
    return [value / largest for value in values]


@api.funkify
def summarize(dag, values):
    print("summarize: aggregating values")
    values = values.value()
    return {"count": len(values), "total": sum(values)}


@api.funkify
def same_as(self, a, b):
    print("same_as: comparing values")
    return a.value() == b.value()


@api.dagclass
class DatasetSummary:
    parse_numbers = parse_numbers
    normalize = normalize
    summarize = summarize
    same_as_fn = same_as

    def same_as(self, a, b):
        print("same_as: comparing values")
        return a.value() == b.value()

    def preprocess(self, raw):
        return self.normalize(self.parse_numbers(raw))  # pyright: ignore[reportCallIssue]

    def main(self, raw):
        if self.same_as_fn.value() != self.same_as.value():  # pyright: ignore[reportAttributeAccessIssue]
            raise ValueError("same_as_fn is different from the method")
        self.intermediate_value = self.preprocess(raw)  # pyright: ignore[reportCallIssue]
        self.put(21, name="i0")
        return self.summarize(self.intermediate_value)  # pyright: ignore[reportCallIssue]


@api.dagclass
class MultiDatasetSummary:
    summarizer = DatasetSummary()

    def main(self, raw_dict):
        return {name: self.summarizer(raw) for name, raw in raw_dict.items()}


def main(dag_name: str) -> None:
    api.run(MultiDatasetSummary(), {"a": ["2", "4", "8"], "b": ["1", "3", "5"]}, name=dag_name)
    print(dml.load(dag_name).result.value())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    main(parser.parse_args().dag_name)
