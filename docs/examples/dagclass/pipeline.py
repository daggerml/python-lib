"""A file-backed dagclass example for the documentation build."""

from daggerml import Dml, use_default_dml
from daggerml.contrib import api


@api.dagclass
class AddOffset:
    offset: int

    def main(self, value):
        return self.offset.value() + value.value()


def run():
    runtime = Dml()
    with use_default_dml(runtime):
        api.run(AddOffset(offset=2), 40, name="docs-dagclass")
        from daggerml import load

        assert load("docs-dagclass").result.value() == 42


if __name__ == "__main__":
    run()
