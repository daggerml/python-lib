from __future__ import annotations

from importlib.metadata import version

import pytest

from daggerml._core import Dml
from daggerml._core.cli import MethodCLI, __version__


def test_root_version_flag_prints_version_and_exits(capsys) -> None:
    assert __version__ == version("daggerml")
    cli = MethodCLI(Dml, prog="dml")

    with pytest.raises(SystemExit) as excinfo:
        cli.parser.parse_args(["--version"])

    assert excinfo.value.code == 0
    assert capsys.readouterr().out == f"dml, version {__version__}\n"
