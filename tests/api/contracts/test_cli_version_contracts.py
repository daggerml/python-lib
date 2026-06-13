from __future__ import annotations

import pytest

from daggerml._cli import MethodCLI, __version__
from daggerml._core import Dml


def test_cli_version_001__root_version_flag_prints_version_and_exits(capsys) -> None:
    cli = MethodCLI(Dml, prog="dml")

    with pytest.raises(SystemExit) as excinfo:
        cli.parser.parse_args(["--version"])

    assert excinfo.value.code == 0
    assert capsys.readouterr().out == f"dml, version {__version__}\n"
