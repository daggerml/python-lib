from daggerml.dashboard import cli as dashboard_cli


def test_dashboard_cli_rejects_non_loopback_without_opt_in(capsys):
    result = dashboard_cli.main(["--host", "0.0.0.0", "--no-open"])

    assert result == 2
    assert "--allow-remote" in capsys.readouterr().err


def test_dashboard_cli_validates_port_before_loading_optional_dependencies(capsys):
    result = dashboard_cli.main(["--port", "0", "--no-open"])

    assert result == 2
    assert "--port must be between 1 and 65535" in capsys.readouterr().err


def test_dashboard_cli_loopback_detection():
    assert dashboard_cli._is_loopback("localhost")
    assert dashboard_cli._is_loopback("127.0.0.1")
    assert dashboard_cli._is_loopback("::1")
    assert not dashboard_cli._is_loopback("0.0.0.0")


def test_dashboard_cli_accepts_config_directory_and_rejects_project_directory_args():
    parsed = dashboard_cli._parser().parse_args(["--config-dir", "/tmp/dml-config"])

    assert parsed.config_home == "/tmp/dml-config"
    try:
        dashboard_cli._parser().parse_args(["--project-home", "/tmp/project"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("--project-home must not be accepted by dml-dashboard")
