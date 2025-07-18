import logging

from typer.testing import CliRunner

from dblinter.__main__ import app

runner = CliRunner()


def test_main_invocation():
    result = runner.invoke(app, ["--host", "127.0.0.1"])
    assert result.exit_code == 2
    assert "Missing option" in result.stderr


def test_main_logger(caplog):
    caplog.set_level(logging.WARNING)
    LOGGER = logging.getLogger("dblinter")
    LOGGER.warning("Error")
    assert LOGGER.hasHandlers
    assert LOGGER.getEffectiveLevel() == logging.WARN
    assert "Error" in caplog.text
