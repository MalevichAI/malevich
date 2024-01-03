from typer.testing import CliRunner
from malevich.cli import app


def test_help():
    runner = CliRunner()
    result = runner.invoke(app, ['--help'])
    assert result.exit_code == 0
    assert len(result.output) > 0

def test_failing_assert():
    assert 3 == 4
