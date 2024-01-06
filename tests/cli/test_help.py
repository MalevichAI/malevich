import typer
from malevich_space.cli.cli import app as space_app
from typer.testing import CliRunner

from malevich.cli import app


def _check_help(result):
    """Check that the help message is not empty."""
    # TODO: More specific help checks
    assert result.exit_code == 0
    assert len(result.output) > 0
    assert 'Usage' in result.output
    assert 'Options' in result.output


def _check_help_recursive(typer_: typer.Typer, path: str = ''):
    """Check that the help message is not empty for all commands and subcommands.

    Args:
        typer_ (typer.Typer): The typer instance to check.
        path (str, optional): The path to the current typer instance. Defaults to ''.

    Returns:
        list[str]: The paths to the commands with non-empty help messages.
    """
    if typer_ is space_app:
        # Not testing Malevich Space CLI here
        return []


    runner = CliRunner()
    result = runner.invoke(typer_, ['--help'])

    paths = []

    if result is not None:
        _check_help(result)
        paths.append(path)

    for command in typer_.registered_commands:
        if not command.name:
            # Skip anonymous commands
            continue

        result = runner.invoke(typer_, [command.name, '--help'])
        if result is not None:
            try:
                _check_help(result)
                paths.append(path + ' ' + command.name)
            except AssertionError as e:
                raise AssertionError(f'Error in {path} {command.name}') from e


    for command in typer_.registered_groups:
        if command.typer_instance is not typer_:
            paths_ = _check_help_recursive(
                command.typer_instance, path + ' ' + command.name
            )
            paths.extend(paths_)

    return paths


def test_help():
    checked = _check_help_recursive(app)
    print(*[f'malevich{x} --help' for x in checked], sep='\n')

