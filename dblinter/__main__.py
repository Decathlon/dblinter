import logging
from enum import Enum

import typer

from dblinter.scan import dblinter


class SslMode(str, Enum):
    allow = "allow"
    prefer = "prefer"
    require = "require"
    verify_ca = "verify-ca"
    verify_full = "verify-full"
    simple = "simple"
    disable = "disable"


class LogLevels(str, Enum):
    debug = "DEBUG"
    info = "INFO"
    warning = "WARNING"
    error = "ERROR"
    critical = "CRITICAL"


app = typer.Typer()


@app.command()
def main(
    user: str = typer.Option(..., "--user", "-U", envvar="PGUSER", help="pg username"),
    password: str = typer.Option(
        ..., "--password", "-W", envvar="PGPASSWORD", help="pg pwd"
    ),
    host: str = typer.Option(..., "--host", "-h", envvar="PGHOST", help="pg hostname"),
    port: str = typer.Option("5432", "--port", "-p", envvar="PGPORT", help="pg port"),
    dbname: str = typer.Option(
        ..., "--dbname", "-d", envvar="PGDATABASE", help="pg dbname"
    ),
    sslmode: SslMode = typer.Option(
        SslMode.disable, "--sslmode", "-s", envvar="PGSSLMODE", help="pg sslmode"
    ),
    describe: str = typer.Option(
        None, "--describe", "-b", help="describe is added in sarif invocation field"
    ),
    filename: str = typer.Option(
        "",
        "--filename",
        "-f",
        help="rules configuration file",
    ),
    append: bool = typer.Option(
        True,
        "--append",
        "-a",
        envvar="APPEND",
        help="sarif report is append to output file",
    ),
    output: str = typer.Option(
        None, "--output", "-o", envvar="OUTPUT", help="report output file"
    ),
    loglevel: LogLevels = typer.Option(
        LogLevels.warning, "--loglevel", "-l", envvar="LOGLEVEL", help="log level"
    ),
    schema: str = typer.Option(
        "", "--schema", "-n", envvar="SCHEMA", help="tables in schema only filter"
    ),
    exclude: str = typer.Option(
        "", "--exclude", "-x", envvar="EXCLUDE", help="Exclude table filter"
    ),
    include: str = typer.Option(
        "", "--include", "-i", envvar="INCLUDE", help="Include table filter"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", envvar="QUIET", help="Quiet mode"
    ),
):
    logging.basicConfig(level=loglevel.value)
    LOGGER = logging.getLogger("dblinter")
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    LOGGER.addHandler(ch)

    dblinter(
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        sslmode=sslmode.value,
        describe=describe,
        filename=filename,
        append=append,
        quiet=quiet,
        output=output,
        host=host,
        schema=schema,
        exclude=exclude,
        include=include,
    )
    # print(result.json_format())


def cli():
    app()


if __name__ == "__main__":
    app()
