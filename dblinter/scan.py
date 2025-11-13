import json
import logging
import os
import sys

import psycopg2
from google.cloud.storage import Client
from pydantic_yaml import parse_yaml_raw_as
from rich import print as rprint

from dblinter.configuration import Configuration
from dblinter.configuration_model import ConfigurationModel
from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import FunctionLibrary
from dblinter.sarif_document import SarifDocument
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

DEFAULT_CONFIG_FILE_NAME = "default_config.yaml"
PATH = "dblinter"
LOGGER = logging.getLogger("dblinter")
RICH_CHECK_COLOR = "bright_blue"


def perform_cluster_check(function_library, db, config_file, sarif_document):
    """Launch cluster checks according to the config file

    Args:
        function_library (FunctionLibrary): Store the rules available
        db (DatabaseConnection): Database connection
        config_file (Configuration): Configuration file
        sarif_document (SarifDocument): Sarif Document
    """
    if sarif_document.quiet_mode is False:
        rprint(
            "[" + RICH_CHECK_COLOR + "]perform_cluster_check[/" + RICH_CHECK_COLOR + "]"
        )
    for check in config_file.cluster_checks.get_enabled_checks():
        function_to_launch = function_library.get_function_by_config_name(check.name)
        if function_to_launch:
            function_to_launch(
                function_library,
                db,
                check.params,
                check.context,
                sarif_document,
            )


def perform_base_check(function_library, db, config_file, sarif_document):
    """Launch database checks according to the config file

    Args:
        function_library (FunctionLibrary): Store the rules available
        db (DatabaseConnection): Database connection
        config_file (Configuration): Configuration file
        sarif_document (SarifDocument): Sarif Document
    """
    # LOGGER.info(rprint(f"[green]perform_base_check for {db.database}[/green]"))
    if sarif_document.quiet_mode is False:
        rprint(
            "["
            + RICH_CHECK_COLOR
            + "]perform_base_check for "
            + db.database
            + "[/"
            + RICH_CHECK_COLOR
            + "]"
        )
    for check in config_file.base_checks.get_enabled_checks():
        function_to_launch = function_library.get_function_by_config_name(check.name)
        if function_to_launch:
            function_to_launch(
                function_library,
                db,
                check.params,
                check.context,
                sarif_document,
            )


def perform_schema_check(function_library, db, config_file, sarif_document, schema=""):
    """Launch schema checks according to the config file

    Args:
        function_library (FunctionLibrary): Store the rules available
        db (DatabaseConnection): Database connection
        config_file (Configuration): Configuration file
        sarif_document (SarifDocument): Sarif Document
    """
    if sarif_document.quiet_mode is False:
        rprint(
            "["
            + RICH_CHECK_COLOR
            + "]perform_schema_check for "
            + db.database
            + "[/"
            + RICH_CHECK_COLOR
            + "]"
        )
    qry = f"""SELECT schema_name
        FROM information_schema.schemata pt
        WHERE schema_name NOT IN ('{EXCLUDED_SCHEMAS_STR}')
        AND (schema_name = '{schema}' or '{schema}'='')
        AND schema_name not in ('aiven_extras')
        """
    schema_list = db.query(qry)
    i = 1
    for oneschema in schema_list:
        # LOGGER.info(rprint(f"[green]perform_schema_check for {str(i)}/{str(len(schema_list))} {oneschema[0]}[/green]"))
        if sarif_document.quiet_mode is False:
            rprint(
                "["
                + RICH_CHECK_COLOR
                + "]perform_schema_check "
                + str(i)
                + "/"
                + str(len(schema_list))
                + " "
                + oneschema[0]
                + "[/"
                + RICH_CHECK_COLOR
                + "]"
            )
        i = i + 1
        for check in config_file.schema_checks.get_enabled_checks():
            function_to_launch = function_library.get_function_by_config_name(
                check.name
            )
            if function_to_launch:
                function_to_launch(
                    function_library,
                    db,
                    check.params,
                    check.context,
                    oneschema,
                    sarif_document,
                )


def perform_table_check(
    function_library, db, config_file, sarif_document, schema="", include="", exclude=""
):
    """Launch table checks according to the config file

    Args:
        function_library (FunctionLibrary): Store the rules available
        db (DatabaseConnection): Database connection
        config_file (Configuration): Configuration file
        sarif_document (SarifDocument): Sarif Document
    """
    # list table from the database
    # LOGGER.info(rprint(f"[green]perform_table_check for {db.database}[/green]"))
    if sarif_document.quiet_mode is False:
        rprint(
            "["
            + RICH_CHECK_COLOR
            + "]perform_table_check for "
            + db.database
            + "[/"
            + RICH_CHECK_COLOR
            + "]"
        )
    qry = f"""SELECT schemaname, tablename
    FROM pg_catalog.pg_tables pt WHERE schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
    AND (schemaname = '{schema}' or '{schema}'='')
    AND ((tablename ilike '{include}' or '{include}'='') AND (tablename not ilike '{exclude}' or '{exclude}'=''))
    AND (tablename not in ('schema_protection'))"""
    table_list = db.query(qry)
    i = 1
    for table in table_list:
        # LOGGER.info(rprint(f"[green]perform_table_check for {str(i)}/{str(len(table_list))} {table[0]}.{table[0]}[/green]"))
        if sarif_document.quiet_mode is False:
            rprint(
                "["
                + RICH_CHECK_COLOR
                + "]perform_table_check "
                + str(i)
                + "/"
                + str(len(table_list))
                + " "
                + table[0]
                + "."
                + table[1]
                + "[/"
                + RICH_CHECK_COLOR
                + "]"
            )
        i = i + 1
        for check in config_file.table_checks.get_enabled_checks():
            function_to_launch = function_library.get_function_by_config_name(
                check.name
            )
            if function_to_launch:
                function_to_launch(
                    function_library,
                    db,
                    check.params,
                    check.context,
                    table,
                    sarif_document,
                )


def save_report_to_disk(output, content, append):
    """Save sarif document to disk

    Args:
        output (str): report file path
        content (str): the sarif report to write in json format
        append (bool): if true append this run to an existing file
    """
    content_json = json.loads(content)

    def write_json(new_data, filename):
        with open(filename, "r+", encoding="utf-8") as file:
            # First we load existing data into a dict.
            file_data = json.load(file)
            # Join new_data with file_data inside emp_details
            file_data["runs"].append(new_data)
            # Sets file's current position at offset.
            file.seek(0)
            # convert back to json.
            json.dump(file_data, file, indent=2)
            file.close()

    if not append or not os.path.exists(output):
        with open(output, "w", encoding="utf-8") as file:
            file.write(content)
    else:
        write_json(content_json["runs"][0], output)


def save_report_to_bucket(output, content, append):
    """Save sarif document to a GCP bucket

    Args:
        output (str): report file path
        content (str): the sarif report to write in json format
        append (bool): if true append this run to an existing file
    """
    content_json = json.loads(content)
    bucket_name = output.split("/")[2]
    object_name = "/".join(output.split("/")[3:])
    bucket = Client().get_bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not append or not blob.exists():
        blob.upload_from_string(content)
    else:
        file_data = json.loads(blob.download_as_string())
        file_data["runs"].append(content_json["runs"][0])
        blob.upload_from_string(json.dumps(file_data, indent=2))


def save_report(output: str, content: str, append: bool):
    """Decide where to save the sarif document according to the file path

    Args:
        output (str): report file path
        content (str): the sarif report to write in json format
        append (bool): if true append this run to an existing file
    """
    if "gs://" in output:
        save_report_to_bucket(output, content, append)
    else:
        save_report_to_disk(output, content, append)


def dblinter(
    user: str,
    password: str,
    host: str,
    port: str,
    dbname: str,
    sslmode="disable",
    describe=None,
    filename=DEFAULT_CONFIG_FILE_NAME,
    append=True,
    quiet=False,
    output=None,
    schema="",
    exclude="",
    include="",
    configuration_path=PATH,
    additional_rule_path=None,
):
    """Scan the objects and perform the check

    Args:
        user (str): The database connection user
        password (str): The database connection password
        host (str): The database connection hostname
        port (str): The database connection port
        dbname (str): The database connection database name
        sslmode (str, optional): The database connection ssl mode. Defaults to "disable".
        describe (str, optional): Describe is added in sarif invocation field. Defaults to None.
        filename (str, optional): Rules configuration file. Defaults to DEFAULT_CONFIG_FILE_NAME.
        append (bool, optional): Sarif report is append to output file. Defaults to True.
        output (str, optional): Write the sarif report to file. Defaults to None.
        schema (str, optional): Run the check only on this schema. Defaults to "" (All schema).
        exclude (str, optional): Filter table to exclude using ILIKE style pattern matching. Defaults to ""(exclude nothing).
        include (str, optional): Filter table to include using ILIKE style pattern matching. Defaults to ""(include all).
        silent (bool, optional): Quiet mode. Defaults to False.
        configuration_path (str, optional): Path to the configuration file. Defaults to PATH.
        additional_rule_path (list, optional): Path to the additional rules file. Defaults to None.

    Raises:
        error: Can't connect to database

    Returns:
        sarif_om: sarif_document
    """

    all_paths_rules = []
    if additional_rule_path is None:
        all_paths_rules = [PATH]
    else:
        all_paths_rules.extend(additional_rule_path)
        # we scan the current module path and the additional rule path
        all_paths_rules.append(os.path.dirname(__file__))

    if filename == "":
        filename = DEFAULT_CONFIG_FILE_NAME
    configuration = Configuration()
    raw_yaml = configuration.read_config_yaml_file(
        path=configuration_path, config_file_name=os.path.basename(filename)
    )
    configuration.config_file = parse_yaml_raw_as(ConfigurationModel, raw_yaml)

    function_library = FunctionLibrary(all_paths_rules)
    sarif_document = SarifDocument(describe)
    # quiet_mode is stored in sarif document because we print the output at the same time we build the doc
    sarif_document.quiet_mode = quiet
    uri = {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "dbname": dbname,
        "sslmode": sslmode,
    }
    # uri = f"postgresql://\"{user}:{password}\"@{host}:{port}/{dbname}?sslmode={sslmode}"
    try:
        db = DatabaseConnection(uri)
    except psycopg2.OperationalError as error:
        rprint("[red]Error connecting to db: [/red]")
        raise error

    perform_cluster_check(
        function_library, db, configuration.config_file, sarif_document
    )

    # list database the provided user can connect to
    database_list = db.query(
        """SELECT datname
        FROM pg_database
        WHERE has_database_privilege(datname,'connect')=TRUE
        and datname not in ('_aiven')
        AND datistemplate=false order by datname"""
    )

    for database_to_connect in database_list or []:
        if database_to_connect[0] == dbname or dbname in (
            "postgres",
            "defaultdb",
        ):
            # Make database check for the specified dbname or all database if connected to postgres or defaultdb database
            try:
                uri = {
                    "user": user,
                    "password": password,
                    "host": host,
                    "port": port,
                    "dbname": dbname,
                    "sslmode": sslmode,
                }
                other_db = DatabaseConnection(uri)
            except psycopg2.OperationalError:
                sys.exit(1)
            perform_base_check(
                function_library, other_db, configuration.config_file, sarif_document
            )
            perform_schema_check(
                function_library,
                other_db,
                configuration.config_file,
                sarif_document,
                schema=schema,
            )
            perform_table_check(
                function_library,
                other_db,
                configuration.config_file,
                sarif_document,
                schema=schema,
                include=include,
                exclude=exclude,
            )
            other_db.close()

    if output is not None:
        save_report(output, sarif_document.json_format(), append)

    return sarif_document
