# from dblinter.function_library import extract_param
import logging

from dblinter.database_connection import DatabaseConnection

LOGGER = logging.getLogger("dblinter")


def schema_prefixed_or_suffixed_with_envt(
    self, db: DatabaseConnection, _, context, schema, sarif_document
):
    LOGGER.debug(
        "schema_prefixed_or_suffixed_with_envt for %s in db %s", schema[0], db.database
    )
    SCHEMA_WITH_ENVT = f"""SELECT count(*)
        FROM information_schema.schemata
        WHERE schema_name LIKE 'dev_%' OR schema_name LIKE '%_dev'
        OR schema_name LIKE 'prod_%' OR schema_name LIKE '%_prod'
        OR schema_name LIKE 'preprod_%' OR schema_name LIKE '%_preprod'
        OR schema_name LIKE 'staging_%' OR schema_name LIKE '%_staging'
        OR schema_name LIKE 'stg_%' OR schema_name LIKE '%_stg'
        OR schema_name LIKE 'sbox_%' OR schema_name LIKE '%_sbox'
        OR schema_name LIKE 'sandbox_%' OR schema_name LIKE '%_sandbox'"""
    schema_with_prefix = db.query(SCHEMA_WITH_ENVT)[0][0]
    uri = f"{db.database}.{schema[0]}"
    if schema_with_prefix > 0:
        message_args = (db.database, schema[0])
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, uri, context
        )
