# from dblinter.function_library import extract_param
import logging

from dblinter.database_connection import DatabaseConnection

LOGGER = logging.getLogger("dblinter")


def schema_with_default_role_not_granted(
    self, db: DatabaseConnection, _, context, schema, sarif_document
):
    LOGGER.debug(
        "schema_with_default_role_not_granted for %s in db %s", schema[0], db.database
    )
    SCHEMA_WITH_ROLE_NOT_GRANTED = f"""SELECT count(*)
    FROM pg_catalog.pg_default_acl d
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = d.defaclnamespace
    WHERE n.nspname='{schema[0]}'"""
    schema_with_default_role = db.query(SCHEMA_WITH_ROLE_NOT_GRANTED)[0][0]
    uri = f"{db.database}.{schema[0]}"
    if schema_with_default_role == 0:
        message_args = (db.database, schema[0])
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, uri, context
        )
