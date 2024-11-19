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
    ENVT = ["dev", "prod", "preprod", "staging", "stg", "sbox", "sandbox"]
    schema_name = schema[0]
    for env in ENVT:
        if schema_name.startswith(env) or schema_name.endswith(env):
            uri = f"{db.database}.{schema_name}"
            message_args = (env, schema_name)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
