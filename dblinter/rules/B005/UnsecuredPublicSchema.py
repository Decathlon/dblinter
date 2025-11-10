import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def unsecured_public_schema(self, db: DatabaseConnection, _, context, sarif_document):
    LOGGER.debug("unsecured_public_schema for %s", db.database)
    wh = "[,\\{]+=U?C/"
    UNSECURED_PUBLIC_SCHEMA = f"""select count(1) from pg_namespace where nspname not in ('{EXCLUDED_SCHEMAS_STR}') and nspacl::text ~ E'{wh}' """
    result = db.query(UNSECURED_PUBLIC_SCHEMA)[0][0]
    uri = db.database
    if result != 0:
        sarif_document.add_check(self.get_ruleid_from_function_name(), [], uri, context)
