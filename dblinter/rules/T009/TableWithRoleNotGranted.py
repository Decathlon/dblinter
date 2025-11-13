import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def table_with_role_not_granted(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_role_not_granted for %s.%s in db %s",
        table[0],
        table[1],
        db.database,
    )

    if table[0] != "public":
        TABLE_WITH_ROLE_NOT_GRANTED = f"""SELECT count(*) FROM information_schema.role_table_grants tg inner join pg_roles pgr on pgr.rolname=tg.grantee WHERE table_schema NOT IN ('{EXCLUDED_SCHEMAS_STR}') and table_schema='{table[0]}' and table_name='{table[1]}' and pgr.rolcanlogin=false"""
        table_with_roles = db.query(TABLE_WITH_ROLE_NOT_GRANTED)[0][0]
        uri = f"{db.database}.{table[0]}.{table[1]}"
        if table_with_roles == 0:
            message_args = (db.database, table[0], table[1])
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
