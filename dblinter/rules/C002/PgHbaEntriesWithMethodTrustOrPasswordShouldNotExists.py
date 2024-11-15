import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import extract_param

LOGGER = logging.getLogger("dblinter")


def pg_hba_entries_with_trust_or_password_method(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("pg_hba_entries_with_trust_or_password_method")
    trust_password = db.query(
        "SELECT count(*) FROM pg_catalog.pg_hba_file_rules WHERE auth_method in ('trust','password')"
    )[0][0]

    warning = int(extract_param(param, "warning"))

    if trust_password >= warning:
        message_args = (trust_password, warning)
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, db.database, context
        )
