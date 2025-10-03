import logging

from dblinter.database_connection import DatabaseConnection

LOGGER = logging.getLogger("dblinter")


def password_encryption_is_md5(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("password_encryption_is_md5")
    password_encryption = db.query(
        "SELECT setting FROM pg_catalog.pg_settings WHERE name='password_encryption'"
    )[0][0]

    if password_encryption == "md5":
        message_args = "password_encryption is set to md5, this will prevent upgrade to Postgres 18"
        sarif_document.add_check(
            self.get_ruleid_from_function_name(), message_args, "cluster", context
        )
