import logging

from dblinter.database_connection import DatabaseConnection

LOGGER = logging.getLogger("dblinter")


def table_with_fk_in_other_schema(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_fk_in_other_schema for %s.%s in db %s",
        table[0],
        table[1],
        db.database,
    )
    TABLE_WITH_FK_OUTSIDE = f"""SELECT connamespace::regnamespace   ,conrelid::regclass AS table_name,
            conname AS foreign_key,
            pg_get_constraintdef(oid) as def_constraint
            FROM   pg_constraint
            WHERE  contype = 'f'
            and conrelid::regclass::text = '{table[0]}.{table[1]}'"""
    table_fk_outside = db.query(TABLE_WITH_FK_OUTSIDE)
    uri = f"{db.database}.{table[0]}.{table[1]}"
    if table_fk_outside:
        for elt in table_fk_outside:
            target_schema = elt[3].split("REFERENCES")[1].split(".")[0].lstrip()
            if target_schema != table[0]:
                message_args = (elt[2], elt[1], target_schema)
                sarif_document.add_check(
                    self.get_ruleid_from_function_name(), message_args, uri, context
                )
