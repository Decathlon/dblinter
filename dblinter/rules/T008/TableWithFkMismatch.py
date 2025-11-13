import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def table_with_fk_type_mixmatch(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_fk_type_mixmatch for %s.%s in db %s",
        table[0],
        table[1],
        db.database,
    )
    TABLE_WITH_FK_MISMATCH = f"""
        select
            conname as constraint_name,
            conrelid::regclass as table_name,
            string_agg(ta.attname,', ') as column_name,
            confrelid::regclass as foreign_table_name,
            string_agg(fa.attname,', ') as foreign_column_name,
            string_agg(ty1.typname,', ') as type_column_name,
            string_agg(ty1.typlen::varchar,', ') as type_column_name_len,
            string_agg(ta.atttypmod::varchar,', ') as len_char_col,
            string_agg(ty2.typname,', ') as type_foreign_column_name,
            string_agg(ty2.typlen::varchar,', ') as type_foreign_column_name_len,
            string_agg(fa.atttypmod::varchar,', ') as len_char_fk
        from
            (
            select
                conname,
                conrelid,
                confrelid,
                unnest(conkey) as conkey,
                unnest(confkey) as confkey
            from
                pg_constraint pc
                join pg_class cls on cls.oid=pc.conrelid
                join pg_catalog.pg_namespace ns on ns.oid=pc.connamespace
            where
                cls.relname = '{table[1]}'
                and ns.nspname='{table[0]}'
                and ns.nspname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
                and contype = 'f'
        ) sub
        join pg_attribute as ta on
            ta.attrelid = conrelid
            and ta.attnum = conkey
        join pg_attribute as fa on
            fa.attrelid = confrelid
            and fa.attnum = confkey
        join pg_type as ty1 on
            ty1.oid = ta.atttypid
            and ta.attnum = conkey
        join pg_type as ty2 on
            ty2.oid = fa.atttypid
            and fa.attnum = confkey
        group by conname,conrelid,confrelid
        having string_agg(ty1.typname,', ')<>string_agg(ty2.typname,', ')
        or
        (string_agg(ty2.typname,', ')<>string_agg(ty1.typname,', ')
        or
        string_agg(fa.atttypmod::varchar,', ') <>string_agg(ta.atttypmod::varchar,', ') )
        """
    table_fk_mismatch = db.query(TABLE_WITH_FK_MISMATCH)
    uri = f"{db.database}.{table[0]}.{table[1]}"
    if table_fk_mismatch:
        for elt in table_fk_mismatch:
            message_args = (
                elt[0],
                uri,
                elt[2],
                elt[5],
                elt[7],
                elt[3],
                elt[4],
                elt[8],
                elt[10],
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
