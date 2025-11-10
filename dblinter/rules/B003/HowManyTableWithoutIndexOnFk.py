import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR, extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_table_without_index_on_fk(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("how_many_table_without_index_on_fk for %s", db.database)
    NB_TABLE_WITH_FK_UNINDEXED = f"""
        with cte as (
        select
        distinct tc.conrelid::regclass
        from pg_catalog.pg_constraint tc
        cross join lateral unnest(tc.conkey) with ordinality as tx(attnum, n)
        join pg_catalog.pg_attribute ta on ta.attnum = tx.attnum and ta.attrelid = tc.conrelid
        inner join pg_class c on c.oid=tc.conrelid
        inner join pg_namespace ns on ns.oid = c.relnamespace
        where ns.nspname not in ('{EXCLUDED_SCHEMAS_STR}') AND not exists (
            select 1 from pg_catalog.pg_index i
            where
            i.indrelid = tc.conrelid and
            (i.indkey::smallint[])[0:cardinality(tc.conkey)-1] @> tc.conkey) and
            tc.contype = 'f'
            group by
            tc.conrelid,
            tc.conname,
            tc.confrelid)
            select count(*) from cte"""
    # Query to get the list of tables without index on foreign keys, including fk name
    TABLES_WITH_FK_UNINDEXED_LIST = f"""
        with cte as (
        select
        distinct pg_get_userbyid(ns.nspowner)||'.'||tc.conrelid::regclass||'.'||tc.conname as table_name
        from pg_catalog.pg_constraint tc
        cross join lateral unnest(tc.conkey) with ordinality as tx(attnum, n)
        join pg_catalog.pg_attribute ta on ta.attnum = tx.attnum and ta.attrelid = tc.conrelid
        inner join pg_class c on c.oid=tc.conrelid
        inner join pg_namespace ns on ns.oid = c.relnamespace
        where ns.nspname not in ('{EXCLUDED_SCHEMAS_STR}') AND not exists (
            select 1 from pg_catalog.pg_index i
            where
            i.indrelid = tc.conrelid and
            (i.indkey::smallint[])[0:cardinality(tc.conkey)-1] @> tc.conkey) and
            tc.contype = 'f'
            group by
            tc.conrelid,
            tc.conname,
            tc.confrelid,
            ns.nspowner,
            ns.nspname,
            c.relname
        )
        select table_name from cte
    """

    NB_TABLE_TABLE = f"""SELECT count(*)
        FROM pg_catalog.pg_tables pt
        WHERE schemaname NOT IN ('{EXCLUDED_SCHEMAS_STR}')"""
    total_number_of_table = db.query(NB_TABLE_TABLE)[0][0]
    number_of_table_with_fk_unindexed = db.query(NB_TABLE_WITH_FK_UNINDEXED)[0][0]
    # Build a comma-separated string of table_name (already includes fk_name if query is correct)
    tables_with_fk_unindexed_rows = db.query(TABLES_WITH_FK_UNINDEXED_LIST)
    tables_with_fk_unindexed_str = ";".join(
        f"{row[0]}" for row in tables_with_fk_unindexed_rows
    )
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    try:
        if (
            int(number_of_table_with_fk_unindexed / total_number_of_table * 100)
            >= warning
        ):
            message_args = (
                number_of_table_with_fk_unindexed,
                warning,
                tables_with_fk_unindexed_str,
            )
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
