import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_table_without_index_on_fk(
    self, db: DatabaseConnection, param, context, sarif_document
):
    LOGGER.debug("how_many_table_without_index_on_fk for %s", db.database)
    NB_TABLE_WITH_FK_UNINDEXED = """
        with cte as (
        select
        distinct tc.conrelid::regclass
        from pg_catalog.pg_constraint tc
        cross join lateral unnest(tc.conkey) with ordinality as tx(attnum, n)
        join pg_catalog.pg_attribute ta on ta.attnum = tx.attnum and ta.attrelid = tc.conrelid
        inner join pg_class c on c.oid=tc.conrelid
        inner join pg_namespace ns on ns.oid = c.relnamespace
        where not exists (
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

    NB_TABLE_TABLE = """SELECT count(*)
        FROM pg_catalog.pg_tables pt
        WHERE schemaname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')"""
    total_number_of_table = db.query(NB_TABLE_TABLE)[0][0]
    number_of_table_with_fk_unindexed = db.query(NB_TABLE_WITH_FK_UNINDEXED)[0][0]
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    try:
        if (
            int(number_of_table_with_fk_unindexed / total_number_of_table * 100)
            >= warning
        ):
            message_args = (number_of_table_with_fk_unindexed, warning)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
