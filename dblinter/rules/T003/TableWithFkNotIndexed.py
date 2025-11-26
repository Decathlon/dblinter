import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def table_without_index_on_fk(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_without_index_on_fk for %s.%s in db %s", table[0], table[1], db.database
    )
    NB_TABLE_WITHOUT_INDEX_ON_FK = """ select
                tc.conrelid::regclass,
                'create index on ' ||
                tc.conrelid::regclass ||
                '(' ||
                string_agg(ta.attname, ', ' order by tx.n) ||
                ')' as create_index
            from pg_catalog.pg_constraint tc
            cross join lateral unnest(tc.conkey) with ordinality as tx(attnum, n)
            join pg_catalog.pg_attribute ta on ta.attnum = tx.attnum and ta.attrelid = tc.conrelid
            inner join pg_class c on c.oid=tc.conrelid
            inner join pg_namespace ns on ns.oid = c.relnamespace
            where ns.nspname NOT IN ('{}') AND ns.nspname='{}' and c.relname='{}' and not exists (
                select 1 from pg_catalog.pg_index i
                where
                i.indrelid = tc.conrelid and
                (i.indkey::smallint[])[0:cardinality(tc.conkey)-1] @> tc.conkey) and
                tc.contype = 'f'
                group by
                tc.conrelid,
                tc.conname,
                tc.confrelid"""
    index_count = db.query(
        NB_TABLE_WITHOUT_INDEX_ON_FK.format(EXCLUDED_SCHEMAS_STR, table[0], table[1])
    )
    uri = f"{db.database}.{table[0]}.{table[1]}"
    if index_count:
        for elt in index_count:
            message_args = (db.database, table[0], table[1], elt[1])
            # message = "unindexed fk {}.{}.{} ddl:{}".format(*message_args)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
