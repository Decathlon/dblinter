import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import EXCLUDED_SCHEMAS_STR

LOGGER = logging.getLogger("dblinter")


def table_with_redundant_index(
    self, db: DatabaseConnection, _, context, table, sarif_document
):
    LOGGER.debug(
        "table_with_redundant_index for %s.%s in db %s", table[0], table[1], db.database
    )
    REDUNDANT_INDEX_IN_TABLE = f"""SELECT unnest(array_agg(indexrelid::regclass))
        FROM    pg_index i,
                pg_class c,
                pg_namespace ns
        WHERE   c.oid = i.indrelid
        AND     ns.oid = c.relnamespace
        AND     ns.nspname||'.'||c.relname = '{table[0]}.{table[1]}' AND ns.nspname NOT IN ('{EXCLUDED_SCHEMAS_STR}')
        GROUP BY indrelid,indkey,ns.nspname,c.relname
        HAVING COUNT(*) > 1;
    """
    uri = f"{db.database}.{table[0]}.{table[1]}"
    index_list = db.query(REDUNDANT_INDEX_IN_TABLE)
    if index_list:
        # We don't use a fstring here because the query is use in a loop and we want to use bind variable mecanism
        INDEX_DEFINITION = """select
            i.relname as index_name,
            array_to_string(array_agg(a.attname), ', ') as column_names
        from
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        where
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            and i.relname like '{}'
        group by
            i.relname
        """
        for elt in index_list:
            idx_name = ""
            try:
                idx_name = elt[0].split(".")[1]
            except IndexError:
                idx_name = elt[0]
            index_definition = db.query(INDEX_DEFINITION.format(idx_name))
            if index_definition:
                for idx in index_definition:
                    message_args = (len(index_list), table[0], table[1], idx[0], idx[1])
                    sarif_document.add_check(
                        self.get_ruleid_from_function_name(), message_args, uri, context
                    )
