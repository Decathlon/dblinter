import logging

from dblinter.database_connection import DatabaseConnection
from dblinter.function_library import extract_param

LOGGER = logging.getLogger("dblinter")


def how_many_redundant_index(
    self, db: DatabaseConnection, param, context, sarif_document
):
    """The number of redundant index."""
    LOGGER.debug("how_many_redundant_index for %s", db.database)
    REDUNDANT_INDEX = """SELECT count(*)
        FROM    pg_index i,
                pg_class c,
                pg_opclass o,
                pg_am a,
                pg_attribute att,
                pg_namespace ns
        WHERE   o.oid = ALL (indclass)
        AND     att.attnum = ANY(i.indkey)
        AND     a.oid = o.opcmethod
        AND     att.attrelid = c.oid
        AND     c.oid = i.indrelid
        AND     ns.oid = c.relnamespace
        AND     ns.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
        GROUP BY ns.nspname,c.relname,
                att.attname,
                indclass,
                amname, indkey
        HAVING count(*) > 1 """
    NB_INDEX = """SELECT count(*) FROM pg_indexes
        WHERE
        schemaname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')"""
    total_number_of_index = db.query(NB_INDEX)[0][0]
    try:
        number_of_redundant_index = db.query(REDUNDANT_INDEX)[0][0]
    except IndexError:
        number_of_redundant_index = 0
    warning = int(extract_param(param, "warning").split("%")[0])
    uri = db.database
    try:
        if int(number_of_redundant_index / total_number_of_index * 100) > warning:
            message_args = (number_of_redundant_index, warning)
            sarif_document.add_check(
                self.get_ruleid_from_function_name(), message_args, uri, context
            )
    except ZeroDivisionError:
        pass
