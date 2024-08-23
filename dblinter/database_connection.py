import logging

from psycopg2 import Error, InterfaceError, connect

LOGGER = logging.getLogger("dblinter")


def log_psycopg2_exception(err):
    """Format psycopg exception

    Args:
        err (psycopg error): psycopg exception
    """

    st = str(err)
    LOGGER.error("\npsycopg2 ERROR: %s", st)
    st = str(err.pgcode)
    LOGGER.error("pgcode: %s", st)


class DatabaseConnection:
    """Manage database connection

    Attributes:
        is_superuser(bool) : is the connection is initiated by a superuser
        database(str) : the database the user is connected at
        conn(Connection) : the connection open on the database

    Methods:
        query(query) : Execute the query on the database
        close() : Close the connection
    """

    is_superuser: bool
    database: str

    def __init__(self, uri: str):
        """Database connection constructor

        Args:
            uri (str): Database connection uri in the form:
            "postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}"

        Raises:
            Exception: cannot init
        """
        try:
            self.conn = connect(uri)
            self.database = self.conn.get_dsn_parameters()["dbname"]
        except Exception as err:
            log_psycopg2_exception(err)
            self.conn = None
            raise

    def query(self, query: str):
        """Return the result of a query on the database

        Args:
            query (str): Query to execute

        Raises:
            InterfaceError: error connecting to the database
        """
        resultset = []
        try:
            if self.conn:
                self.cur = self.conn.cursor()
                self.cur.execute(query)
            if self.cur.pgresult_ptr is not None:
                resultset = self.cur.fetchall()
            self.cur.close()
            return resultset
        except (Error, InterfaceError) as err:
            log_psycopg2_exception(err)
            print(query)
            raise

    def close(self):
        if self.conn:
            self.conn.close()
