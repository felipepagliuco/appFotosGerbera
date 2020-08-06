import fdb
import psycopg2

from constantes import *
class conexaoFirebird:

    def __init__(self):
        self.con=None

    def __enter__(self):
        self.con = fdb.connect(dsn = DSN_FIREBIRD,
                               user = USER_FIREBIRD,
                               password = PASSWORD_FIREBIRD,
                               charset = CHARSET_FIREBIRD)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.con.close()

    def executeSQL(self,sql):
        try:
            self.cur = self.con.cursor()
            return self.cur.execute(sql)
        finally:
            self.con.cursor().close()

    def retorna_bd(self):
        print(self.con.database_name)

class conexaoPostgresRDS(object):

    def __init__(self):
        self._db_connection = psycopg2.connect(host = HOST_POSTGRES,
                                    database = DATABASE_POSTGRES,
                                    user = USER_POSTGRES,
                                    password = PASSWORD_POSTGRES)
        self.db_cur = self._db_connection.cursor()

    def __enter__(self):
        return self

    def query(self, query,params):
        return self.db_cur.execute(query, params)

    def retorna_todos_registros(self):
        return self.db_cur.fetchall()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db_connection.commit()
        self._db_connection.close()

