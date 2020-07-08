import fdb
import psycopg2

class conexaoFirebird:

    def __init__(self):
        self.con=None

    def __enter__(self):
        self.con = fdb.connect(dsn='xxx',
                               user='Xxx',
                               password='Xxx')
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
        self._db_connection = psycopg2.connect(host="xxx",
                                    database="xxx",
                                    user="xxx",
                                    password="xxx")
        self._db_cur = self._db_connection.cursor()

    def __enter__(self):
        return self

    def query(self, query, params):
        return self._db_cur.execute(query, params)


    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db_connection.commit()
        self._db_connection.close()

# with conexaoPostgresRDS() as conexao:
#     with conexao.con.cursor() as curs:
#         sql = "SELECT * FROM produtos_fornecedor"
#         curs.execute(sql)
#         rows = curs.fetchall()
# print(rows)

# with conexaoPostgresRDS() as conexao:
#     with conexao.con.cursor() as curs:
#         sql = "SELECT * FROM produtos_fornecedor"
#         curs.execute(sql)
#         rows = curs.fetchone()
#         curs.close()
# print(rows)


