from conexoesBD import conexaoFirebird,conexaoPostgresRDS

class RotinasSincronizacao:

    def __init__(self):
        pass

    def busca_codigo_e_descricao_fornecedores_bd_firebird(self):
        with conexaoFirebird() as conexao_firebird:
            instrucao_sql = ("SELECT CODGRUPO,DESCRICAO FROM GRUPO WHERE DESCRICAO IS NOT NULL ORDER BY CODGRUPO")
            return conexao_firebird.executeSQL(instrucao_sql).fetchall()

    def busca_cod_descricao_fornecedores_bd_firebird(self):
        with conexaoFirebird() as conexao_firebird:
            instrucao_sql = ("SELECT CODSUBGRUPO,DESCRICAO FROM SUBGRUPO "
                             "WHERE DESCRICAO IS NOT NULL ORDER BY CODSUBGRUPO")
            return conexao_firebird.executeSQL(instrucao_sql).fetchall()

    def popula_tabela_fornecedores_do_postgres(self,fornecedores):
        with conexaoPostgresRDS() as conexao :
            sql = u"INSERT INTO public.produtos_fornecedor (data_criacao,data_alteracao,ativo,codigo,nome) VALUES (%s,%s,%s,%s,%s);"
            for key, value in fornecedores.items() :
                parametros = '2020-07-08', '2020-07-08', True, key, value
                conexao.query(sql, parametros)

sinc =  RotinasSincronizacao()
fornecedores = dict(sinc.busca_codigo_e_descricao_fornecedores_bd_firebird())
sinc.popula_tabela_fornecedores_do_postgres(fornecedores)

