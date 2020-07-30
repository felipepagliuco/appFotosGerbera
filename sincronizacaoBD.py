from conexoesBD import conexaoFirebird, conexaoPostgresRDS


class SincronizaFornecedores :

    def __init__(self) :
        pass

    def limpa_tabela_temporaria_fornecedores(self) :
        with conexaoPostgresRDS() as conexao :
            sql = u"DELETE FROM public.temp_fornecedor"
            parametros = ''
            conexao.query(sql, parametros)

    def busca_codigo_e_descricao_fornecedores_bd_firebird(self) :
        with conexaoFirebird() as conexao_firebird :
            instrucao_sql = ("SELECT CODGRUPO,DESCRICAO FROM GRUPO WHERE DESCRICAO IS NOT NULL ORDER BY CODGRUPO")
            return conexao_firebird.executeSQL(instrucao_sql).fetchall()

    def carrega_fornecedores_para_a_tabela_temporaria(self) :
        fornecedores = dict(self.busca_codigo_e_descricao_fornecedores_bd_firebird())
        with conexaoPostgresRDS() as conexao :
            sql = u"INSERT INTO public.temp_fornecedor(codigo, nome) VALUES (%s,%s);"
            for codigo, nome in fornecedores.items() :
                parametros = codigo, nome
                conexao.query(sql, parametros)

    def codigo_fornecedor_nao_cadastrado(self, cod_fornecedor) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = "SELECT * FROM produtos_fornecedor WHERE codigo = '%s';"
            parametro = cod_fornecedor,
            conexao.query(instrucao_sql, parametro)
            return not conexao.retorna_todos_registros()

    def insere_fornecedor(self, codigo, nome) :
        with conexaoPostgresRDS() as conexao :
            sql = u"INSERT INTO public.produtos_fornecedor (data_criacao, data_alteracao, ativo,codigo,nome) VALUES (%s,%s,%s,%s,%s);"
            parametros = '2020-07-13', '2020-07-13', True, codigo, nome
            conexao.query(sql, parametros)

    def registros_de_fornecedores_diferentes_tabela_temporaria_e_fornecedor(self) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = ("SELECT\n" +
                             "TEMPFORN.CODIGO,\n" +
                             "TEMPFORN.NOME\n" +
                             "FROM\n" +
                             "TEMP_FORNECEDOR TEMPFORN\n" +
                             "LEFT JOIN PRODUTOS_FORNECEDOR FORN ON\n" +
                             "TEMPFORN.CODIGO = FORN.CODIGO\n" +
                             "AND TEMPFORN.NOME = FORN.NOME\n" +
                             "WHERE\n" +
                             "FORN IS NULL\n")
            parametros = ''
            conexao.query(instrucao_sql, parametros)
            return conexao.retorna_todos_registros()

    def atualiza_fornecedor(self, codigo, nome) :
        with conexaoPostgresRDS() as conexao :
            sql = u"UPDATE public.produtos_fornecedor SET data_alteracao= %s, nome= %s WHERE codigo = %s"
            parametros = '2020-07-13', nome, codigo
            conexao.query(sql, parametros)

    def sincroniza_fornecedores(self) :
        print('Iniciando Sincronização de Fornecedores')
        self.limpa_tabela_temporaria_fornecedores()
        self.carrega_fornecedores_para_a_tabela_temporaria()
        fornecedores_divergentes = dict(self.registros_de_fornecedores_diferentes_tabela_temporaria_e_fornecedor())
        for codigo, nome in fornecedores_divergentes.items() :
            if self.codigo_fornecedor_nao_cadastrado(int(codigo)) :
                self.insere_fornecedor(codigo, nome)
            else :
                self.atualiza_fornecedor(codigo, nome)
        print('Sincronização de Peças finalizada com sucesso!')


# Tenho que fazer quando o fornecedor não está mais na tabela temporaria setar para ativo = false

class SincronizaPecas :

    def __init__(self) :
        pass

    def limpa_tabela_temporaria_pecas(self) :
        with conexaoPostgresRDS() as conexao :
            sql = u"DELETE FROM public.temp_peca"
            parametros = ''
            conexao.query(sql, parametros)

    def busca_cod_descricao_tipos_de_peca_bd_firebird(self) :
        with conexaoFirebird() as conexao_firebird :
            instrucao_sql = ("SELECT CODSUBGRUPO,DESCRICAO FROM SUBGRUPO "
                             "WHERE DESCRICAO IS NOT NULL ORDER BY CODSUBGRUPO")
            return conexao_firebird.executeSQL(instrucao_sql).fetchall()

    def carrega_pecas_para_a_tabela_temporaria(self) :
        pecas = dict(self.busca_cod_descricao_tipos_de_peca_bd_firebird())
        with conexaoPostgresRDS() as conexao :
            sql = u"INSERT INTO public.temp_peca(codigo, nome) VALUES (%s,%s);"
            for codigo, nome in pecas.items() :
                parametros = codigo, nome
                conexao.query(sql, parametros)

    def codigo_peca_nao_cadastrado(self, cod_peca) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = "SELECT * FROM produtos_peca WHERE codigo = '%s';"
            parametro = cod_peca,
            conexao.query(instrucao_sql, parametro)
            return not conexao.retorna_todos_registros()

    def insere_peca(self, codigo, nome) :
        with conexaoPostgresRDS() as conexao :
            sql = u"INSERT INTO public.produtos_peca (data_criacao, data_alteracao, ativo,codigo,nome) VALUES (%s,%s,%s,%s,%s);"
            parametros = '2020-07-15', '2020-07-15', True, codigo, nome
            conexao.query(sql, parametros)

    # busca_registros divergentes
    def busca_registros_de_pecas_divergentes(self) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = ("select\n" +
                             "tpeca.codigo,\n" +
                             "tpeca.nome\n" +
                             "from\n" +
                             "temp_peca tpeca\n" +
                             "left join produtos_peca ppeca on\n" +
                             "tpeca.codigo = ppeca.codigo\n" +
                             "and tpeca.nome = ppeca.nome\n" +
                             "where\n" +
                             "ppeca is null\n")
            parametros = ''
            conexao.query(instrucao_sql, parametros)
            return conexao.retorna_todos_registros()

    def atualiza_peca(self, codigo, nome) :
        with conexaoPostgresRDS() as conexao :
            sql = u"UPDATE public.produtos_peca SET data_alteracao= %s, nome= %s WHERE codigo = %s"
            parametros = '2020-07-15', nome, codigo
            conexao.query(sql, parametros)

    def sincroniza_pecas(self) :
        print('Iniciando Sincronização de Peças')
        self.limpa_tabela_temporaria_pecas()
        self.carrega_pecas_para_a_tabela_temporaria()
        pecas_divergentes = dict(self.busca_registros_de_pecas_divergentes())
        for codigo, nome in pecas_divergentes.items() :
            if self.codigo_peca_nao_cadastrado(int(codigo)) :
                self.insere_peca(codigo, nome)
            else :
                self.atualiza_peca(codigo, nome)
        print('Sincronização de Peças finalizada com sucesso!')


class SincronizaProdutos :

    def __init__(self) :
        pass

    def limpa_tabela_temporaria_produto(self) :
        with conexaoPostgresRDS() as conexao :
            sql = u"DELETE FROM public.temp_produto"
            parametros = ''
            conexao.query(sql, parametros)

    def busca_todos_produtos_cadastrados_firebird(self) :
        with conexaoFirebird() as conexao_firebird :
            instrucao_sql = ("SELECT REFERENCIA,CODGRUPO,CODSUBGRUPO,DATACADASTRO,DATAALTERACAO FROM PRODUTO")
            return conexao_firebird.executeSQL(instrucao_sql).fetchall()

    def carrega_produtos_para_a_tabela_temporaria(self,produtos) :
        #produtos = self.busca_todos_produtos_cadastrados_firebird()
        with conexaoPostgresRDS() as conexao :
            for codigo, fornecedor, peca, data_cadastro, data_alteracao in produtos :
                parametros = codigo, fornecedor, peca, data_cadastro, data_alteracao
                sql = u"INSERT INTO public.temp_produto (codigo, fornecedor, peca, data_cadastro, data_alteracao) VALUES(%s,%s,%s,%s,%s);"
                conexao.query(sql, parametros)

    def retorna_ids_de_fornecedor_e_peca_das_tabelas_do_postgres(self) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = ("SELECT\n" +
                             "TEMPPROD.CODIGO AS CODIGO, PRODFORN.ID AS FORNECEDOR,PRODPECA.ID AS PECA\n" +
                             "FROM\n" +
                             "TEMP_PRODUTO TEMPPROD\n" +
                             "INNER JOIN PRODUTOS_FORNECEDOR PRODFORN ON\n" +
                             "TEMPPROD.FORNECEDOR = PRODFORN.CODIGO\n" +
                             "INNER JOIN PRODUTOS_PECA PRODPECA ON\n" +
                             "TEMPPROD.PECA = PRODPECA.CODIGO\n")
            # "WHERE tempp   rod.codigo in (%s)\n")
            parametros = ""
            conexao.query(instrucao_sql, parametros)
            return conexao.retorna_todos_registros()

    def insere_todos_os_produtos_da_tabela_temporaria(self) :
        produtos_temp = self.retorna_ids_de_fornecedor_e_peca_das_tabelas_do_postgres()
        with conexaoPostgresRDS() as conexao :
            sql = u"INSERT INTO public.produtos_produto (data_criacao, data_alteracao, ativo, codigo, fornecedor_id, peca_id) VALUES(%s,%s,%s,%s,%s,%s);"
            for codigo, fornecedor_id, peca_id in produtos_temp :
                parametros = '2020-07-28', '2020-07-28', True, codigo, fornecedor_id, peca_id
                conexao.query(sql, parametros)

    def verifica_produtos_a_serem_inseridos_ou_atualizados(self) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = ("SELECT\n" +
                             "*\n" +
                             "FROM\n" +
                             "TEMP_PRODUTO TPROD\n" +
                             "LEFT JOIN PRODUTOS_PRODUTO PPROD ON\n" +
                             "TPROD.CODIGO = PPROD.CODIGO\n" +
                             "WHERE PPROD.CODIGO IS NULL\n" +
                             "OR TPROD.DATA_CADASTRO > (\n" +
                             "SELECT\n" +
                             "MAX(DATA_CRIACAO) AS DATACRIACAO\n" +
                             "FROM\n" +
                             "PRODUTOS_PRODUTO )\n" +
                             "OR TPROD.DATA_ALTERACAO > (\n" +
                             "SELECT\n" +
                             "MAX(DATA_ALTERACAO) AS DATAALTERACAO\n" +
                             "FROM\n" +
                             "PRODUTOS_PRODUTO)\n" +
                             "ORDER BY TPROD.CODIGO\n" +
                             "\n")
            parametros = ''
            conexao.query(instrucao_sql, parametros)
            return conexao.retorna_todos_registros()

    def quantidade_de_produtos_na_tabela(self) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = ("select count(*) from produtos_produto")
            parametros = ''
            conexao.query(instrucao_sql, parametros)
            for quantidade in conexao.retorna_todos_registros()[0] :
                return quantidade

    def retorna_maior_data_de_criacao_e_atualizacao_de_produto(self) :
        with conexaoPostgresRDS() as conexao :
            instrucao_sql = ("select\n" +
                             "max(data_criacao) as dataCriacao,\n" +
                             "max(data_alteracao) as dataAlteracao\n" +
                             "from\n" +
                             "produtos_produto\n" +
                             "\n")
            parametros = ''
            conexao.query(instrucao_sql, parametros)
            return conexao.retorna_todos_registros()

    def produtos_a_serem_atualizados_ou_inseridos(self) :
        data_cadastro, data_alteracao = self.retorna_maior_data_de_criacao_e_atualizacao_de_produto()[0]
        with conexaoFirebird() as conexao_firebird :
            instrucao_sql = ("SELECT\n" +
                             "REFERENCIA,\n" +
                             "CODGRUPO,\n" +
                             "CODSUBGRUPO,\n" +
                             "DATACADASTRO,\n" +
                             "DATAALTERACAO\n" +
                             "FROM\n" +
                             "PRODUTO\n" +
                             "WHERE\n" +
                             "DATAALTERACAO > '%s'\n" +
                             "OR DATACADASTRO >'%s'") % (str(data_alteracao), str(data_cadastro))
            return conexao_firebird.executeSQL(instrucao_sql).fetchall()

    def realiza_carga_total(self) :
        print("Carga Total de Produtos será realizada...")
        self.limpa_tabela_temporaria_produto()
        self.carrega_produtos_para_a_tabela_temporaria(self.busca_todos_produtos_cadastrados_firebird())
        self.insere_todos_os_produtos_da_tabela_temporaria()
        print("Carga Total de Produtos Finalizada")

    def sincroniza_produtos(self) :
        print('Iniciando Sincronização de Produtos')
        if self.quantidade_de_produtos_na_tabela() == 0 :
            self.realiza_carga_total()
        else :
            if self.produtos_a_serem_atualizados_ou_inseridos() :
                print('Produtos a serem atualizados:')
                self.limpa_tabela_temporaria_produto()
                self.carrega_produtos_para_a_tabela_temporaria(self.produtos_a_serem_atualizados_ou_inseridos())



sincProdutos = SincronizaProdutos()
sincProdutos.sincroniza_produtos()
print(sincProdutos.produtos_a_serem_atualizados_ou_inseridos())
# datas = sincProdutos.retorna_maior_data_de_criacao_e_atualizacao_de_produto()
# data_cria,data_alt = datas[0]
# print(data_cria)
# for data_cri, data_alt in datas:
#     print(data_cri)
#     print(data_alt)

# print(sincProdutos.produtos_a_serem_atualizados_ou_inseridos())
