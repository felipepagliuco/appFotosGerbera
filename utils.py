import os
import datetime
import shutil
from collections import Counter
import re
import hashlib
import fdb
import sys
from PIL import Image

class BD:

    def __init__(self):
        self.con=None

    def __enter__(self):
        self.con = fdb.connect(dsn='localhost/3050:/home/felipe/workspace/appFotosGerbera/BD/arqccs.fdb',
                               user='SYSDBA',
                               password='masterkey')
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.con.close()

    def executeSQL(self,sql):
        # Create a Cursor object that operates in the context of Connection con:
        try:
            self.cur = self.con.cursor()
            return self.cur.execute(sql)
        finally:
            self.con.cursor().close()

    def retorna_bd(self):
        print(self.con.database_name)

class PreparaArqImportacao:

    def __init__(self):
        self.folder_a_ser_importada = "Fotos"
        self.diretorio_temporario = None
        self.path_folder_conformes = None
        self.path_folder_arq_pendentes_verificacao = None
        self.path_folder_pendencias = None
        self.path_folder_pendencias_arquivo_duplicado = None
        self.path_folder_pendencias_cod_ref_nao_encontrada = None
        self.path_folder_pendencias_nome_arquivo_invalido = None
        self.path_folder_pendencias_tipo_arquivo_invalido = None

    def __cria_nomes_arquivos__(self):

        self.diretorio_temporario = (os.getcwd() + "/" + "temp_" + str(datetime.datetime.now().microsecond))
        self.path_folder_conformes = self.diretorio_temporario+"/"+"conformes"
        self.path_folder_arq_pendentes_verificacao = self.diretorio_temporario + "/" + "arquivos_pendentes_verificacao"
        self.path_folder_pendencias = self.diretorio_temporario + "/" + "pendencias"
        self.path_folder_pendencias_arquivo_duplicado = self.diretorio_temporario + "/" + "pendencias/arquivos_duplicados"
        self.path_folder_pendencias_cod_ref_nao_encontrada = self.diretorio_temporario + "/" + "pendencias/cod_ref_nao_encontrada"
        self.path_folder_pendencias_nome_arquivo_invalido = self.diretorio_temporario + "/" + "pendencias/nome_arquivo_invalido"
        self.path_folder_pendencias_tipo_arquivo_invalido = self.diretorio_temporario + "/" + "pendencias/tipo_arquivo_invalido"

    def cria_estrutura_de_pastas(self):

        self.__cria_nomes_arquivos__()
        os.mkdir(self.diretorio_temporario)
        os.mkdir(self.path_folder_conformes)
        os.mkdir(self.path_folder_arq_pendentes_verificacao)
        os.mkdir(self.path_folder_pendencias)
        os.mkdir(self.path_folder_pendencias_arquivo_duplicado)
        os.mkdir(self.path_folder_pendencias_cod_ref_nao_encontrada)
        os.mkdir(self.path_folder_pendencias_nome_arquivo_invalido)
        os.mkdir(self.path_folder_pendencias_tipo_arquivo_invalido)

    def lista_arquivos_de_uma_pasta(self,path_folder) :
        arquivos = []
        with os.scandir(path_folder) as it :
            for entry in it :
                arquivos.append(entry.name)
        os.scandir().close()
        return arquivos

    def copia_arquivos_a_serem_importados(self,folder_origem,folder_destino) :
        """
        Percorre todas as pastas e sub pastas e copia todos os arquivos
        Para a pasta : arquivos_pendentes_verificacao
        """
        arquivos_encontrados = []
        arquivos_copiados = []
        for (dirpath, dirnames, files) in os.walk(folder_origem, topdown=True) :
            with os.scandir(dirpath) as it :
                for entry in it :
                    if entry.is_file() :
                        arquivos_encontrados.append(entry.name)
                        old_file_path = os.path.join(dirpath, entry.name)
                        shutil.copy(old_file_path,folder_destino)
                        print   ("Copiando arquivo.....", entry.name)
        os.scandir().close()

        print("Total de arquivos encontrados: ", len(arquivos_encontrados))
        print("Arquivos encontrados: ",arquivos_encontrados)
        print("Arquivos repetidos = ",[item for item, count in Counter(arquivos_encontrados).items() if count > 1])
        print("Total de arquivos copiados: ", len(arquivos_copiados))
        print("Arquivos copiados: ",arquivos_copiados)

        # return diretorio_temporario

    def mover_arquivo_de_uma_pasta(self,origem,destino,file):
        shutil.move(origem+"/"+file,destino)

    def arquivos_sao_jpg(self,file_name):
        file_name = file_name.lower()
        return (file_name.find(".jpg") >= 0)

    def nome_arquivo_e_valido(self,string) :
        """
        Retorna se o nome do arquivo é válido contendo os caracteres -,.jpg,.JPG
        :param string: nome do arquivo
        :return: True or False
        """
        string = string.lower()
        return not (not (string.find("-") >= 0) or not (string.find(",") >= 0))
                    #or not ((string.find(".jpg") >= 0)))

    def retorna_cod_produto_do_nome_do_arquivo(self,nome_arquivo) :
        """
        Retorna o código do produto contido no nome do arquivo
        :param nome_arquivo:
        :return: str código do produto
        """
        return (((re.search("[0-9]*", nome_arquivo)).group()))


    def gerar_hash_md5(self,file):
        dict_arquivo_hash = dict()
        hasher = hashlib.md5()
        with open(file,'rb') as file:
            buf = file.read()
            hasher.update(buf)
        dict_arquivo_hash[file.name] = hasher.hexdigest()
        return dict_arquivo_hash
        print(hasher.hexdigest())

    def retorna_lista_de_codigos_dos_nomes_dos_arquivos(self,lista_nomes_arquivos_validos):
        lista_codigos = []
        for nome_arquivo in lista_nomes_arquivos_validos:
            lista_codigos.append(self.retorna_cod_produto_do_nome_do_arquivo(nome_arquivo))
        return lista_codigos


    def __codigo_esta_no_bd_de_dados(self,conexao,cod_produto):
        sql = "select REFERENCIA from PRODUTO where REFERENCIA = '%s'" % cod_produto
        return conexao.executeSQL(sql).fetchonemap() is not None

    def verifica_se_cod_produto_esta_no_BD(self,arquivos):
        with BD() as conexao:
            cod_nao_esta_no_BD = []
            for arquivo in arquivos:
                if not self.__codigo_esta_no_bd_de_dados(conexao,
                                                         self.retorna_cod_produto_do_nome_do_arquivo(arquivo)):
                    cod_nao_esta_no_BD.append(arquivo)
            return cod_nao_esta_no_BD

class ManipulacaoImagensUtils():

    def __init__(self):
        pass

    def compressJPG(this,pathfolder,file,verbose=False) :
        os.chdir(pathfolder)
        filepath = pathfolder+"/"+file
        picture = Image.open(filepath)
        picture = picture.resize((1414, 1414))
        picture.save("Compressed_" + file, "JPEG", optimize=True, quality=15,)



importacao = PreparaArqImportacao()
importacao.cria_estrutura_de_pastas()
importacao.copia_arquivos_a_serem_importados(importacao.folder_a_ser_importada,
                                             importacao.path_folder_arq_pendentes_verificacao)
arquivos = importacao.lista_arquivos_de_uma_pasta(importacao.path_folder_arq_pendentes_verificacao)

for arquivo in arquivos:
    if not importacao.nome_arquivo_e_valido(arquivo):
        importacao.mover_arquivo_de_uma_pasta(importacao.path_folder_arq_pendentes_verificacao,
                                              importacao.path_folder_pendencias_nome_arquivo_invalido,
                                              arquivo)
    elif not importacao.arquivos_sao_jpg(arquivo):
        importacao.mover_arquivo_de_uma_pasta(importacao.path_folder_arq_pendentes_verificacao,
                                              importacao.path_folder_pendencias_tipo_arquivo_invalido,
                                              arquivo)
arquivos = importacao.lista_arquivos_de_uma_pasta(importacao.path_folder_arq_pendentes_verificacao)
arquivos_nao_estao_no_BD = importacao.verifica_se_cod_produto_esta_no_BD(arquivos)
for arquivo in arquivos_nao_estao_no_BD:
    importacao.mover_arquivo_de_uma_pasta(importacao.path_folder_arq_pendentes_verificacao,
                                          importacao.path_folder_pendencias_cod_ref_nao_encontrada,
                                          arquivo)
arquivos = importacao.lista_arquivos_de_uma_pasta(importacao.path_folder_arq_pendentes_verificacao)
comprimeImagem = ManipulacaoImagensUtils()
for arquivo in arquivos:
    comprimeImagem.compressJPG(importacao.path_folder_arq_pendentes_verificacao,arquivo)




