import os
import datetime
import shutil

def copia_todos_arquivos_para_pasta_temporaria(pasta) :
    """
    Percorre todas as pastas e sub pastas e copia todos os arquivos
    para uma uma pasta temporária
    """
    arquivos_encontrados = []
    arquivos_copiados = []
    diretorio_temporario = (os.getcwd() + "/" + "temp_" + str(datetime.datetime.now().microsecond))
    os.mkdir(diretorio_temporario)
    for (dirpath, dirnames, files) in os.walk(pasta, topdown=True) :
        with os.scandir(dirpath) as it :
            for entry in it :
            # if img_file.name.endswith('.jpg') and img_file.is_file() :
            # shutil.copyfile(dirpath, diretorio_temporario)
                if entry.is_file() :
                    arquivos_encontrados.append(entry.name)
                    old_file_path = os.path.join(dirpath, entry.name)
                    shutil.copy(old_file_path,diretorio_temporario)

            print("Copiando arquivos.....")
    with os.scandir(diretorio_temporario) as it :
        for entry in it :
            arquivos_copiados.append(entry.name)

    print("Total de arquivos encontrados: ", len(arquivos_encontrados))
    print("Arquivos encontrados: ",arquivos_encontrados)
    print("Total de arquivos copiados: ", len(arquivos_copiados))
    print("Arquivos copiados: ",arquivos_copiados)
    lista_final = list(set(arquivos_encontrados) - set(arquivos_copiados))
    print("Arquivos não copiados: ",lista_final )

    return diretorio_temporario


# print(type(lista_de_arquivos))
# def verifica_se_arquivo_e_jpg(lista_de_arquivos):
#     for arquivo in lista_de_arquivos:
#         if img_file.name.endswith('.jpg') and img_file.is_file() :
#

def valida_se_arquivos_sao_jpg(path):
    arquivos_jpg = []
    arquivos_not_jpg = []
    with os.scandir(path) as it :
        for entry in it :
            if entry.name.endswith('.jpg') :
                arquivos_jpg.append(entry.name)
            else:
                arquivos_not_jpg.append(entry.name)
    print("Arquivos JPG: ",arquivos_jpg)
    print("Arquivos não são JPG: ",arquivos_not_jpg)
lista_de_arquivos = copia_todos_arquivos_para_pasta_temporaria("Fotos")
valida_se_arquivos_sao_jpg(lista_de_arquivos)
# lista_arquivos = lista_diretorios("Fotos")
