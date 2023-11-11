from time import sleep
import mysql.connector
from mysql.connector import Error
import requests
import json
import csv
import os
import dotenv
dotenv.load_dotenv()

host = os.environ['host']
user = os.environ['user']
password = os.environ['password']
database = os.environ['database']


try:
  db_connection = mysql.connector.connect(host= host, user= user, password= password, database= database)
	
  consulta_cnpj = "select ce.cpf_cnpj, fc.CodCli from comum_entidades ce inner join faturamento_clientes fc on ce.id = fc.entidade_id left join faturamento_clientes_cnae fcc on fc.CodCli = fcc.faturamento_clientes_id where cpf_cnpj is not null and CHAR_LENGTH(cpf_cnpj) = 14 and fcc.faturamento_clientes_id is null"
  cursor = db_connection.cursor()
  cursor.execute(consulta_cnpj)
  linhas = cursor.fetchall()
  print("Numero total de registros retornados: ", cursor.rowcount)
  
except Error as e:
    print("Erro ao acessar a tabela MySQL", e)
finally:
  if  (db_connection.is_connected()):
      db_connection.close()
      cursor.close()
      print("Conexão ao MySQL encerrada.")


def consulta_cnpj2(cnpj):
  
    url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
    querystring = {"token":"XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX","cnpj":"06990590000123","plugin":"RF"}
    response = requests.request("GET", url, params=querystring)
    resp = json.loads(response.text)
    # if resp["status"] == "ERROR":
    #   print(resp["message"], cnpj)
    # else:
    #   print(cnpj, "Atividade principal:",  resp["atividade_principal"], "Atividades secundárias:", resp["atividades_secundarias"]) 
    return resp
        
def inserir_banco(id, resposta):
  try:
    db_connection = mysql.connector.connect(host= host, user= user, password= password, database= database)
    codigo_atividade_principal = resposta["atividade_principal"][0]["code"]
    atividade_principal = resposta["atividade_principal"][0]["text"]
    for atividade_secundaria in resposta['atividades_secundarias']:
      inserir_info = f"insert into faturamento_clientes_cnae (faturamento_clientes_id, codigo_atividade_principal, atividade_principal, codigo_atividade_secundaria, atividade_secundaria) values ({id}, '{codigo_atividade_principal}', '{atividade_principal}', '{atividade_secundaria['code']}', '{atividade_secundaria['text']}')"
      print(inserir_info)
      cursor = db_connection.cursor()
      cursor.execute(inserir_info)
      print("inseriu")
    db_connection.commit()
  except Error as e:
    print("Erro ao inserrir a tabela MySQL", e)
  finally:
    if  (db_connection.is_connected()):
      db_connection.close()
      cursor.close()

csv_cpf_invalido = "cpf_invalido.csv"

cpfs_invalidos = []

for i, linha in enumerate(linhas):
  if i % 3 == 0 and i>0:
    print("Espere um minuto, por favor :)")
    sleep(60)
    print("Passou 1 min")
  guard_resp = consulta_cnpj2(linha[0])
  if guard_resp["status"] != "ERROR":
    inserir_banco(linha[1], guard_resp)
  else:
    print("CNPJ invalido: ", linha[0])
    cpfs_invalidos.append([linha[0], "CNPJ invalido"])
    with open(csv_cpf_invalido, mode='w', newline='') as arquivo_csv:
        escritor_csv = csv.writer(arquivo_csv)
        escritor_csv.writerows(cpfs_invalidos)
    print(f"Arquivo CSV '{csv_cpf_invalido}' criado com sucesso!")