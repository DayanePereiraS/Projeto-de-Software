import os
import random
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

nome_arquivo = "xls_conformidade_site_20240505_101650943.xls"
url = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/arquivos/xls_conformidade_site_20240505_101650943.xls"; response = requests.get(url)

if response.status_code == 200:
    with open(nome_arquivo, 'wb') as arquivo:
        arquivo.write(response.content)
    print("Download concluído com sucesso. O arquivo foi salvo como", nome_arquivo)
else:
    print("Erro ao fazer o download. Código de status:", response.status_code)

produto_preco_maximo_consumidor = pd.read_excel(nome_arquivo,  header=41)

colunas_desejadas = [     
                    'SUBSTÂNCIA', 'CNPJ', 'LABORATÓRIO', 'CÓDIGO GGREM', 'REGISTRO',
                    'EAN 1', 'PRODUTO', 'APRESENTAÇÃO', 'CLASSE TERAPÊUTICA',
                    'TIPO DE PRODUTO (STATUS DO PRODUTO)', 'REGIME DE PREÇO',
                    'PF Sem Impostos', 'PF 0%', 'PF 17%', 'PF 17,5%', 'PF 18%', 'PF 19%', 'PF 20%', 'PF 21%', 'PF 22%',
                    'PMC Sem Imposto', 'PMC 0%', 'PMC 17%', 'PMC 17,5%', 'PMC 18%', 'PMC 19%', 'PMC 20%', 'PMC 21%', 'PMC 22%',
                    'RESTRIÇÃO HOSPITALAR', 'CAP', 'CONFAZ 87', 'ICMS 0%', 'TARJA'
                    ]

colunas_pmc = [
                'PF 0%', 'PF 17%', 'PF 17,5%', 'PF 18%', 'PF 19%', 'PF 20%', 'PF 21%', 'PF 22%',
                'PMC 17%', 'PMC 17,5%', 'PMC 18%', 'PMC 19%', 'PMC 20%', 'PMC 21%', 'PMC 22%'
            ]

produto_preco_maximo_consumidor[colunas_pmc] = produto_preco_maximo_consumidor[colunas_pmc].map(lambda val: float(str(val).replace('*', '')))
produto_preco_maximo_consumidor = produto_preco_maximo_consumidor[colunas_desejadas].copy()
produto_preco_maximo_consumidor['_CNPJ'] = produto_preco_maximo_consumidor['CNPJ'].str.replace('.', '').str.replace('/', '').str.replace('-', '')
produto_preco_maximo_consumidor.info(); print(); os.remove(nome_arquivo); print('o arquivo foi deletado no diretório..'); produtos = produto_preco_maximo_consumidor

produtos['ID CLASSE TERAPÊUTICA'] = (produtos['CLASSE TERAPÊUTICA'].str.split('-')).str[0]
produtos['CLASSE TERAPÊUTICA'] = produtos['CLASSE TERAPÊUTICA'].str.split('-').str[1].str.strip()

colunas_desejadas = [   
                        'CNPJ', '_CNPJ', 'EAN 1', 'PRODUTO', 'SUBSTÂNCIA', 'APRESENTAÇÃO', 'ID CLASSE TERAPÊUTICA',
                        'CLASSE TERAPÊUTICA', 'PMC 17%', 'PMC 17,5%', 'PMC 18%', 'PMC 19%', 'PMC 20%', 'PMC 21%', 'PMC 22%'
                    ]

produtos = produtos[colunas_desejadas].copy(); produtos = produtos[~produtos['PMC 17%'].isna()]

produtos['PREÇO MÉDIO'] = produto_preco_maximo_consumidor[colunas_pmc].mean(axis=1); produtos = produtos.sort_values(by=['PRODUTO'], ascending = True); produtos.info()

compra_taxas_icms_23 = {
    'Sul': 0.175,
    'Nordeste': 0.18,
    'Sudeste': 0.22,
    'Norte': 0.21,
    'Centro-Oeste': 0.22,
}

reajuste_medicamento_23 = 0.10; reajuste_medicamento_24 = 0.06; 

# key_path = os.getenv('onedrive')+'//Projetos//bigQuery//chave//bc_key.json'; key_path; 
# credentials = service_account.Credentials.from_service_account_file(filename=key_path, scopes=['https://www.googleapis.com/auth/cloud-platform'])

def get_credentials():
    # Option 1: Use environment variables
    if 'GCP_SA_KEY' in os.environ:
        credentials_info = json.loads(os.environ['GCP_SA_KEY'])
        return service_account.Credentials.from_service_account_info(credentials_info)
    
    # Option 2: Use a JSON file (more secure than hardcoding, but still requires careful handling)
    elif os.path.exists('path/to/your/service-account-key.json'):
        return service_account.Credentials.from_service_account_file(
            'path/to/your/service-account-key.json',
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
    
    # Option 3: Fall back to hardcoded credentials (not recommended for production)
    else:
        print("Warning: Using hardcoded credentials. This is not secure for production use.")
        credentials_info = {
            "type": "service_account",
            "project_id": "teste-f60d3",
            "private_key_id": "65ca22febda3d8add67ec0da1ab0c645cd8494af",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDWMhiHiubdbo8t\nRAHDDm4tU7DG1fUNxkJMG6+PwvrssQimU0jfqZdV10j6gMqUcYr5e0tcd/ABm43n\nwI68vX5libN3EXZkQcreA2fA3WS/cZCIyTWlgSp0wKVSCu11vB1ZHD/tuWbILc40\n7eN8yVRlM7DUEB76p5atEBWNlUhNSwhmYcGxlXltgaqrwon1VrLoNkPNkfiU/Ref\nJuja13Bbv4JHa54o7eVwKGWyBkuV01oeub+bHk9jbxxQK9RMuE90S+696pzndkcW\n/MZ0M9yjevclJIY4W7RMVHHNTv3ILkecjj+V7Ock+oMcHTYYppvNLIOZbdvHI4Fz\nzkV5V1gtAgMBAAECggEACVSJzn+tskgSWwurEvrSME3EtciUodq4wRV0pou8/+nv\nQUV/92QtCCt2TZkmGeS/Q8JL5FWCIiPLQOTZot2TP6uJon5jfJcEFfsqfhN2w8MX\n42l2WYUAmzDYErSQpvAAjETngPyyPjaGlLYnEmS3tSfVBEvGea8qqFO6yJ3P1jQB\n27xU1nje6vMfNsPIkCLJIbu3JmSKM8NHhmJeYYwZcKBcf3fyKZHQYyd13HrOilMk\nX6VEGfTyPVApdqZh3h9DdWu81ay1H4oCTswdng9J5W1nOnLF+bOCo+7FVTNIZST2\nhpExq5oBbD76YORSBJu7j2e67UoaGQaNuSvaoiTIcQKBgQDwEWLKoDHkDWlzQoeR\nN2qVZIw3pzi9NcIM1AyIkNw9FxZA9Rtv7XWPkikvtJn6T+ejMHZJ5qtT+iOghNu7\nyz3HzJwvx+LI3ixy41qCSwvYyOJmop+ubcP7g7LpfpfhTD3Y7O6F84Ix9CsADpI8\ncDo2Dg1IhSPNGf9iP+N/FWHkPQKBgQDkaSflLEXX+ajewUTlbJQCBvZZWbFlff2n\n+qms8kIiaVVrkuJiVQeSpbePIeImFTA7IYAUltRlfwFMJhPxz4GyWKSJOyqStVF3\nC+/jQEdLDximwzgnt3LEeBDc2ezuIO6C8yymkwtQTlhSkqAUUfoAcjjTXkTG0A5U\nt/qqO5FSsQKBgQCXuLUyEDpXwe4yLQrWySowToKbrbCbTD1etClTqhG2/j9PQFld\nzLpuyVYEU2S5IQTpSviHTiRbh4w6p5zju9hIzNStEewPPPLkjTGnhNxw3szPRtoa\neD7TV3GcRiig9/lifqNkvvg6r/D5MTxUvCzd+tWETrjIqDSSGISjzZS3lQKBgC4i\nQymZsJ230nDzjmvca2ShbV6MrDT4pqQoD99bqxnyNoucxlbgH8Bx8kpZqKjSfMRk\nts7xzAKYDXYI9txPcj3Ig6soJSgusT6fTZuT3xJ9ARils5DqD6c5LQa+iYlrY2FV\npn4akx1sRZCgBu8zw5AVgf7HOpMBcPORmXKKx4ZxAoGBAK/mi00K6dmA17iaXo6k\nXHbcFopx7tyOybcpygkky2ECcM3UD7Sq16dtiUuwCHTD7+ZT+BCo8eHivVA95/Qs\nVVn1KFzGezJdznkiAeX9b/rkFkGQyQ0iMdw84BDcoLgfHrrPuUukY6zafg3Bg9Nh\nigL8oi7SRGOxJaOY+USZ8EGu\n-----END PRIVATE KEY-----\n",
            "client_email": "apppub@teste-f60d3.iam.gserviceaccount.com",
            "client_id": "109826359829824923347",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/apppub%40teste-f60d3.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }
        return service_account.Credentials.from_service_account_info(credentials_info)

# Use the credentials
credentials = get_credentials()


cnpjs = produto_preco_maximo_consumidor['CNPJ'].str.replace('.', '').str.replace('/', '').str.replace('-', '').unique().tolist(); print(f'Criação da lista ', cnpjs[0:5])

cnpjs_formatados = ", ".join([f"'{cnpj}'" for cnpj in cnpjs]); print(f'Formatação para o SQL: ',cnpjs_formatados)

query_laboratorio =f'''
          SELECT DISTINCT	cnpj,	razao_social,	cnae_2_primaria,	id_municipio, cep, numero
          FROM basedosdados.br_me_exportadoras_importadoras.estabelecimentos
          WHERE cnpj IN ({cnpjs_formatados})
        '''

query_hospitais = '''
                SELECT DISTINCT cnpj,	razao_social,	cnae_2_primaria,	id_municipio, cep,	numero
                FROM basedosdados.br_me_exportadoras_importadoras.estabelecimentos
                WHERE cnae_2_primaria LIKE '86%'
              '''
              
query_municipio = '''
              SELECT * FROM basedosdados.br_bd_diretorios_brasil.municipio
'''
query_cep = '''
          SELECT * FROM basedosdados.br_bd_diretorios_brasil.cep
        '''
        
gcp_base_dados = pd.read_gbq(credentials=credentials, query=query_hospitais); hospitais = gcp_base_dados; hospitais.info(); print(); hospitais

gcp_base_dados = pd.read_gbq(credentials=credentials, query=query_laboratorio); laboratorio = gcp_base_dados; laboratorio.info(); print(); laboratorio

gcp_base_dados = pd.read_gbq(credentials=credentials, query=query_municipio)
municipio = gcp_base_dados; municipio.info(); municipio = municipio.rename(columns={'nome':'municipio',
                                                                                    'nome_regiao':'regiao',
                                                                                    'nome_mesorregiao':'zona',
                                                                                    'nome_uf':'estado',
                                                                                    'nome_microrregiao':'cidade'
                                                                          })

municipio = municipio[[ 'id_municipio',
                        'municipio',
                        'zona',
                        'regiao',
                        'sigla_uf',
                        'estado',
                        'cidade',
                        'nome_regiao_intermediaria'
                      ]]; print(); municipio

hospitais_cadastro = hospitais.merge(municipio, how='left', on='id_municipio'); hospitais_cadastro.info(); print()
laboratorio_cadastro = laboratorio.merge(municipio, how='left', on='id_municipio'); laboratorio_cadastro.info(); print()

lista_hospitais = hospitais['cnpj'].tolist(); lista_ean = produtos['EAN 1'].tolist(); lista_status = ['Aceito', 'Pendente de Aceite', 'Em Aprovação']

df = pd.DataFrame(columns = ['nr_pedido', 'cnpj', 'cod_ean', 'status', 'quantidade',  'data_pedido', 'estratégia' ])

for i in range(20000):
    numero_pedido_aleatorio = random.randint(100000, 999999)
    hospital_aleatorio = random.choice(lista_hospitais)
    ean_aleatorio = random.choice(lista_ean)
    status_aleatorio = random.choice(lista_status)
    quantidade_aleatoria = random.randint(1, 30)
    
    data_inicial = datetime(2023, 1, 1)
    data_final = datetime.now()
    delta = data_final - data_inicial
    dias_aleatorios = random.randint(0, delta.days)
    data_aleatoria = data_inicial + timedelta(days=dias_aleatorios)

    contrato_ou_spot = random.randint(0, 1) ; contrato_spot = 'Contrato' if contrato_ou_spot == 0 else 'Spot'
    
    df.loc[len(df)] = [numero_pedido_aleatorio, hospital_aleatorio, ean_aleatorio, status_aleatorio, quantidade_aleatoria, data_aleatoria, contrato_spot]

duplicatas_primeira_ocorrencia = df.duplicated(subset=['nr_pedido'], keep='first'); pedidos_duplicados = df[df.duplicated(subset=['nr_pedido'], keep='first')]

num_duplicatas_por_pedido = pedidos_duplicados.groupby('nr_pedido').cumcount() + 1; df.loc[num_duplicatas_por_pedido.index, 'nr_pedido'] += num_duplicatas_por_pedido

pedidos = df; pedidos = pedidos.merge(hospitais_cadastro[['cnpj', 'razao_social', 'sigla_uf', 'regiao' ]], how='left', on='cnpj')
pedidos = pedidos.merge(produtos[['EAN 1', 'PREÇO MÉDIO' ]], how='left', left_on='cod_ean', right_on='EAN 1')

def aplicar_icms_compra(row):
    regiao = row['regiao']; preco_medio = row['PREÇO MÉDIO']; ano_pedido = row['data_pedido'].year
    
    if regiao in compra_taxas_icms_23:
        taxa_icms_compra = compra_taxas_icms_23[regiao]
        preco_com_icms = preco_medio * (1 + taxa_icms_compra)
    else:
        preco_com_icms = preco_medio

    if ano_pedido == 2024:
        reajuste_anual = reajuste_medicamento_24
    elif ano_pedido == 2023:
        reajuste_anual = reajuste_medicamento_23

    preco_final = preco_com_icms * (1 + reajuste_anual)
    return preco_final

pedidos['preço_unitário'] = pedidos.apply(aplicar_icms_compra, axis=1)

def calcular_valor_total_item(row):
    if row['status'] == 'Cancelado':
        return 0
    else:
        return row['quantidade'] * row['preço_unitário']

def formatar_cnpj(cnpj):

    cnpj = re.sub(r'\D', '', cnpj); cnpj = cnpj.zfill(14); cnpj_formatado = '{}.{}.{}/{}-{}'.format(cnpj[:2], cnpj[2:5], cnpj[5:8], cnpj[8:12], cnpj[12:])

    return cnpj_formatado

hospitais_cadastro['cnpj'] = hospitais_cadastro['cnpj'] .apply(formatar_cnpj)
laboratorio_cadastro['cnpj'] = laboratorio_cadastro['cnpj'] .apply(formatar_cnpj)
pedidos_duplicados['cnpj'] = pedidos_duplicados['cnpj'].apply(formatar_cnpj)
pedidos['cnpj'] = pedidos['cnpj'].apply(formatar_cnpj)
    
pedidos['preço_total'] = pedidos.apply(calcular_valor_total_item, axis=1); pedidos.drop(columns=['EAN 1', 'PREÇO MÉDIO'], inplace=True)
pedidos.info(); print(); pedidos; pedidos.sort_values('nr_pedido').info(); print(); pedidos.sort_values('nr_pedido').head()
