#PROJETO WEBSCRAPING CÂMARA DOS DEPUTADOS VIA PHYTON
#Bruno Campos

MIT License
Copyright (c) 2020 Eduardo Luís Bartholomay
# Código modificado por Bruno Campos 

#######################
#baixar no prompt "pip", "bs4", e "pandas" para funcionar

from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from datetime import datetime
import glob
import numpy as np

#Função para processar o conteúdo da página
def process(df, content):    
    new_content = {'Discurso': content.find(align="justify").get_text(),
                  'Sessão': re.search(re.compile("Sessão: ([\w.]+)"),
                      content.find('td', text=re.compile('Sessão:')).get_text().strip()).group(1),
                  'Fase': re.search(re.compile("Fase: ([\w.]+)"),
                      content.find('td', text=re.compile('Fase:')).get_text().strip()).group(1),
                  'Data': re.search(re.compile("Data: ([\w/]+)"),
                      content.find('td', text=re.compile('Data:')).get_text().strip()).group(1)}
    
    df = pd.concat([df, pd.DataFrame([new_content])], ignore_index=True) #Pandas 1.4.0, substituir o append pelo concat, o msm na função vazio
    return df

#Função para adicionar valores nulos
def vazio(df):
    null_content = {'Discurso': None,
                  'Sessão': None,
                  'Fase': None,
                  'Data': None}
    df = pd.concat([df, pd.DataFrame([null_content])], ignore_index=True) #Pandas 1.4.0, substituir o append pelo concat
    return df

# Função principal para acessar cada página com 50 discursos cada, rodar 500 para garantir que todas sejam pegas
def main():
    # Armazena discursos do site da câmara em um único dataframe.
    #base_url = 'https://www.camara.leg.br/internet/sitaqweb/resultadoPesquisaDiscursos.asp?CurrentPage={page_number}&BasePesq=plenario&txIndexacao=&txOrador=&txPartido=&dtInicio=01/01/2023&dtFim=31/12/2023&txUF=&txSessao=&listaTipoSessao=&listaTipoInterv=&inFalaPres=&listaTipoFala=&listaFaseSessao=&txAparteante=&listaEtapa=&CampoOrdenacao=dtSessao&TipoOrdenacao=DESC&PageSize=50&txTexto=&txSumario='
    base_url = 'https://www.camara.leg.br/internet/sitaqweb/resultadoPesquisaDiscursos.asp?txIndexacao=&CurrentPage={page_number}&BasePesq=plenario&txOrador=&txPartido=&dtInicio=01/01/2023&dtFim=31/12/2023&txUF=&txSessao=&listaTipoSessao=&listaTipoInterv=&inFalaPres=&listaTipoFala=&listaFaseSessao=&txAparteante=&listaEtapa=&CampoOrdenacao=dtSessao&TipoOrdenacao=DESC&PageSize=50&txTexto=&txSumario='
    base_link = 'https://www.camara.leg.br/internet/sitaqweb/'
    links = list()
    oradores_tag = []
    oradores=[]
    partidos = []
    pattern = re.compile(r'[A-Za-zÀÁÂÃÇÈÉÊÌÍÒÓÔÕÙÚÛçàáâãçèéêìíòóôõùúû]+(?:[ ]|-|,|(?:das?|dos?|de|e|\(|\)|[A-Za-zÀÁÂÃÇÈÉÊÌÍÒÓÔÕÙÚÛçàáâãçèéêìíòóôõùúû]+))*')
    pattern_p = re.compile('[A-Z]+-(?:[A-Z]+)*')
    df = pd.DataFrame({'Orador': [], 'Partido': [],'Discurso': [], 'Sessão': [], 'Data': [], 'Fase': [], 'Link': []})


    
    for page_number in range(1, 500):
        # Para cada página, adiciona os links dos discursos em `links`
        print(f'Obtendo links: página {page_number}', end='\r')
        site_data = requests.get(base_url.format(page_number=page_number))
        soup = BeautifulSoup(site_data.content, 'html.parser')
        link_tags = soup.find_all('a', href=re.compile('TextoHTML'))
        for tag in link_tags:
            links.append(re.sub(r"\s", "", tag['href']))
        tabela = soup.find('table', class_='table table-bordered variasColunas')
        for row in tabela.findAll("tr"): #para tudo que estiver em <tr>
            cells = row.findAll('td') #variável para encontrar <td>
            if len(cells)==8: #número de colunas
                oradores_tag.append(cells[5].find(text=True)) #iterando sobre cada linha 
    for tag in oradores_tag:
        oradores.append(str(pattern.findall(tag)))
        partidos.append(str(pattern_p.findall(tag)))  
    print()
    
    # Salva os links caso dê algum problema no caminho.
    #with open('links_discursos_2022.txt', 'w') as f:
     #   f.write('\n'.join(links))
        
    n_links = len(links)
    links_erro = list()
    links_discursos = list()
    print(f'Encontrados {n_links} links.')
    print('Extraindo discursos...')
    for n, link in enumerate(links):
        link_data = requests.get(base_link+link)
        content = BeautifulSoup(link_data.content, 'html.parser')
        
        if content:
            try:
                df = process(df, content)
                links_discursos.append(base_link+link)
            except:
            # Salva os links com erro.
                df = vazio(df)
                links_erro.append(base_link+link)
                links_discursos.append(base_link+link)
                print(f'{n+1} discursos com erro.', end='\r')
        else:
            df = vazio(df)
            links_discursos.append(base_link+link)
        print(f'{n+1} discursos de {n_links} extraídos.', end='\r')
    
    with open('links_erro_2023.txt', 'w') as f:
        f.write('\n'.join(links_erro))
    print()
    df['Orador'] = oradores
    df['Partido'] = partidos
    df['Link'] = links_discursos

    return df

if __name__ == '__main__':
    df = main()
    df.to_excel('camara_2023.xlsx', index=False, engine='openpyxl')

#Quando terminar, mudar o ano de interesse no url (linha 33 e 34), e o nome do arquivo ao final "camara_XXXX_.xlsx" para não confundir no diretório final
#2011-2023
#O tratamento dos dados será feito via R
