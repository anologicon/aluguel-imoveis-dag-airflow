
# airflow related
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# other packages
from os import environ
import pandas as pd
import urllib3;
from bs4 import BeautifulSoup

# Google storage
import os
from google.cloud import storage

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DataSourceToCsv(BaseOperator):
    """
    Extract data from the data source to CSV file
    """
    @apply_defaults
    def __init__(
            self,
            *args, **kwargs):
        super(DataSourceToCsv, self).__init__(*args, **kwargs)
    
    def __datasource_to_csv(self,context):
        # Scrapper 
        http  = urllib3.PoolManager()
        Valor = []
        TipoAluguel = []
        Iptu = []
        Area = []
        Quartos = []
        Banheiros = []
        Vagas = []
        Uri = []
        Cidade = []
        Endereco = []

        def findTextNull(elementFind):
            elementText = elementFind.text

            if "--" in elementText:
                return 0
            
            return elementText

        for cidade in ('florianopolis', 'brusque', 'blumenau'):

            for page in range(1,30):
                
                http = urllib3.HTTPSConnectionPool('www.zapimoveis.com.br', port=443, cert_reqs='CERT_NONE')       
                
                url = '/aluguel/casas/sc+'+cidade+'/?__zt=srl%3Aa&transacao=Aluguel&tipoUnidade=Residencial,Casa&tipo=Im%C3%B3vel%20usado&pagina='+str(page)

                page = http.request('GET',url)

                soup = BeautifulSoup(page.data.decode('utf-8'),"html.parser")

                alugueis = soup.find_all('div', {'class': 'card-container'})

                for row in alugueis:

                    Cidade.append(cidade)

                    enderecoFind = row.find('p', 'color-dark text-regular simple-card__address')

                    Endereco.append(enderecoFind.text)

                    valor = row.find("p", "simple-card__price js-price heading-regular heading-regular__bolder align-left")

                    TipoAluguel.append(valor.find('small').text.replace('/',''))

                    valor.find('small').decompose()

                    Valor.append(valor.find("strong").text
                        .replace('.','')
                        .replace(' ','')
                        .replace('R$','')
                        )

                    iptuFind = row.find('span', 'card-price__value')

                    if(iptuFind):
                        Iptu.append(iptuFind.text
                            .replace('R$','')
                            .replace('.','')
                            )
                    else :
                        Iptu.append(0)

                    areaFindLi = row.find_all('li', {'class': 'feature__item text-small js-areas'})
                    
                    areaText = 0

                    for li in areaFindLi:
                        areaSpanFind = li.find_all('span')[1]
                        areaText = areaSpanFind.text

                    Area.append(areaText
                        .replace('m²','')
                        .replace(' ', '')
                    )

                    quartosFindLi = row.find_all('li', 'feature__item text-small js-bedrooms')
                    
                    quartosText = 0

                    for li in quartosFindLi:
                        quartosSpanFind = li.find_all('span')[1]
                        quartosText = quartosSpanFind.text
                        quartosText = quartosText.replace(' ', '')

                    Quartos.append(quartosText)

                    vagasFindLi = row.find_all('li', 'feature__item text-small js-parking-spaces')
                    
                    vagasText = 0

                    for li in vagasFindLi:
                        vagasSpanFind = li.find_all('span')[1]
                        vagasText = vagasSpanFind.text
                        vagasText = vagasText.replace(' ', '')

                    Vagas.append(vagasText)

                    banheirosFindLi = row.find_all('li', 'feature__item text-small js-bathrooms')
                    
                    banheirosText = 0

                    for li in banheirosFindLi:
                        banheirosSpanFind = li.find_all('span')[1]
                        banheirosText = banheirosSpanFind.text
                        banheirosText = banheirosText.replace(' ', '')

                    Banheiros.append(banheirosText)

        df=pd.DataFrame(Valor,columns=['Valor'])

        df['TipoAluguel'] = TipoAluguel
        df['Iptu'] = Iptu
        df['Area'] = Area
        df['Quartos'] = Quartos
        df['Banheiros'] = Banheiros
        df['Vagas'] = Vagas
        df['Cidade'] = Cidade
        df['Endereco'] = Endereco

        # End scrapper
        df.to_csv('alugueis.csv', index=False)
    
    def execute(self, context):
        self.__datasource_to_csv(context)

class CsvToStorage(BaseOperator):

    """
    Save CSV into Storage bucket
    """
    @apply_defaults
    def __init__(
            self,
            *args, **kwargs):
            super(CsvToStorage, self).__init__(*args, **kwargs)
    
    def __csv_to_storage(self,context):
        client = storage.Client()
        bucket = client.bucket('aluguel-data-scraper')
        blob = bucket.blob('alugueis.csv')
        blob.upload_from_filename('alugueis.csv')
    
    def execute(self, context):
        self.__csv_to_storage(context)
