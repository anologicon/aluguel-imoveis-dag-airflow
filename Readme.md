# aluguel-imoveis-dag-airflow


### Idéia

Entender como está os valores de alugueis de imoveis residênciais em algumas cidades de Santa Catarina, utilizando o site [Zap Imoveis](https://www.zapimoveis.com.br/)
como fonte de dados.

#### Diagrama da arquitetura

![Flow diagram](https://github.com/anologicon/aluguel-imoveis-dag-airflow/blob/master/images/ELTflow.png)

### Execução

Obs.: *A arquitetura foi criada nos serviços do google cloud platform*

- Task 1: executa um operator que faz o scraping no site [Zap Imoveis](https://www.zapimoveis.com.br/), faz um pequeno tratamento, formatando os valores e salva localmente os dados em formato CSV, o scraping são das primeiras 50 paginas de cada cidade.

- Task 2: O operator envia o CSV salvo localmente e persiste em um bucket no google data storage.

- Task 3: O operator salva os dados que estão salvo no storage e salva em uma tabela do BigQuery, a tabela é limpa a cada execução da DAG e persiste os novos dados.

### Link

O link para o google data studio: [Mercado de aluguel de imovel em Santa Catarina: Google Data Studio](https://datastudio.google.com/reporting/ba8747f9-f984-4a89-a80a-68f662a83b1e)