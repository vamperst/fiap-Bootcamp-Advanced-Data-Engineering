# 01 - Glue Catalog

### Criando com CLI

1. No terminal do Cloud9 execute os comandos abaixo para garantir que tem as variaveis necessárias para o exercicio disponiveis no terminal.

```shell
AWS_ACCOUNT_ID=`aws sts get-caller-identity --query Account --output text`
AWS_REGION=`aws configure get region`
BUCKET_NAME=glueworkshop-${AWS_ACCOUNT_ID}-${AWS_REGION}
echo "export BUCKET_NAME=\"${BUCKET_NAME}\"" >> /home/ec2-user/.bashrc
echo "export AWS_REGION=\"${AWS_REGION}\"" >> /home/ec2-user/.bashrc
echo "export AWS_ACCOUNT_ID=\"${AWS_ACCOUNT_ID}\"" >> /home/ec2-user/.bashrc
echo ${BUCKET_NAME}
echo ${AWS_REGION}
echo ${AWS_ACCOUNT_ID}
```

2. Para verificar como é o arquivo de amostra de dados a ser utilizado execute o comando `c9 open ~/environment/glue-workshop/data/lab1/csv/sample.csv`
3. Vamos primeiro criar o banco de dados do Glue catalog a ser utilizado. Para isso execute o comando abaixo no terminal do cloud9:
``` shell
aws glue create-database --database-input "{\"Name\":\"cli_glueworkshop\", \"Description\":\"This database is created using AWS CLI\"}"
```

4. Verifique o banco de dados  criado na página de [serviço do Glue](https://us-east-2.console.aws.amazon.com/glue/home?region=us-east-2#/v2/data-catalog/databases).Você verá um banco de dados com nome **cli_glueworkshop** na seção de bancos de dados do Glue.

    ![](img/cli-glue-db.png)

5. Agora vamos criar o crawler que vai varrer os CSV e Json do lab1 no bucket S3 e criar tabela no banco de dados récem criado. Para tal, execute o comando abaixo:
```shell
aws glue create-crawler \
--name cli-lab1 \
--role AWSGlueServiceRole-glueworkshop \
--database-name cli_glueworkshop \
--table-prefix cli_ \
--targets "{\"S3Targets\": [{\"Path\": \"s3://${BUCKET_NAME}/input/lab1/csv\"}, \
                            {\"Path\": \"s3://${BUCKET_NAME}/input/lab5/json\"} ]}"
```

6. Uma vez criado, é necessário rodar o crawler via CLI. O Crawler vai demorar por volta de 2 minutos para executar. O comando abaixo executa o crawler:

``` shell
aws glue start-crawler --name cli-lab1
```
![](img/cli-glue-db.png)

7. Depois que os crawlers terminarem em execução, você poderá ver os resultados clicando em tabelas à esquerda da página do console Glue. Você deve ver duas novas tabelas criadas pelos rastreadores - ***cli_csv*** e ***cli_json***.
   ![](img/cli-tables.png)

8. Clique na tabela ***cli_csv*** e você verá o esquema da tabela gerado automaticamente pelo crawler com base no arquivo CSV.
   
   ![](img/lab1-5.png)

9. Clique na tabela cli_json e você verá o esquema da tabela gerado automaticamente pelo crawler com base no arquivo json.
    
    ![](img/lab1-6.png)

