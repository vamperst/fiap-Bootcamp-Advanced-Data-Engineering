# 01 - Glue Studio

### Transformações Básicas


1. Abra o menu [Glue Studio](https://console.aws.amazon.com/gluestudio/home) à esquerda e clique em **Jobs**.
2. Em **Create job**, selecione **Visual with a blank canvas**, e clique **Create**.

![](img/lab6-1-1.png)

3. Renomeie o job para `glueworkshop-lab5-basic-job`. Agora você tem um editor de trabalho visual em branco do Studio.

![](img/lab6-1-2.png)

4. Clique na guia Script, você deve ver editor vazio para o script do Glue ETL. À medida que adicionamos novas etapas no editor visual, o script será atualizado automaticamente.

![](img/lab6-1-3.png)

5. Clique na aba **Job Details** para ver todas as configurações do job.
    - IAM role: `AWSGlueServiceRole-glueworkshop`
    - Glue version: `Glue 3.0 - Support Spark 3.1, Scala 2, Python 3`
    - Requested number of workers: `4`
    - Job bookmark: `Disable`
    - Number of retries: `0`

![](img/lab6-1-4.png)

6. Se houver um botão no topo da tela `Try new UI`, desabilite ele para voltar ao editor visual a versão dos prints.
7. Clique na aba **Visual** novamente para voltar ao Editor Visual. Você deve ver 3 botões: source, action e target, clique em **action**. Você notará que existem ações prontas e ações personalizadas. O Glue Studio foi projetado para ser usado por desenvolvedores que poderiam escrever código Apache Spark, Glue e SQL personalizado, mas também fornece ações comuns prontas. Nesta parte do laboratório, você entenderá como usar ações básicas no Glue Studio.

![](img/lab6-1-5.png)

8. Clique em **Source** e selecione **AWS Glue Data Catalog**
   - Clique em aba **Data source properties - Data Catalog**
     - Database: `console_glueworkshop`
     - Table: `console_json`
   - Clique na aba **Node properties**
     - Name: `COVID data`
> **Observação**: Este é o conjunto de dados Covid-19 que carregamos em passos anteriores

![](img/lab6-1-6.png)

9. Clique em **Action** e selecione **Drop Fields**. Na guia **Transform**, selecione todos os campos, exceto os seguintes, mostrados na tela, verificando a caixa de seleção ao lado do nome do arquivo para soltar esses campos.Ou, em outras palavras, mantenha (desmarcado) os campos mencionados abaixo.
    - date
    - state
    - deathincrease
    - hospitalizeincrease

![](img/lab6-1-7.png)

10. Clique na guia **Data Preview** e clique em **Start data preview session**, escolha `AWSGlueServiceRole-glueworkshop` e clique em **Confirm**. Aguarde a sessão de visualização de dados ficar pronta e clique no botão **Preview 4 de 9 fields**.Você poderá visualizar a saída do nó de ação atual. Em qualquer etapa de ação, você pode visualizar o resultado da etapa indo para a guia **Data Preview**.

![](img/lab6-1-8.png)

11. Clique em **Action** e selecione **Filter**. Na guia **Transform**, selecione `Global AND` clicando no radio button e adicione **Filter conditions** com:
   - **Key**: `state` **Operation**: `matches` **Value**: `NY|CA` e clique em `Save`

![](img/lab6-1-9.png)

12. Clique no **Action** e selecione **Change Schema**. Na guia **Tansform**:
   - Mude o tipo do campo `deathincrease`  para `int`
   - Mude o tipo do campo `hospitalizedincrease`  para `int`

![](img/lab6-1-10.png)

13. Clique em **Source** e selecione **S3**:
    - Clique na guia **Data source properties - S3**
      - S3 source type: `S3 location`
      - S3 URL: s3://${BUCKET_NAME}/input/lab5/state/
      - Clique no botão **Infer schema** no final.
    - Clique na  gui **Node properties**
      - Name: `State Name`
      - Clique em **Save**

![](img/lab6-1-11.png) 

14. Clique na caixa de ação **Change Schema** na tela para destacá -la, clique em **Action** e selecione **Join**.
    - Clique na aba **Node properties**
      - Em **node parents**, selecione `State Name` Clicando na caixa de seleção ao lado
    - Clique na aba **Transform**
      - Join type: `Left Join`
      - Em **Join condition** clique em **Add condition**, em **Change Schema** selecione `state`, em **State Name** e selecione `Code`

![](img/lab6-1-12.png)

15. Clique em  **Action** e selecione **Aggregate**. Na aba **Transform**:
    - Fields to group by: `StateName`
    - Clique em **Aggregate another column**
      - Coloque **Field to aggregate**:`deathincrease`, **Aggregation function**: `sum`
      - Coloque **Field to aggregate**:`hospitalizeincrease`, **Aggregation function**: `sum`

![](img/lab6-1-13.png)

16.  Clique em **Target** e selecione **S3**:
    - Na guia **Data target properties - S3**
      - Format: `JSON``
      - Compression Type: `None`
      - S3 Target Location: `s3://${BUCKET_NAME}/output/lab5/basic/`

![](img/lab6-1-14.png)

17. Clique `Save` e então clique em `Run`.
18. Clique na guia **Runs** a tela de design para monitorar o status de execução do job. Aguarde o **Run statuso** atualizar para `Succeeded`.

![](img/lab6-1-15.png)

19. Você pode baixar os arquivos de saída no terminal Cloud9 usando os seguintes comandos e explorá -los dentro da Cloud9.
``` shell
AWS_ACCOUNT_ID=`aws sts get-caller-identity --query Account --output text`
AWS_REGION=`aws configure get region`
BUCKET_NAME=glueworkshop-${AWS_ACCOUNT_ID}-${AWS_REGION}
echo "export BUCKET_NAME=\"${BUCKET_NAME}\"" >> /home/ec2-user/.bashrc
echo "export AWS_REGION=\"${AWS_REGION}\"" >> /home/ec2-user/.bashrc
echo "export AWS_ACCOUNT_ID=\"${AWS_ACCOUNT_ID}\"" >> /home/ec2-user/.bashrc

aws s3 cp s3://${BUCKET_NAME}/output/lab5/ ~/environment/glue-workshop/output/lab5 --recursive
```
