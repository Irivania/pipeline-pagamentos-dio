# Pipeline de Streaming de Pagamentos em Tempo Real com Confluent Cloud

Este repositório contém a implementação prática de um pipeline de dados de ponta a ponta construído no **Confluent Cloud**, cobrindo desde a ingestão via Change Data Capture (CDC) a partir de um banco de dados relacional até ao processamento analítico em tempo real com Flink SQL.

O projeto faz parte do desafio prático de arquitetura de dados em streaming, evidenciando o caminho completo do dado (*do banco de dados até o resultado analítico*).

---

## 🏗️ Arquitetura do Caminho dos Dados

O pipeline é composto por 5 camadas fundamentais:
1. **Fundação:** Criação de ambiente, clusters e identidades de acesso na Confluent Cloud.
2. **Contrato (Schema Registry):** Registo automático dos contratos de dados (JSON/Avro) gerados pelo conector.
3. **Ingestão (CDC):** Conector Debezium ligado a um PostgreSQL (hospedado no Neon), capturando inserções, atualizações e exclusões em tempo real.
4. **Processamento (Flink SQL):** Consultas de streaming e agregações analíticas executadas diretamente sobre os tópicos Kafka.
5. **Operação e Monitorização:** Validação de métricas, estados de execução e custos operacionais.

---

## 📸 Evidências de Execução por Camada

### 1. Camada de Ingestão (PostgreSQL & CDC)
Os dados nascem através de operações `INSERT` executadas no banco de dados relacional PostgreSQL (Neon), que são capturadas instantaneamente pelo conector de CDC.

* **Evidência:** Execução de um registo de pagamento na base de dados de origem.
> ![Inserção no Neon](./evidencias/Anota%C3%A7%C3%A3o%202026-10-16%20124513.png)

---

### 2. Camada de Processamento (Flink Statements)
As declarações Flink SQL foram configuradas e colocadas em execução para monitorizar e processar o fluxo contínuo de eventos provenientes do Kafka.

* **Evidência:** Estado das declarações no painel do Flink (`Running`).
> ![Flink Statements Running](./evidencias/Anota%C3%A7%C3%A3o%202026-10-16%20124945.png)

---

### 3. Camada Analítica (Flink SQL Workspace)
Processamento em tempo real dos pagamentos, realizando agregações e agrupamentos dinâmicos sobre o stream de dados.

* **Evidência:** Consulta Flink SQL agregando os dados por `status` com sucesso.
> ![Flink SQL Resultado](./evidencias/Anota%C3%A7%C3%A3o%202026-10-16%20130924.png)

---

## ⚙️ Como Subir o Pipeline do Zero

1. **Origem (Neon PostgreSQL):**
   - Cria uma tabela de pagamentos no teu cluster PostgreSQL:
     ```sql
     CREATE TABLE pagamentos (
       id SERIAL PRIMARY KEY,
       valor DECIMAL(10, 2),
       status VARCHAR(50),
       data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     );
     ```

2. **Ingestão (Confluent Cloud):**
   - Configura e ativa o conector **Debezium PostgreSQL Source** apontando para o teu banco de dados.

3. **Processamento (Flink SQL):**
   - No Workspace do Flink SQL na Confluent Cloud, cria uma nova declaração e executa a consulta analítica sobre o tópico gerado pelo CDC:
     ```sql
     SELECT
       status,
       COUNT(*) AS total_transacoes
     FROM `db-pagamentos.public.pagamentos`
     GROUP BY status;
     ```

---

## 🛑 Como Desligar Tudo no Fim
* **No Confluent Cloud:** Para evitar custos adicionais, para (*Stop*) todas as declarações ativas no Flink SQL Workspace e pausa ou apaga o conector CDC nos *Connectors*.
* **Na Base de Dados:** Podes suspender a computação do projeto no Neon se aplicável.

---

## 💰 Custos de Execução
* O projeto foi executado integralmente utilizando os créditos de teste e recursos gratuitos/essenciais disponíveis na **Confluent Cloud** e no plano gratuito do **Neon PostgreSQL**, resultando num custo operacional direto de **$0.00** durante os testes de desenvolvimento.

