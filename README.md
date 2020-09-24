COVID19 Pipeline
---

Here, we've built an end-to-end ML pipeline where we're extracting data from Twitter API on daily basis and some news website. Initially, we're using this pipeline to store data localy but later moved to Azure server. So, you can store and process data either locally or azure database. Just change the config template in **src/config** folder

## Setup
It's easy to setup COVID19 Pipeline which we can setup either locally or on Azure Server.
1. sh run_pipeline
2. import json file from **dags/covid19_dag/config/arguments_parsing.json** into Airflow environment.
3. Change the variables according to your requirements.
4. Trigger the dag
https://github.com/AbhishekSingl/COVID19_Pipeline/blob/master/dags/COVID19_Dag.png

## Tools Used:
- Airflow: Workflow management for ETL pipeline
- Azure Server: Azure Database Storage and Virtual Machine
- NRC: Emotion Lexicons for Sentiment Analysis

## Data:
- Twitter API
- Covid Cases, Deaths, and Recovered Info (Atlantic) 
- DOD Actions taken in favour of COVID19 (DOD)

## Resources

| SNO |      References       |
|------|-----------------------|
| 1    | [Apache Airflow Docker Image](https://github.com/puckel/docker-airflow) |
| 2    | [Airflow Learn Resource](https://www.applydatascience.com/airflow/airflow-tutorial-introduction/) |
| 3    | [Azure Storage with Python](https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python) |

## Please feel to try out it and kindly raise an issue if you face any problem in its execution.
