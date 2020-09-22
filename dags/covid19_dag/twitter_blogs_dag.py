import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import date, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

tweets_parsing_config = Variable.get("tweets_parsing_variables", deserialize_json=True)
database = tweets_parsing_config["database"]
location = tweets_parsing_config["location"]
tweets = tweets_parsing_config["tweets"]
config_path = tweets_parsing_config['config_path']

tweets_processing_config = Variable.get("tweets_processing_variables", deserialize_json=True)
storage = tweets_processing_config["storage"]
incremental = tweets_processing_config['incremental']
region = tweets_processing_config['region']

aggregation_config = Variable.get("aggregation_variables", deserialize_json=True)
agg_storage = aggregation_config['storage']
agg_incremental = aggregation_config['incremental']

dag = DAG(
    'COVID19',
    default_args=default_args,
    description='Data scraping from twitter and news blogs and cleaning it',
    schedule_interval=timedelta(days=1)
)

twitter = BashOperator(
    task_id='Tweets_Scrapping',
    bash_command=f'Rscript /usr/local/airflow/projects/tweets_scraper.R -d {database} \
                -l "{location}" -n {tweets} -c {config_path}',
    dag=dag
)
#
tweet_cleaning = BashOperator(
    task_id='Tweets_preprocessing',
    bash_command=f'python3 /usr/local/airflow/projects/tweets_processing.py -s {storage} \
                 -i {incremental} -c {config_path} -l "{location}" \
                 -r "{region}"',
    dag=dag
)

aggregation = BashOperator(
    task_id='Day_Level_Aggregation',
    bash_command=f'python3 /usr/local/airflow/projects/day_level_aggregation.py -c {config_path } \
                 -s {agg_storage} -i {agg_incremental}',
    dag=dag
)

# Scraping Covid Cases Deaths Information from news website'
# covid_case_deaths = BashOperator(
#     task_id='Covid_Cases_Deaths',
#     bash_command=f'python3 /usr/local/airflow/projects/covid_cases_deaths_info.py -c {config_path}',
#     dag=dag
# )

# combining = BashOperator(
#     task_id='Combining data from all sources',
#     bash_command='python3 /usr/local/airflow/projects/day_level_aggregation.py',
#     dag=dag
# )

twitter >> tweet_cleaning
tweet_cleaning >> aggregation
# covid_case_deaths >> combining
#