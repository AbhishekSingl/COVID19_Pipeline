import datetime

dates = []
with open('/usr/local/airflow/dags/config/Script_Execution_Time.txt', 'r') as f:
    for i in f:
        dates.append(i.strip())

print(dates)

