import datetime
import os

print(os.getcwd())
with open('/usr/local/airflow/dags/config/Script_Execution_Time.txt', 'a+') as f:
    f.write(str(datetime.date.today()) + '\n')

