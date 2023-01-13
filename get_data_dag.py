import os
from yaml import safe_load
from airflow.models import DAG
from datetime import date

from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.bash_operator import BashOperator
import json

owner = 'airflow'
interval = "30 5 * * *"
now = date.today()
date_data = now.strftime("%Y%m%d")
endpoint_sales = '?Start=' + date_data + '&End_Date=' + date_data + ''

default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'schedule_interval': interval,
    'catchup': False,
}

dag = DAG(
    dag_id='get_files_from_api',
    schedule_interval=interval,
    start_date=days_ago(1),
    default_args=default_args
)

start = EmptyOperator(
    dag=dag,
    task_id="start"
)
end = EmptyOperator(
    dag=dag,
    task_id="end"
)

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

for file in os.listdir(f'{CUR_DIR}/config_files/'):

    # Достаём из папки с конфигами все файлы и на каждый запускаем ветку тасков, как и раньше
    with open(f'{CUR_DIR}/config_files/{file}') as stream:
        config = safe_load(stream)

    test_http_connect = HttpSensor(
        task_id=f'{config["file_name"]}test_http_connect',
        # Вместо явного указания коннекшна здесь и дальше тащим его из конфига
        http_conn_id=config["conn_id"],
        endpoint=endpoint_sales,
        request_params={},
        poke_interval=5,
        dag=dag,
    )

    take_data = SimpleHttpOperator(
        task_id=f'{config["file_name"]}take_data',
        http_conn_id=config["conn_id"],
        endpoint=endpoint_sales,
        method='GET',
        response_filter=lambda response: json.loads(response.text),
    )


    def save_json_file(ti) -> None:
        posts = ti.xcom_pull(task_ids=['take_data'])
        # Здесь и дальше явное упоминание файла "Provider_TYPE" заменил на упоминание конфига
        with open(f'/opt/airflow/JSON_Files/{config["file_name"]}' + date_data + '.json', 'w') as f:
            json.dump(posts[0], f, indent=4, ensure_ascii=False)


    put_file = SFTPOperator(
        task_id=f"{config['file_name']}put_file",
        ssh_conn_id="sftp_cnt",
        local_filepath=f"/opt/airflow/JSON_Files/{config['file_name']}" + date_data + ".json",
        remote_filepath=f"/home/cntadmin/{config['file_name']}" + date_data + ".json",
        operation="put",
        create_intermediate_dirs=True,
        dag=dag,
    )


    def move_file():
        sftp = SSHHook(ssh_conn_id="sftp_cnt")
        sftp_client = sftp.get_conn()
        sftp_client.exec_command(
            f'sudo mv /home/cntadmin/{config["file_name"]}' +
            date_data + f'.json /u01/SFTP/{config["path"]}{config["file_name"]}'
            + date_data + '.json')


    delete_local_file = BashOperator(
        task_id=f'{config["file_name"]}bash_task',
        bash_command=f'find /opt/airflow/JSON_Files -type f -name ""*{config["file_name"]}*"" -delete',
        dag=dag,
    )

    save_file = PythonOperator(task_id=f'{config["file_name"]}save_file', python_callable=save_json_file, dag=dag, )
    move_file = PythonOperator(task_id=f'{config["file_name"]}move_file', python_callable=move_file, dag=dag, )

    start >> test_http_connect >> take_data >> save_file >> put_file >> move_file >> delete_local_file >> end
