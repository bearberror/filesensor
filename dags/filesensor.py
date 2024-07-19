from airflow.decorators import task, dag
from airflow.models.baseoperator import chain
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor


@task(task_id="runafter", trigger_rule="none_skipped")
def run_after_sensor():
    print("This line should be run after filesensor found")


@task(task_id="print_file", trigger_rule="all_done")
def print_file():
    import pandas as pd

    file_path = "/opt/airflow/source/test.csv"

    data = pd.read_csv(file_path)
    print(data.head())


@task.bash(task_id="moving_file", trigger_rule="all_done")
def mv_file():
    import pendulum

    now = pendulum.DateTime.now().strftime(format="%Y-%m-%d_%H_%M")
    return f'mv /opt/airflow/source/test.csv /opt/airflow/target/test{now}.csv && echo "Move Succesful"'


@task.branch(task_id="check_file_3_version")
def branching():
    import os

    num_file = len(os.listdir("/opt/airflow/target"))

    if num_file == 3:
        return "remove_oldest_file"
    else:
        return "continue_without_remove"


def get_oldest_file():
    import os

    create_time_list = []
    root, _, files = os.walk("/opt/airflow/target").__next__()
    for file in files:
        filepath = f"{root}/{file}"
        create_time = os.path.getctime(filepath)
        create_time_list.append(create_time)
    oldets_file = sorted(
        dict(zip(files, create_time_list)).items(), key=lambda x: x[1], reverse=True
    )[-1][0]

    return f"{root}/{oldets_file}"


@task.bash(task_id="remove_oldest_file")
def remove_oldest_file(filename):
    return f"rm {filename}"


@dag(
    dag_id="test_file_sensor",
    start_date=pendulum.datetime(2024, 7, 19),
    schedule_interval="0 * * * *",
    tags=["sensor"],
    catchup=False,
)
def test_file_sensor():
    start = EmptyOperator(task_id="start")
    sensor = FileSensor(
        task_id="sensor",
        fs_conn_id="test_filesensor",
        filepath="/opt/airflow/source/test.csv",
        soft_fail=True,
        poke_interval=30,
        timeout=30,
        mode="poke",
    )
    continue_without_remove = EmptyOperator(
        task_id="continue_without_remove", trigger_rule="all_success"
    )
    # continue_with_remove = EmptyOperator(
    #     task_id="continue_with_remove", trigger_rule="all_success"
    # # )
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    oldest_file_name = get_oldest_file()
    # branch = branching()
    # remove = remove_oldest_file(oldest_file_name)

    (
        start
        >> sensor
        >> run_after_sensor()
        >> branching()
        >> [
            continue_without_remove,
            remove_oldest_file(oldest_file_name),
        ]
        >> print_file()
        >> mv_file()
        >> end
    )
    start >> sensor >> end
    # start >> oldest_file_name >> remove_oldest_file(oldest_file_name)


test_file_sensor()
