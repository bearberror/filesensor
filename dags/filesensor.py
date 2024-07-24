from airflow.decorators import task, dag, task_group
from airflow.models.baseoperator import chain
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor


@task(task_id="runafter", trigger_rule="none_skipped")
def run_after_sensor():
    print("This line should be run after filesensor found")


@task(task_id="print_file", trigger_rule="one_success")
def print_file():
    import pandas as pd

    file_path = "/opt/airflow/source/test.csv"

    data = pd.read_csv(file_path)
    print(data.head())


@task.bash(task_id="moving_file", trigger_rule="all_success")
def mv_file():
    import pendulum

    now = pendulum.DateTime.now().strftime(format="%Y-%m-%d_%H_%M")
    return f'mv /opt/airflow/source/test.csv /opt/airflow/target/test{now}.csv && echo "Move Succesful"'


@task.branch(task_id="check_file_3_version")
def branching():
    import os

    num_file = len(os.listdir("/opt/airflow/target"))

    if num_file >= 3:
        return "getFile_and_remove.getOldestFileName"
    else:
        return "continue_without_remove"


@task(task_id="getOldestFileName")
def get_oldest_file():
    import os

    try:
        create_time_dict = {}
        root, _, files = os.walk("/opt/airflow/target").__next__()
        for file in files:
            filepath = f"{root}/{file}"
            create_time = os.path.getctime(filepath)

            create_time_dict[filepath] = create_time
        oldets_file = sorted(
            create_time_dict.items(), key=lambda x: x[1], reverse=True
        )[-1][0]

        return oldets_file
    except Exception as e:
        print(e)


@task.bash(task_id="remove_oldest_file")
def remove_oldest_file(filename):
    return f"echo '{filename}' && rm {filename}"


@task_group(group_id="getFile_and_remove")
def getFile_and_remove():
    oldestFile = get_oldest_file()

    oldestFile >> remove_oldest_file(oldestFile)  # type: ignore


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
        poke_interval=10,
        timeout=100,
        mode="poke",
    )
    continue_without_remove = EmptyOperator(
        task_id="continue_without_remove", trigger_rule="all_success"
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    (
        start
        >> sensor
        >> run_after_sensor()
        >> branching()
        >> [
            getFile_and_remove(),
            continue_without_remove,
        ]  # type: ignore
        >> print_file()
        >> mv_file()
        >> end
    )  # type: ignore
    start >> sensor >> end  # type: ignore


test_file_sensor()
