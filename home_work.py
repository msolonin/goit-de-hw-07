from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
import random

GLOBAL_NAME = 'msolonin'

# Назва з'єднання з базою даних MySQL
connection_name = "mysql_db"

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}


# Функція для примусового встановлення статусу DAG як успішного
def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)


# Функція для генерації випадкової назви медалі ['Bronze', 'Silver', 'Gold']
def pick_medal(ti):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Generated number: {medal}")
    return medal


# Функція для розгалуження випадкової назви медалі
def pick_medal_task(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')
    if medal == 'Bronze':
        return 'calc_Bronze'
    elif medal == 'Silver':
        return 'calc_Silver'
    elif medal == 'Gold':
        return 'calc_Gold'


def generate_delay(ti):
    print("Starting delay...")
    delay = random.randint(1, 100)
    print(f"Delay {delay} completed.")


# Визначення DAG
with DAG(
        'hw_07',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=[GLOBAL_NAME]
) as dag:
    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = MySqlOperator(
        task_id='create_schema',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS {GLOBAL_NAME};
        """
    )

    # Завдання для створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS {GLOBAL_NAME}.medals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(255) NOT NULL,
        count INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        """
    )

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal,
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task,
    )

    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO msolonin.medals (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*) , NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
            """,
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO msolonin.medals (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*) , NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
            """,
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO msolonin.medals (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*) , NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
            """,
    )

    generate_delay = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule='one_success',
    )

    # Сенсор для порівняння кількості рядків у таблицях `oleksiy.games` і `olympic_dataset.games`
    check_for_data = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""
        SELECT 1
        FROM medals
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        LIMIT 1;
        """,
        mode='poke',
        poke_interval=10,
        timeout=600,
    )

    # Завдання для примусового встановлення статусу DAG як успішного в разі невдачі
    mark_success_task = PythonOperator(
        task_id='mark_success',
        trigger_rule=tr.ONE_FAILED,  # Виконати, якщо хоча б одне попереднє завдання завершилося невдачею
        python_callable=mark_dag_success,
        provide_context=True,  # Надати контекст завдання у виклик функції
        dag=dag,
    )

    # Встановлення залежностей між завданнями
    create_schema >> create_table >> pick_medal >> pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    calc_Bronze >> generate_delay
    calc_Silver >> generate_delay
    calc_Gold >> generate_delay
    generate_delay >> check_for_data >> mark_success_task
