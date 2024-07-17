from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from datetime import datetime
from time import sleep
import pandas as pd 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from sqlalchemy import Table, Column, Integer, String, Date, VARCHAR, CHAR, Numeric, Float, MetaData, Text, Sequence


workflow_name = "Task_1_2"


#------------------------Подключение_к_БД------------------------

engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres', isolation_level="AUTOCOMMIT")
connection = engine.connect()

#----------------------Инициализация_таблиц----------------------

metadataobj_ds1 = MetaData(schema = 'ds')
metadataobj_dm2 = MetaData(schema = 'dm')
metadataobj_logs3 = MetaData(schema = 'logs')

logs_lg_messages_table= Table( 	
    "lg_messages",                       
    metadataobj_logs3,                           
	Column("record_id", Integer, autoincrement = True, primary_key=True), #autoincrement = True: создает sequence
	Column("date_time", Date),
	Column("pid", VARCHAR(50)),
	Column("message", Text),
	Column("message_type",VARCHAR(50)),
	Column("usename",VARCHAR(50)),
	Column("datname", VARCHAR(50)),
	Column("client_addr", VARCHAR(50)),
	Column("application_name", VARCHAR(90)),
	Column("backend_start", Date)
)

ds_account_turnover_f_table = Table(
    "ds_account_turnover_f",
    metadataobj_ds1,
    Column("on_date", Date), 
    Column("account_rk", Numeric), 
    Column("credit_amount", Numeric(23,8)), 
    Column("credit_amount_rub",Numeric(23,8)), 
    Column("debet_amount", Numeric(23,8)), 
    Column("debet_amount_rub", Numeric(23,8))
)

dm_f101_round_f_table = Table(
    "dm_f101_round_f",
    metadataobj_dm2,
    Column("from_date", Date), 
    Column("to_date", Date), 
    Column("chapter", CHAR), 
    Column("ledger_account", VARCHAR(5)), 
    Column("characteristic", CHAR), 
    Column("balance_in_rub", Numeric(23,8)), 
    Column("r_balance_in_rub", Numeric(23,8)), 
    Column("balance_in_val", Numeric(23,8)), 
    Column("r_balance_in_val", Numeric(23,8)), 
    Column("balance_in_total", Numeric(23,8)), 
    Column("r_balance_in_total", Numeric(23,8)), 
    Column("turn_deb_rub", Numeric(23,8)), 
    Column("r_turn_deb_rub", Numeric(23,8)), 
    Column("turn_deb_val", Numeric(23,8)), 
    Column("r_turn_deb_val", Numeric(23,8)), 
    Column("turn_deb_total", Numeric(23,8)), 
    Column("r_turn_deb_total", Numeric(23,8)), 
    Column("turn_cre_rub", Numeric(23,8)), 
    Column("r_turn_cre_rub", Numeric(23,8)), 
    Column("turn_cre_val", Numeric(23,8)), 
    Column("r_turn_cre_val", Numeric(23,8)), 
    Column("turn_cre_total", Numeric(23,8)), 
    Column("r_turn_cre_total", Numeric(23,8)), 
    Column("balance_out_rub", Numeric(23,8)), 
    Column("r_balance_out_rub", Numeric(23,8)), 
    Column("balance_out_val", Numeric(23,8)), 
    Column("r_balance_out_val", Numeric(23,8)), 
    Column("balance_out_total", Numeric(23,8)), 
    Column("r_balance_out_total", Numeric(23,8))
)

#---------------------Функции для логирования#---------------------

from functools import wraps

def start_wf_log():
    if Variable.get("Task_1_2_run_id") == None:
        Variable.set("Task_1_2_run_id", 1)
    else:
        value = int(Variable.get("Task_1_2_run_id")) + 1
        Variable.set("Task_1_2_run_id", value)
        
    row_timestamp = datetime.now()
    wf_name = workflow_name,

    run_id = Variable.get("Task_1_2_run_id")
    
    event = 'start'
    event_date = datetime.now()
    event_datetime = datetime.now()
    
    columns = ['row_timestamp', 'wf_name', 'run_id', 'event', 'event_date', 'event_datetime']
    
    cort = (row_timestamp, wf_name, run_id, event, event_date, event_datetime)

    df_start = pd.DataFrame([cort], columns=columns)
    #sleep(60)
    df_start.to_sql(name='wf_log', con=engine, schema='logs', if_exists='append', index=False)
    

def end_wf_log():

    row_timestamp = datetime.now()
    wf_name = workflow_name,

    run_id = Variable.get("Task_1_2_run_id")
    
    
    event = 'end'
    event_date = datetime.now()
    event_datetime = datetime.now()

    columns = ['row_timestamp', 'wf_name', 'run_id', 'event', 'event_date', 'event_datetime']
    
    cort = (row_timestamp, wf_name, run_id, event, event_date, event_datetime)
    
    df_end = pd.DataFrame([cort], columns=columns)
    df_end.to_sql(name='wf_log', con=engine, schema='logs', if_exists='append', index=False)

def task_log_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        task_date_launche = datetime.today()
        task_name = func.__name__
        run_id = Variable.get("Task_1_2_run_id")
        time_start = datetime.now()
        more_info = 'all_good'
        result = 'SUCCESS!'
        
        try:
            func(*args, **kwargs)
        except Exception as ex:
            result = 'FAIL...'
            more_info = ex
            print(f"ERROR: {ex}")
        sleep(1)
        time_end = datetime.now()
        time_duration = (time_end - time_start).total_seconds()
        
        columns = ['row_timestamp', 'task_name', 'run_id', 'time_start', 'time_end', 'duration_sec', 'result', 'info']
    
        cort = (task_date_launche, task_name, run_id, time_start, time_end, time_duration, result, more_info)

        df_start = pd.DataFrame([cort], columns=columns)
        df_start.to_sql(name='tasks_log', con=engine, schema='logs', if_exists='append', index=False)
    return wrapper       
 
#------------------------Создание_таблиц-------------------------

@task_log_function
def create_logs_lg_messages_table():
    metadataobj_logs3.create_all(engine)	
    
@task_log_function
def create_ds_account_turnover_f_table():
    metadataobj_ds1.create_all(engine)
    
@task_log_function
def create_dm_f101_round_f_table():
    metadataobj_dm2.create_all(engine)


#---------------------Функции запуска процедур---------------------

@task_log_function
def call_turnover_procedure():
    query = "call ds.turnover_loop('2018-01-01')"
    do = connection.execute(text(query))

@task_log_function
def call_f101_procedure():
    query = "call dm.fill_f101_round_f('2018-01-31')"
    do = connection.execute(text(query))


#----------------------Инициализация DAG'a#------------------------
args = {
    'owner': 'Alexandr',
    'start_date':datetime(2024, 7, 15),
    'provide_context':True
}

with DAG(
    dag_id = workflow_name, 
    description='work_with_DS', 
    schedule_interval=None,  
    catchup=False, 
    default_args=args) as dag:

    start_task=PythonOperator(
        task_id='start_task',
        python_callable=start_wf_log
    )

    end_task=PythonOperator(
        task_id='end_task',
        python_callable=end_wf_log
    )

    create_lg_messages_table = PythonOperator(
        task_id = 'create_lg_messages_table',
        python_callable = create_logs_lg_messages_table
    )
    
    create_account_turnover_f_table = PythonOperator(
        task_id = 'create_account_turnover_f_table',
        python_callable = create_ds_account_turnover_f_table
    )
    
    create_f101_round_f_table = PythonOperator(
        task_id = 'create_f101_round_f_table',
        python_callable = create_dm_f101_round_f_table
    )

    call_fill_account_turnover_f_procedure_31 = PythonOperator(
        task_id = 'call_fill_account_turnover_f_procedure',
        python_callable = call_turnover_procedure
    )
    
    call_fill_f101_round_f_procedure = PythonOperator(
        task_id = 'call_fill_f101_round_f_procedure',
        python_callable = call_f101_procedure
    )

    
    start_task >> create_lg_messages_table
    start_task >> create_account_turnover_f_table
    start_task >> create_f101_round_f_table
    create_lg_messages_table >> call_fill_account_turnover_f_procedure_31
    create_account_turnover_f_table >> call_fill_account_turnover_f_procedure_31
    create_f101_round_f_table >> call_fill_account_turnover_f_procedure_31
    call_fill_account_turnover_f_procedure_31 >> call_fill_f101_round_f_procedure
    call_fill_f101_round_f_procedure >> end_task