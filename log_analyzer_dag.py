from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
from pathlib import Path

# Create Airflow DAG:

default_args = {
    "start_date" : dt.datetime(2021, 8, 21),     
    "retries" : 2,                                  # Retry twice
    "retry_delay" : dt.timedelta(minutes=5),        # Retry with a 5-minute interval
}

dag = DAG(
    dag_id = "log_analyzer_dag",
    default_args = default_args,
    description = "Analyze logs from DAG marketvol2 runs",      
    schedule_interval = "0 20 * * 1-5",             # Run daily, from Monday to Friday, at 08:00 PM
    catchup=False
)

def analyze_file():
    '''Create generator of files in Airflow logs subdirectory and prints the count of errors found on log files
    and list of errors.
    '''
    # Get generator of files included in Airflow logs subdirectory
    log_dir = '/usr/local/airflow/logs'
    file_list = Path(log_dir).rglob('*.log')
    
    error_list = []
    error_count = 0
    
    # Iterate over file_list to parse file by file
    for file_path in file_list:
        
        task = str(file_path).replace("/usr/local/airflow/logs/marketvol2/", "")
        file = open(file_path,"r")
          
        # Iterate over file to parse line by line
         
        for line in file:
            if "ERROR" in line:
                error_list.append(task + ": " + line.strip())
                error_count += 1
 
    print(f"Total number of errors: {error_count}")
    print(f"These are the logfiles: errors:")
    for line in error_list:
        print(line)


# Python Operators to call analyze_file function
analyze_log_files = PythonOperator(task_id = "analyze_log_files",
                        python_callable = analyze_file,            
                        dag = dag)