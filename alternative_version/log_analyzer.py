from pathlib import Path

# Get generator of files included in Airflow logs subdirectory
log_dir = '/usr/local/airflow/logs'
file_list = Path(log_dir).rglob('*.log')


def analyze_file(file_list):
    '''Read generator of files in Airflow logs subdirectory and returns the count of errors found on log files
    and list of errors.
            
    Args:
        file_list (generator object): Files in airflow logs subdirectory
    
    Returns:
        Int, List
    '''

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
 
    return error_count, error_list

count, cur_list = analyze_file(file_list)

print(f"Total number of errors: {count}")
print(f"These are the errors:")
for line in cur_list:
    print(line)

