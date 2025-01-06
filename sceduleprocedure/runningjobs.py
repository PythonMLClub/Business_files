import os
import snowflake.connector
import time
import csv
from datetime import datetime, timedelta
from collections import defaultdict

import csv
from collections import defaultdict

# Provide Snowflake account credentials
user = "Denny3"
password = "Dhanu@10299"
account = "phvecuk-xh45395"
warehouse = "COMPUTE_WH"
database = "DB_STREAMLIT"
schema = "SC_STREAMLIT"

def init_connection(user, password, account, warehouse, database, schema):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    return conn

def extract_highest_times(file_path):
    unique_times = defaultdict(str)

    with open(file_path, 'r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            id = row['id']
            time = row['controlid']
            if time > unique_times[id]:
                unique_times[id] = time

    return unique_times




def write_control_csv(unique_times, pending_jobs_file, control_file):
    with open(pending_jobs_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        with open(control_file, 'w', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if row['id'] in unique_times and row['controlid'] == unique_times[row['id']]:
                    writer.writerow(row)

def fetch_jobs(control_file):
    jobs = []
    with open(control_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            jobs.append(row)
    return jobs

def read_jobs_from_csv():
    with open('control.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        return list(reader)

def parse_interval(time_str):
    hours, minutes = map(int, time_str.split(':'))
    return hours * 60 + minutes

def check_and_run_jobs(jobs, conn):
    current_time = datetime.now()
    day_of_week = current_time.weekday()
    day_of_month = current_time.day
    current_minutes = current_time.hour * 60 + current_time.minute

    pending_jobs_found = False  # Flag to track if any pending jobs are found

    for job in jobs:
        id, job_name, job_frequency, day_of_week, day_of_month, daily_time, proc_to_call, lastrun_datetime, controlid = job

        lastrun_datetime = datetime.strptime(lastrun_datetime, "%Y-%m-%d %H:%M:%S.%f")
        
        # Calculate the time difference in minutes since the last run
        time_since_last_run = (current_time - lastrun_datetime).total_seconds() / 60

        # Check if it's been more than 1 minute since the last run
        if time_since_last_run > 1:
            if job_frequency == 'daily':
                interval = parse_interval(daily_time)
                if current_minutes % interval == 0:
                    run_job(proc_to_call, conn)
                    sch_metatable(id, conn)
                    pending_jobs_found = True  # Set flag as pending job found

            elif (job_frequency == 'weekly' and day_of_week == day_of_week and daily_time == current_time.strftime('%H:%M')) or \
                 (job_frequency == 'monthly' and day_of_month == day_of_month and daily_time == current_time.strftime('%H:%M')):
                run_job(proc_to_call, conn)
                sch_metatable(id, conn)
                pending_jobs_found = True  # Set flag as pending job found

    # If no pending jobs found, print the message
    if not pending_jobs_found:
        print("No pending jobs found at the current time.")


def run_job(proc_to_call, conn):
    try:
        with conn.cursor() as cur:
            call_statement = f"CALL {proc_to_call}()"
            print(call_statement)
            cur.execute(call_statement)
            print(f"Successfully ran job: {proc_to_call}")

           
    except Exception as e:
        print(f"Error running job {proc_to_call}: {e}")

def sch_metatable(id,conn):
    try:
        with conn.cursor() as cur:
            # Update lastruntime column in SCH_METADATA table
            update_metadata_query = "UPDATE SCH_METADATA SET LASTRUNDATETIME = CURRENT_TIMESTAMP WHERE id = %s"
            print(update_metadata_query)
            cur.execute(update_metadata_query, (id,))   
    except Exception as e:
        print(f"Error updating metadata : {e}")

if __name__ == "__main__":
    pending_jobs_file = 'pendingjobs.csv'
    control_file = 'control.csv'
    refresh_interval_hrs = 1  # Example interval, adjust as needed

    conn = init_connection(user, password, account, warehouse, database, schema)

    unique_times = extract_highest_times(pending_jobs_file)
    write_control_csv(unique_times, pending_jobs_file, control_file)

    try:
        while True:
            jobs = fetch_jobs(control_file)
            check_and_run_jobs(jobs, conn)
            time.sleep(10)

    except KeyboardInterrupt:
        print("Script stopped by user")

    finally:
        conn.close()