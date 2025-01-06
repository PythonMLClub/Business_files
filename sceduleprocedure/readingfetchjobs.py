import snowflake.connector
import csv
import datetime
import time


def clean_pending_jobs():
    current_time = datetime.datetime.now()
    if current_time.hour == 23 and current_time.minute == 59:  # Check if it's 16:45
        with open('pendingjobs.csv', 'w', newline='') as file:
            file.truncate()  # Clear the contents of the file
        print("pendingjobs.csv cleaned successfully.")

def parse_interval(time_str):
    return datetime.datetime.strptime(time_str, '%H:%M')


def fetch_data():
    try:
        while True:
            current_time = datetime.datetime.now().strftime('%H:%M')  # Get current system time

            # Read fetchjobs.csv and update the daily_time column to the current time
            with open('fetchjobs.csv', 'r+', newline='') as fetch_file:
                fetch_reader = csv.reader(fetch_file)
                header = next(fetch_reader)  # Read header row
                rows = list(fetch_reader)
                for row in rows:
                    row[5] = current_time  # Update daily_time column to current time
                fetch_file.seek(0)  # Move cursor to the beginning of the file
                writer = csv.writer(fetch_file)
                writer.writerow(header)  # Write header row
                writer.writerows(rows)  # Write updated rows
                fetch_file.truncate()  # Truncate any remaining data

            # Clean pendingjobs.csv at 23:59 every night
            clean_pending_jobs()

            # Wait for a minute before checking again
            time.sleep(10)


            # Check fetchjobs.csv for updates
            with open('fetchjobs.csv', 'r', newline='') as fetch_file:
                fetch_reader = csv.reader(fetch_file)
                next(fetch_reader)  # Skip the header row
                fetch_rows = list(fetch_reader)

            current_time = datetime.datetime.now().strftime('%H:%M')  # Get current system time

            pending_rows = []
            for row in fetch_rows:
                # Convert daily_time from float to time format
                daily_time = parse_interval(row[5]).strftime('%H:%M')
                if daily_time == current_time:
                    if not row[7] or (row[7] and datetime.datetime.strptime(row[7], '%Y-%m-%d %H:%M:%S.%f') > datetime.datetime(1, 1, 1)):
                        pending_rows.append(row)

            if pending_rows:
                with open('pendingjobs.csv', 'a', newline='') as stored_file:
                    writer = csv.writer(stored_file)
                    # Write header row only if the file is empty
                    if stored_file.tell() == 0:
                        writer.writerow(['id', 'job_name', 'job_frequency', 'day_of_week', 'day_of_month', 'daily_time', 'proc_to_call', 'lastrun_datetime', 'controlid'])
                    for row in pending_rows:
                        # Get the current system time with seconds
                        current_time_with_seconds = datetime.datetime.now().strftime('%H:%M:%S')
                        # Append current_time_with_seconds as controlid
                        row.append(current_time_with_seconds)
                        writer.writerow(row)

                print("pendingjobs.csv updated with pending jobs", pending_rows)
            else:
                print("No pending jobs found at the current time.")



    except Exception as e:
        print("Error:", e)


fetch_data()