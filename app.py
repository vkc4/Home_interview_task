from flask import Flask, request, jsonify
import pandas as pd
from pytz import timezone
from dateutil.parser import parse
from datetime import datetime, timedelta
import random
import string

app = Flask(__name__)

# Placeholder for storing report generation status
report_status = {}

# Function to load data from CSV files
def load_data():
    store_activity_data = pd.read_csv('store_status.csv')
    business_hours_data = pd.read_csv('business_hours.csv')
    timezone_data = pd.read_csv('timezones.csv')
    return store_activity_data, business_hours_data, timezone_data

store_activity_data, business_hours_data, timezone_data = load_data()

# Helper function to convert UTC time to local time
def convert_utc_to_local(utc_time, timezone_str):
    try:
        # Attempt to parse the UTC time string
        utc_time_cleaned = utc_time.split('.')[0]  # Remove any milliseconds
        utc_time_parsed = parse(utc_time_cleaned)

        # Convert to local time
        local_timezone = timezone(timezone_str)
        local_time = utc_time_parsed.astimezone(local_timezone)
        return local_time
    except Exception as e:
        raise ValueError("Error converting UTC to local time: " + str(e))

# Helper function to calculate uptime and downtime
def calculate_uptime_downtime(store_id, end_time):
    store_business_hours = business_hours_data[business_hours_data['store_id'] == store_id]
    if store_business_hours.empty:
        return timedelta(hours=24), timedelta(0)

    uptime_last_hour = timedelta(0)
    downtime_last_hour = timedelta(0)
    uptime_last_day = timedelta(0)
    downtime_last_day = timedelta(0)
    uptime_last_week = timedelta(0)
    downtime_last_week = timedelta(0)

    for index, row in store_business_hours.iterrows():
        start_time_local = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
        end_time_local = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
        start_datetime = datetime.combine(end_time.date(), start_time_local)
        end_datetime = datetime.combine(end_time.date(), end_time_local)
        business_duration = end_datetime - start_datetime
        uptime_last_hour += min(end_time - max(start_datetime, end_time - timedelta(hours=1)), business_duration)
        downtime_last_hour += max(min(end_time - start_datetime, business_duration) - uptime_last_hour, timedelta(0))

        uptime_last_day += min(end_time - max(start_datetime, end_time - timedelta(days=1)), business_duration)
        downtime_last_day += max(min(end_time - start_datetime, business_duration) - uptime_last_day, timedelta(0))

        uptime_last_week += min(end_time - max(start_datetime, end_time - timedelta(weeks=1)), business_duration)
        downtime_last_week += max(min(end_time - start_datetime, business_duration) - uptime_last_week, timedelta(0))

    return uptime_last_hour.total_seconds() / 60, uptime_last_day.total_seconds() / 3600, uptime_last_week.total_seconds() / 3600, \
           downtime_last_hour.total_seconds() / 60, downtime_last_day.total_seconds() / 3600, downtime_last_week.total_seconds() / 3600

# Define an API endpoint to trigger report generation
@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    try:
        # Generate a random report_id
        report_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

        # Start report generation process (in this case, simply mark as 'Running')
        report_status[report_id] = 'Running'

        # Here you would typically initiate the report generation process, possibly in a separate thread or asynchronously

        return jsonify({"report_id": report_id})
    except Exception as e:
        return jsonify({"error": str(e)})

# Define an API endpoint to retrieve report status or CSV
@app.route('/get_report', methods=['GET'])
def get_report():
    try:
        report_id = request.args.get('report_id')

        # Check if report_id exists
        if report_id not in report_status:
            return jsonify({"error": "Invalid report_id"})

        # Check if report generation is complete
        if report_status[report_id] == 'Running':
            return jsonify({"status": "Running"})
        elif report_status[report_id] == 'Complete':
            # Generate report based on stored data
            report_data = []

            # Get current timestamp as the end_time
            end_time = datetime.now()

            # Loop through each store
            for store_id in store_activity_data['store_id'].unique():
                # Get the store's timezone
                store_timezone = "America/Chicago"  # Default if not found
                timezone_info = timezone_data[timezone_data['store_id'] == store_id]
                if not timezone_info.empty:
                    store_timezone = timezone_info.iloc[0]['timezone_str']

                # Filter store activity for the specified store
                store_activity = store_activity_data[store_activity_data['store_id'] == store_id]

                # Convert timestamps to local time
                local_store_activity = []
                for index, row in store_activity.iterrows():
                    local_time = convert_utc_to_local(row['timestamp_utc'], store_timezone)
                    local_store_activity.append({"start_time_local": local_time.strftime('%Y-%m-%d %H:%M:%S'), "status": row['status']})

                # Calculate uptime and downtime
                uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week = \
                    calculate_uptime_downtime(store_id, end_time)

                report_data.append({
                    "store_id": str(store_id),
                    "uptime_last_hour": int(uptime_last_hour),
                    "uptime_last_day": uptime_last_day,
                    "uptime_last_week": uptime_last_week,
                    "downtime_last_hour": int(downtime_last_hour),
                    "downtime_last_day": downtime_last_day,
                    "downtime_last_week": downtime_last_week
                })

            # Convert report_data to CSV format
            report_df = pd.DataFrame(report_data)
            csv_data = report_df.to_csv(index=False)

            # Return complete status along with CSV data
            return jsonify({"status": "Complete", "csv_data": csv_data})
        else:
            return jsonify({"error": "Unknown report status"})
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(debug=True)
