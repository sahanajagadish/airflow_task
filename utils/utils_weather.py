import os
import csv
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
CITY = os.getenv("CITY")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")

def fetch_historical_weather(**kwargs):
    """Fetch historical weather data from Visual Crossing API."""
    params = kwargs.get("params", {})
    start_date = params.get("start_date")
    end_date = params.get("end_date")

    if not start_date or not end_date:
        raise ValueError("Start and end dates must be provided")

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{CITY}/{start_date}/{end_date}?unitGroup=metric&include=days&key={API_KEY}&contentType=json"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json().get("days", [])
    else:
        raise Exception(f"Failed to fetch weather data: {response.text}")

def store_historical_weather(**kwargs):
    """Store fetched weather data into the PostgreSQL database."""
    data = fetch_historical_weather(**kwargs)
    if not data:
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    upsert_query = """
    INSERT INTO weatherhistorical (city, temperature, humidity, condition, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (timestamp)
    DO UPDATE SET
        temperature = EXCLUDED.temperature,
        humidity = EXCLUDED.humidity,
        condition = EXCLUDED.condition;
    """

    for entry in data:
        cursor.execute(upsert_query, (
            CITY,
            entry.get("temp"),
            entry.get("humidity"),
            entry.get("conditions"),
            entry.get("datetime")
        ))

    conn.commit()
    cursor.close()
    conn.close()

def export_weather_data_to_csv(**kwargs):
    """Export weather data from PostgreSQL to a CSV file."""
    params = kwargs.get("params", {})
    start_date = params.get("start_date")
    end_date = params.get("end_date")

    if not start_date or not end_date:
        raise ValueError("Start and end dates must be provided")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT city, temperature, humidity, condition, timestamp
        FROM weatherhistorical
        WHERE timestamp BETWEEN %s AND %s;
    """, (start_date, end_date))

    rows = cursor.fetchall()
    if not rows:
        return

    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
    with open(CSV_FILE_PATH, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["City", "Temperature", "Humidity", "Condition", "Timestamp"])
        writer.writerows(rows)

    cursor.close()
    conn.close()
