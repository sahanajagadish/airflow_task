import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import StringIO

def fetch_nasa_data(api_key, start_date, end_date):
    print(api_key)
    """Fetches Near-Earth Object (NEO) data from NASA API within a given date range."""
    try:
        url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def process_nasa_data(data):
    """three transformations on the fetched data"""
    try:
        records = []
        for date, neos in data["near_earth_objects"].items():
            for obj in neos:
                # Extract required values
                diameter_min_km = float(obj["estimated_diameter"]["kilometers"]["estimated_diameter_min"])
                diameter_max_km = float(obj["estimated_diameter"]["kilometers"]["estimated_diameter_max"])
                velocity_km_per_s = float(obj["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"])
                miss_distance_km = float(obj["close_approach_data"][0]["miss_distance"]["kilometers"])
                is_potentially_hazardous = obj["is_potentially_hazardous_asteroid"]

                # Calculate mass approximation based on volume (assuming spherical asteroid)
                mass = (4 / 3) * 3.14159 * (diameter_min_km / 2) ** 3
                # Calculate Kinetic Energy (KE = 0.5 * m * v^2)
                kinetic_energy = 0.5 * mass * velocity_km_per_s ** 2

                # Determine risk category based on hazard status and miss distance
                if is_potentially_hazardous or miss_distance_km < 500000:
                    risk_category = "Very High" if miss_distance_km < 500000 else "High"
                else:
                    risk_category = "Low"

                records.append({
                    "id": obj["id"],
                    "name": obj["name"],
                    "close_approach_date": date,
                    "diameter_min_km": diameter_min_km,
                    "diameter_max_km": diameter_max_km,
                    "velocity_km_per_s": velocity_km_per_s,
                    "miss_distance_km": miss_distance_km,
                    "is_potentially_hazardous": is_potentially_hazardous,
                    "kinetic_energy": kinetic_energy,  # Added Kinetic Energy
                    "risk_category": risk_category    # Added Risk Category
                })
        print(f"Processed {len(records)} records.")
        return pd.DataFrame(records)

    except Exception as e:
        print(f"Error processing data: {e}")
        return pd.DataFrame()



def store_in_db(df, conn_id="target_postgres"):
    """Stores processed asteroid data in PostgreSQL database."""
    try:
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        print("connection to{conn_id} db successful")

        # Create the table if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS near_earth_objects (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            close_approach_date DATE,
            diameter_min_km FLOAT,
            diameter_max_km FLOAT,
            velocity_km_per_s FLOAT,
            miss_distance_km FLOAT,
            is_potentially_hazardous BOOLEAN,
            kinetic_energy FLOAT,
            risk_category VARCHAR
        );
        """
        cursor.execute(create_table_query)

        # Prepare the DataFrame for insertion into the table
        output = StringIO()
        df.to_csv(output, index=False, header=False)
        output.seek(0)

        # Copy the data into the PostgreSQL table
        cursor.copy_expert(f"COPY near_earth_objects FROM STDIN WITH CSV", output)
        conn.commit()

        print("Data stored successfully in PostgreSQL")

    except Exception as e:
        print(f"Error storing data: {e}")

    finally:
        cursor.close()
        conn.close()

def export_to_csv(df, file_path):
    """Exports processed asteroid data to a CSV file."""
    try:
        df.to_csv(file_path, index=False)
        print(f"Data exported to {file_path}")
    except Exception as e:
        print(f"Error exporting data: {e}")
