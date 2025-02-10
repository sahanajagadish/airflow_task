import os
import pandas as pd
import re
import logging
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Constants
CSV_FOLDER = Variable.get("CSV_FOLDER")

# Ensure CSV folder exists
if not os.path.exists(CSV_FOLDER):
    raise ValueError(f"CSV folder not found: {CSV_FOLDER}")

def validate_data(df,validation_params):
    """Validates student data in the DataFrame."""
   
    email_regex = Variable.get("email_regex")  
    age_min = int(Variable.get("age_min")) 
    age_max = int(Variable.get("age_max"))  
    date_format = Variable.get("date_format")  

    errors = []
   
    errors += validate_email(df, email_regex)

    errors += validate_age(df, age_min, age_max)

    errors += validate_date_of_joining(df, date_format)

    if errors:
        error_message = "\n".join(errors)
        logging.error(f"Validation Errors:\n{error_message}")
        raise ValueError(f"Validation failed. Check logs for details.\n{error_message}")

    logging.info("Validation passed successfully.")
    return df

def validate_email(df, email_regex):
    """Validate the email format."""
    errors = []
    for index, email in df["email"].items():
        if not isinstance(email, str) or not re.match(email_regex, email):
            errors.append(f"Row {index + 1}: Invalid email '{email}'")
    return errors

def validate_age(df, age_min, age_max):
    """Validate the age is within the specified range."""
    errors = []
    for index, age in df["age"].items():
        try:
            age = int(age)
            if age < age_min or age > age_max:
                errors.append(f"Row {index + 1}: Age {age} out of range ({age_min}-{age_max})")
        except ValueError:
            errors.append(f"Row {index + 1}: Invalid age '{age}' (not a number)")
    return errors

def validate_date_of_joining(df, date_format):
    """Validate the date of joining format."""
    errors = []
    for index, date in df["date_of_joining"].items():
        try:
            pd.to_datetime(date, format=date_format)
        except:
            errors.append(f"Row {index + 1}: Invalid date '{date}', expected format {date_format}")
    return errors

def cleanup_data(df):
    """Cleans student data in the DataFrame."""

   
    df["first_name"] = df["first_name"].str.replace(r"[^a-zA-Z ]", "", regex=True).str.strip().str.title()
    df["last_name"] = df["last_name"].str.replace(r"[^a-zA-Z ]", "", regex=True).str.strip().str.title()


    if 'student_id' in df.columns:
        df = df.drop_duplicates(subset=['student_id'], keep='first')


    df["email"] = df["email"].str.lower()

    logging.info("Data cleanup completed.")
    return df

def get_postgres_data_type(pandas_dtype):
    """Map Pandas data types to PostgreSQL data types."""
    type_mapping = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN'
    }
    return type_mapping.get(str(pandas_dtype), 'TEXT') 

def create_table_if_not_exists(cursor, df, table_name):
    """Create table in PostgreSQL if it doesn't exist based on DataFrame structure."""
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    
  
    for column, dtype in df.dtypes.items():
        postgres_type = get_postgres_data_type(dtype)
        create_table_query += f"{column} {postgres_type}, "
    
    

    create_table_query = create_table_query.rstrip(", ") + ");"
    logging.info(f"Creating table with query: {create_table_query}")
    
    try:
        cursor.execute(create_table_query)
        cursor.connection.commit()
        logging.info(f"Table {table_name} created or already exists.")
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        cursor.connection.rollback()

def insert_into_db(df):
    """Inserts cleaned student data into PostgreSQL using PostgresHook."""
    
 
    hook = PostgresHook(Variable.get("postgres_conn_id"))  
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    table_name = Variable.get("table_name")

    # Step 1: Create table if not exists
    create_table_if_not_exists(cursor, df, table_name)

    # Step 2: Prepare data for insertion (convert DataFrame to records)
    records = df.astype(str).to_records(index=False)
    values = [tuple(map(lambda x: x.item() if hasattr(x, "item") else x, row)) for row in records]

    # Dynamically generate the INSERT query
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (email) DO NOTHING;"

    logging.info(f"Prepared insert query: {insert_query}")
    logging.info(f"Data being inserted: {values[:5]}")  

    # Step 3: Insert data into PostgreSQL
    try:
        cursor.executemany(insert_query, values)
        conn.commit()  # Commit after successful insertion
        logging.info("Data inserted successfully into PostgreSQL.")
    except Exception as e:
        logging.error(f"Error inserting data: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Main function
def main():
    """Main function to execute the complete process."""
    # Read the CSV into a DataFrame
    csv_file_path = os.path.join(CSV_FOLDER)
    if not os.path.exists(csv_file_path):
        raise ValueError(f"CSV file not found: {csv_file_path}")
    
    # Load DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Validate and clean data
    df = validate_data(df)
    df = cleanup_data(df)
    
    # Insert into DB
    insert_into_db(df)

if __name__ == "__main__":
    main()
