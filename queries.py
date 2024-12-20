import sys
import sqlite3
import json
import jsonlines
import ijson
import pandas as pd

# Function to read the n-th line
def read_line_as_df(filename, line_id):
    with jsonlines.open(filename, mode='r') as reader:
        # Iterate through the file
        for idx, record in enumerate(reader.iter(), 1):
            if idx == line_id:
                print(f"Line {line_id} found.")
                print(record)
                # Convert to DataFrame
                df = pd.DataFrame([record])
                return df
            
# Custom SQL query function
def run_custom_query(conn, query, params=()):
    """Run a custom SQL query."""
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.fetchall()

def fetch_first_n_rows(conn, n=10):
    """
    Fetch the first n rows from the entire records table.
    """
    query = """
    SELECT * 
    FROM records
    LIMIT ?
    """
    
    # Use Pandas to Return as DataFrame
    import pandas as pd
    df = pd.read_sql_query(query, conn, params=(n,))
    return df




 



