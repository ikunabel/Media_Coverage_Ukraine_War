import sqlite3
import json
import jsonlines

# Create a connection
def create_connection(db_file="data.db"):
    """Create a database connection."""
    conn = sqlite3.connect(db_file, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")  # Enable Write-Ahead Logging
    return conn

# Create an improved table
def create_table(conn):
    """Create an enhanced table to store more fields."""
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tweet_id TEXT,
        author_id TEXT,
        created_at TEXT,
        lang TEXT,
        retweet_count INTEGER,
        reply_count INTEGER,
        like_count INTEGER,
        quote_count INTEGER,
        text TEXT,
        media TEXT,
        en_text TEXT,
        stanza_output TEXT,
        stanza_named_entities TEXT,
        sentiment TEXT,
        stance TEXT,
        channel TEXT,
        country TEXT,
        verified TEXT,
        follower_count INTEGER,
        image_tags TEXT
    )
    """)
    conn.commit()

# Load Data with Safe Insertion
def load_data(conn, jsonl_file):
    """Load data from a JSONL file into the database."""
    cursor = conn.cursor()
    line_number = 0

    with jsonlines.open(jsonl_file, mode='r') as reader:
        for record in reader.iter(skip_invalid=True):
            line_number += 1

            try:
                # Convert nested fields to JSON strings for storage
                cursor.execute("""
                INSERT INTO records (
                    tweet_id, author_id, created_at, lang, retweet_count, reply_count, like_count, 
                               quote_count, text, media, en_text, stanza_output, stanza_named_entities, 
                               sentiment, stance, channel, country, verified, follower_count, image_tags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get("tweet_id"),
                    record.get("author_id"),
                    record.get("created_at"),
                    record.get("lang"),
                    record.get("retweet_count"),
                    record.get("reply_count"),
                    record.get("like_count"),
                    record.get("quote_count"),
                    record.get("text"),
                    json.dumps(record.get("media")),
                    record.get("en_text"),
                    json.dumps(record.get("stanza_output")),
                    json.dumps(record.get("stanza_named_entities")),
                    json.dumps(record.get("sentiment")),
                    json.dumps(record.get("stance")),
                    record.get("channel"),
                    record.get("country"),
                    record.get("verified"),
                    record.get("follower_count"),
                    json.dumps(record.get("image_tags"))
                ))
            except sqlite3.Error as e:
                print(f"Error inserting line {line_number}: {e}")

    conn.commit()
    print(f"Finished loading {line_number} lines.")

# Add New Columns to the Table for confidence thresholds
def add_new_columns(conn):
    """Add pro_russia, pro_ukraine, and unsure columns to the records table."""
    cursor = conn.cursor()
    
    # Add Columns if They Don't Already Exist
    cursor.execute("ALTER TABLE records ADD COLUMN pro_russia INTEGER DEFAULT 0")
    cursor.execute("ALTER TABLE records ADD COLUMN pro_ukraine INTEGER DEFAULT 0")
    cursor.execute("ALTER TABLE records ADD COLUMN unsure INTEGER DEFAULT 0")
    
    conn.commit()
    print("Columns added successfully.")

import sqlite3

def update_stance_categories(conn):
    """Update stance categories (pro_russia, pro_ukraine, unsure) for each record based on the given truth table."""
    cursor = conn.cursor()

    query = """
    UPDATE records
    SET
        pro_russia = CASE
            WHEN (
                (
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Russia'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                    )
                    OR
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is against Ukraine'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                    )
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Ukraine'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is against Russia'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                )
            ) THEN 1
            ELSE 0
        END,

        pro_ukraine = CASE
            WHEN (
                (
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Ukraine'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                    )
                    OR
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is against Russia'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                    )
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Russia'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is against Ukraine'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>=0.9
                )
            ) THEN 1
            ELSE 0
        END,

        unsure = CASE
            WHEN pro_russia = 0 AND pro_ukraine = 0 THEN 1
            ELSE 0
        END
    """

    try:
        cursor.execute(query)
        conn.commit()
        print("Stance categories updated according to the truth table logic.")
    except sqlite3.Error as e:
        print(f"Error updating categories: {e}")


def update_stance_categories_threshold(conn, threshold):
    """Update stance categories (pro_russia, pro_ukraine, unsure) for each record based on the given threshold."""
    cursor = conn.cursor()

    query = f"""
    UPDATE records
    SET
        pro_russia = CASE
            WHEN (
                (
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Russia'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                    )
                    OR
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is against Ukraine'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                    )
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Ukraine'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is against Russia'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                )
            ) THEN 1
            ELSE 0
        END,

        pro_ukraine = CASE
            WHEN (
                (
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Ukraine'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                    )
                    OR
                    EXISTS(
                        SELECT 1
                        FROM json_each(records.stance)
                        WHERE json_extract(value,'$.hypothesis')='This statement is against Russia'
                          AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                    )
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is in favour of Russia'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                )
                AND NOT EXISTS(
                    SELECT 1
                    FROM json_each(records.stance)
                    WHERE json_extract(value,'$.hypothesis')='This statement is against Ukraine'
                      AND CAST(json_extract(value,'$.entail_prob') AS REAL)>={threshold}
                )
            ) THEN 1
            ELSE 0
        END,

        unsure = CASE
            WHEN pro_russia = 0 AND pro_ukraine = 0 THEN 1
            ELSE 0
        END
    """

    try:
        cursor.execute(query)
        conn.commit()
        print("Stance categories updated according to the threshold logic.")
    except sqlite3.Error as e:
        print(f"Error updating categories: {e}")







