import os
import logging
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
# Initialize Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# FastAPI instance
app = FastAPI()

# Create Database URL
DATABASE_URL = os.environ.get("supabase_uri")

# Create SQLAlchemy Engine
engine = create_engine(DATABASE_URL)

# SQL Query Template
formattable_sql_query = """WITH faculty_schedule AS (
    SELECT tt.day_id, tt."Time_slot_id", fs."Faculty"  
    FROM time_table_db tt
    JOIN faculty_subject_db fs ON tt.fs_id = fs.fs_id
    WHERE fs."Faculty" = :faculty_name  -- Replace dynamically if needed
), 
all_slots AS (
    SELECT d.day_id, 
           s."Time_slot_id", 
           s."Time Slot" AS slot_time,  -- Keep full slot format (09:00-10:00)
           SPLIT_PART(s."Time Slot", '-', 1)::TIME AS start_time,  -- Extract start time
           SPLIT_PART(s."Time Slot", '-', 2)::TIME AS end_time,  -- Extract end time
           d."Day"
    FROM days_db d
    CROSS JOIN slots_db s
) 
SELECT :faculty_name  AS Faculty, 
       c.cabin,
       a.slot_time AS Slot -- Keep full time range
FROM all_slots a
LEFT JOIN faculty_schedule f 
    ON a.day_id = f.day_id AND a."Time_slot_id" = f."Time_slot_id"
JOIN cabin_db c ON c."Faculty" = :faculty_name   -- Map faculty to their cabin
WHERE f."Time_slot_id" IS NULL
AND (
    a.day_id > (SELECT day_id FROM days_db WHERE "Day" = :day)  
    OR (
        a.day_id = (SELECT day_id FROM days_db WHERE "Day" = :day)  
        AND (:time ::TIME < a.start_time OR :time ::TIME < a.end_time)  -- Compare both start & end
    )
) 
ORDER BY a.day_id, a.start_time
LIMIT 1;

"""

def execute_query(faculty_name: str, day: str, time: str) -> str:
    """Executes the SQL query on Supabase and returns results."""
    logging.info(f"Executing query for Faculty: {faculty_name}, Day: {day}, Time: {time}")

    try:
        with engine.connect() as connection:
            result = connection.execute(text(formattable_sql_query), {
                "faculty_name": faculty_name,
                "day": day,
                "time": time
            })
            rows = result.fetchall()

            if not rows:
                logging.warning("No schedule found for given input.")
                return "No schedule available for this faculty at the given time."

            output = [dict(row._mapping) for row in rows] 
            results = output[0]  # First result
            logging.info(f"Query result: {results}")
            keys=list(results.keys())
            response = f"You can meet {results[keys[0]]} in room no. {results[keys[1]]} from {results[keys[2]]}."
            logging.info(f"response: {response}")
            return response
    except Exception as e:
        logging.error(f"Database query error: {e}")
        return "Error retrieving schedule. Please try again later."

@app.get("/faculty-schedule/")
def get_faculty_schedule(
    faculty_name: str = Query(..., description="Enter faculty name"),
    day: str = Query(..., description="Enter the day (e.g., Monday)"),
    time: str = Query(..., description="Enter time in HH:MM format (e.g., 9:00)")
):
    """API endpoint to get faculty schedule."""
    return {"schedule": execute_query(faculty_name, day, time)}

@app.get("/faculty_list")
def faculty_list():#dept:str=Query(...,description="Enter department of faculty yu want to meet")):
    #logging.info(f"Executing query for department: {dept}")
    with engine.connect() as connection:
        result=connection.execute(text("""Select "Faculty" from faculty_db;"""))
    rows=result.fetchall()
    logging.info(f"Query result: {rows}\n\n")
    output = [row[0] for row in rows[1:]]
     # First result
    logging.info(output)
    return output