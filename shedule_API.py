import os
import logging
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from typing import List, Dict
import uvicorn
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
formattable_sql_query = """SELECT f."Faculty" AS Faculty_Name, 
       r."Room No" AS "Room No", 
       s."Time Slot" AS "Time Slot", 
       d."Day" 
FROM faculty_db f 
JOIN (
    SELECT fs."Faculty_id", 
           MIN(CASE WHEN LOWER(f2."Faculty") LIKE LOWER(:faculty_name) THEN 0 ELSE 1 END) AS like_priority, 
           MIN(LEVENSHTEIN(LOWER(TRIM(f2."Faculty")), LOWER(:faculty_name))) AS min_distance 
    FROM faculty_db f2 
    JOIN faculty_subject_db fs ON f2."Faculty_id" = fs."Faculty_id" 
    GROUP BY fs."Faculty_id" 
    ORDER BY like_priority ASC, min_distance ASC 
    LIMIT 1
) AS closest_match ON f."Faculty_id" = closest_match."Faculty_id" 
LEFT JOIN faculty_subject_db fs ON f."Faculty_id" = fs."Faculty_id" 
LEFT JOIN time_table_db tt ON tt."fs_id" = fs."fs_id" 
LEFT JOIN room_db r ON tt."Room ID" = r."Room ID" 
LEFT JOIN slots_db s ON tt."Time_slot_id" = s."Time_slot_id" 
LEFT JOIN days_db d ON tt."day_id" = d."day_id" 
WHERE d."Day" = :day 
AND CAST(TO_TIMESTAMP(SPLIT_PART(s."Time Slot", '-', 1), 'HH24:MI') AS TIME) >= TIME :time 
ORDER BY CAST(TO_TIMESTAMP(SPLIT_PART(s."Time Slot", '-', 1), 'HH24:MI') AS TIME) ASC 
LIMIT 1;"""

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
def faculty_list(dept:str=Query(...,description="Enter department of faculty yu want to meet")):
    logging.info(f"Executing query for department: {dept}")
    with engine.connection() as connection:
        result=connection.execute((text("""Select "Faculty" from faculty_db f join dept_db d on f."dept_id"=d."dept_id" where dept=:department"""),{
            "department":dept
        }))
    rows=result.fetchall()
    logging.info(f"Query result: {results}")
    output = [dict(row._mapping) for row in rows] 
    results = output[0]  # First result
    return results
# Run FastAPI locally
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)