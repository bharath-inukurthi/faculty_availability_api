from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from typing import List, Dict
import os
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

def execute_query(faculty_name: str, day: str, time: str) -> List[Dict]:
    """Executes the SQL query on Supabase and returns results."""
    with engine.connect() as connection:
        result = connection.execute(text(formattable_sql_query), {
            "faculty_name": faculty_name,
            "day": day,
            "time": time
        })
        rows = result.fetchall()
        output= [dict(row._mapping) for row in rows] 
        results=output[0]
        return f"You can meet {results['faculty_name']} in room.no {results['Room No']} from {results['Time Slot']} "


@app.get("/faculty-schedule/")
def get_faculty_schedule(
    faculty_name: str = Query(..., description="Enter faculty name"),
    day: str = Query(..., description="Enter the day (e.g., Monday)"),
    time: str = Query(..., description="Enter time in HH:MM format (e.g., 9:00)")
):
    """API endpoint to get faculty schedule."""
    return {"schedule": execute_query(faculty_name, day, time)}

