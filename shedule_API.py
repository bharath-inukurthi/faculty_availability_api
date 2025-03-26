import os
import logging
from fastapi import FastAPI, Query
import threading
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import boto3
import time
import  requests


load_dotenv()
# Initialize Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

API_URL = "https://faculty-availability-api.onrender.com/health"


# Create Database URL
DATABASE_URL = os.environ.get("supabase_uri")

# Create SQLAlchemy Engine
engine = create_engine(DATABASE_URL)

# SQL Query Template
free_room_query=query = """
        WITH occupied_rooms AS (
            SELECT tt."Room ID"
            FROM "time_table_db" tt
            JOIN "days_db" d ON tt."day_id" = d."day_id"
            JOIN "slots_db" s ON tt."Time_slot_id" = s."Time_slot_id"
            WHERE d."Day" = :day  
            AND (
                :time >= SPLIT_PART(s."Time Slot", '-', 1)  
                AND :time < SPLIT_PART(s."Time Slot", '-', 2)
            )
        )
        SELECT r."Room ID", r."Room No"
        FROM "room_db" r
        LEFT JOIN occupied_rooms o ON r."Room ID" = o."Room ID"
        WHERE o."Room ID" IS NULL;
    """

faculty_sql_query = """ WITH faculty_schedule AS (
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
app = FastAPI()
# Function to periodically ping itself
def keep_alive():
    while True:
        try:
            response = requests.get(API_URL)
            print(f"Pinged API: {response.status_code}")
        except Exception as e:
            print(f"Error pinging API: {e}")
        time.sleep(660)  # 11 minutes (660 seconds)

# Startup event that runs when the API starts
@app.on_event("startup")
def lifespan():
    task = threading.Thread(target=keep_alive, daemon=True)  # Run as a daemon thread
    task.start()



# Simple health check endpoint
@app.get("/health",include_in_schema=True)
def health_check():
    return {"status": "keeping live"}


async def execute_query(faculty_name: str, day: str, time: str) -> str:
    """Executes the SQL query on Supabase and returns results."""
    logging.info(f"Executing query for Faculty: {faculty_name}, Day: {day}, Time: {time}")

    try:
        with engine.connect() as connection:
            result = connection.execute(text(faculty_sql_query), {
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
async def get_faculty_schedule(
    faculty_name: str = Query(..., description="Enter faculty name"),
    day: str = Query(..., description="Enter the day (e.g., Monday)"),
    time: str = Query(..., description="Enter time in HH:MM format (e.g., 9:00)")
):
    """API endpoint to get faculty schedule."""
    return {"schedule": execute_query(faculty_name, day, time)}

@app.get("/faculty_list")
async def faculty_list():#dept:str=Query(...,description="Enter department of faculty yu want to meet")):
    #logging.info(f"Executing query for department: {dept}")
    with engine.connect() as connection:
        result=connection.execute(text("""SELECT "Faculty"
FROM faculty_db
ORDER BY REGEXP_REPLACE("Faculty", '^(Dr\.|Prof\.|Mr\.|Ms\.)\s*[A-Z]\.\s*', '', 'gi');
"""))
    rows=result.fetchall()
    logging.info(f"Query result: {rows}\n\n")
    output = [row[0] for row in rows[1:]]
     # First result
    logging.info(output)
    return output


@app.get("/list-objects/")
async def list_objects(folder :str="Forms"):
    try:
        s3_client = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                 aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                 region_name=os.getenv("AWS_REGION"))
        logging.info("S3 client successfully created")
    except Exception as e:
        logging.error(f"Error creating S3 client: {e}")
        s3_client = None
    if s3_client is None:
        return {"error": "S3 client could not be initialized"}

    bucket_name = os.getenv("AWS_BUCKET_NAME")  # Get bucket name from .env
    region = os.getenv("AWS_REGION", "us-east-1")  # Default region: us-east-1
    logging.info(f"Attempting to list objects in: {bucket_name}/{folder}")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=f"{folder}")
        if "Contents" in response:
            files = []
            for obj in response["Contents"][1:]:
                file_name = obj["Key"]
                public_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{file_name}"
                logging.info(public_url)
                public_url = public_url.replace("+", "%20")  # Replace '+' with '%20' for spaces

                files.append({"file_name": file_name.split("/")[1], "public_url": public_url})

            return {"files": files}
        return {"message": "No files found"}
    except Exception as e:
        logging.error(f"Error listing objects: {e}")
        return {"error": str(e)}

@app.get("/get-file/")
async def get_file(object_key:str):
    try:
        s3_client = boto3.client("s3", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                 aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                                 region_name=os.getenv("AWS_REGION"))
        logging.info("S3 client successfully created")
    except Exception as e:
        logging.error(f"Error creating S3 client: {e}")
        s3_client = None
    if s3_client is None:
        return {"error": "S3 client could not be initialized"}

    bucket_name = os.getenv("AWS_BUCKET_NAME")  # Get bucket name from .env
    logging.info(f"Attempting download: {object_key}")
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': object_key},
        ExpiresIn=60  # URL valid for 1 hour
    )

    return {"Pre-signed URL": presigned_url}


@app.get("/free-rooms/")
async def get_free_rooms(day: str = Query(..., title="Day of the week"),
                   time: str = Query(..., title="Time (HH:MM)")):


    with engine.connect() as connection:
        result = connection.execute(text(free_room_query), {"day": day, "time": time})
        free_rooms = [
            {"Room ID": row[0], "Room No": row[1]}
            for row in result if "&" not in row[1]
        ]

    return {"day": day, "time": time, "free_rooms": free_rooms[1]["Room No"]}