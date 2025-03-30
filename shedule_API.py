import os
import logging
import asyncio
import pdf2image
from fastapi import FastAPI, Query
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import boto3
from sqlalchemy import text
import aiohttp
from datetime import datetime
from random import randint
load_dotenv()
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
# Initialize Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

API_URL = "https://faculty-availability-api.onrender.com/health"
DATABASE_URL = os.environ.get("supabase_uri")

# Create Async SQLAlchemy Engine
engine = create_async_engine(DATABASE_URL, echo=True)
async_session_factory = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

app = FastAPI()

async def keep_alive():
    """Asynchronously pings the API every 11 minutes."""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL) as response:
                    logging.info(f"Pinged API: {response.status}")
        except Exception as e:
            logging.error(f"Error pinging API: {e}")
        await asyncio.sleep(660)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(keep_alive())

@app.get("/health")
async def health_check():
    return {"status": "keeping live"}

async def execute_query(faculty_name:str, day:str, time:str):
    """Executes the SQL query asynchronously and returns results."""
    async with async_session_factory() as session:
        try:
            parsed_time = datetime.strptime(time, "%H:%M").time()
            result = await session.execute(
                text(faculty_sql_query), {"faculty_name": faculty_name, "day": day, "time": parsed_time}
            )
            rows = result.fetchall()

            if not rows:
                return "No schedule available."
            output = [dict(row._mapping) for row in rows]

            return output[0]
        except Exception as e:
            logging.error(f"Database error: {e}")
            return "Error retrieving schedule."

@app.get("/faculty-schedule/")
async def get_faculty_schedule(faculty_name: str=Query(..., description="Enter faculty name you want to meet"),
                        day: str=Query(..., description="Enter the name of the weekday you want to meet(e.g.Mondya,Tuesday"),
                        time: str=Query(..., description="enter time on which you want meet the faculty")):
    return await execute_query(faculty_name, day, time)

@app.get("/faculty_list")
async def faculty_list():
    async with async_session_factory() as session:
        result = await session.execute(
            text('''SELECT "Faculty" FROM faculty_db ORDER BY REGEXP_REPLACE("Faculty", '^(Dr\\.|Prof\\.|Mr\\.|Ms\\.)\\s*[A-Z]\\.\\s*', '', 'gi');''')
        )
        rows = result.fetchall()
    return [row[0] for row in rows]

async def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

@app.get("/list-objects/")
async def list_objects(folder: str =Query(...,description="Enter the folder available in S3 bucket")):
    s3_client = await get_s3_client()
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    region=os.getenv("AWS_REGION")

    try:
        if folder == "Forms":
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
            files = [
                {
                    "file_name": obj["Key"].split("/")[-1],  # Ensures correct filename extraction
                    "public_url": f"https://{bucket_name}.s3.{region}.amazonaws.com/{obj['Key']}"
                }
                for obj in response.get("Contents", [])[1:]
            ]
            return {"files": files}
        else:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
            files = [
                {
                    "file_name": obj["Key"].split("/")[-1] ,# Ensures correct filename extraction
                    "public_url":"Use 'get-item' endpoint"
                }
                for obj in response.get("Contents", []) if obj["Key"] != ""
            ]
            return {"files": files}
    except Exception as e:
        logging.error(f"Error listing objects: {e}")
        return {"error": str(e)}

@app.get("/empty-rooms/")
async def find_empty_rooms(day: str=Query(...,description="Enter the name of weekday on which you want to find empty room"),
                         time: str=Query(...,description="Enter the time of when you need an empty room")):
    async with async_session_factory() as session:
        result = await session.execute(text(free_room_query), {"day": day, "time": time})
        free_rooms = [dict(row._mapping) for row in result.fetchall() if "&" not in row[1]]
        room=randint(0,len(free_rooms)-1)
    return {"day": day, "time": time, "free_room": free_rooms[room]["Room No"]}


@app.get("/get-item/")
async def generate_temp_url(

        object_key: str = Query(..., description="Key (file path) of the S3 object")

):
    bucket_name=os.getenv("AWS_BUCKET_NAME")
    s3_client = await get_s3_client()
    try:
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=300
        )

        file_name = object_key.split("/")[-1]  # Extract file name from path
        return {"file_name": file_name, "presigned_url": url}
    except Exception as e:
        logging.error(f"Error generating pre-signed URL: {e}")
        return {"error": str(e)}