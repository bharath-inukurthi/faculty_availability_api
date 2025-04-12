import os
import logging
import asyncio
from fastapi import FastAPI, Query,Request,HTTPException
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import boto3
import aioboto3
from sqlalchemy import text
import aiohttp
from datetime import datetime
from random import randint
import requests
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import  io
import re
from typing import Dict, Any
import google.auth
from googleapiclient.discovery import build
import json
from fastapi.responses import StreamingResponse

load_dotenv()

free_room_query=query = """
        WITH occupied_rooms AS (
    SELECT 
        tt."Room ID", 
        s."Time Slot"
    FROM "time_table_db" tt
    JOIN "days_db" d ON tt."day_id" = d."day_id"
    JOIN "slots_db" s ON tt."Time_slot_id" = s."Time_slot_id"
    WHERE d."Day" = :day 
        AND (
            :time >= SPLIT_PART(s."Time Slot", '-', 1)  
            AND :time < SPLIT_PART(s."Time Slot", '-', 2)
        )
)
SELECT 
    r."Room No", 
    s."Time Slot"
FROM "room_db" r
JOIN "slots_db" s 
    ON (
        :time >= SPLIT_PART(s."Time Slot", '-', 1)  
        AND :time < SPLIT_PART(s."Time Slot", '-', 2)
    )
LEFT JOIN occupied_rooms o 
    ON r."Room ID" = o."Room ID" 
    AND s."Time Slot" = o."Time Slot"
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

API_URL_1= "https://faculty-availability-api.onrender.com/health"
API_URL_2 = "https://faculty-availability-api.onrender.com/watch_inbox"
DATABASE_URL = os.environ.get("supabase_uri")

# Create Async SQLAlchemy Engine
engine = create_async_engine(DATABASE_URL, echo=True)
async_session_factory = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

app = FastAPI()

async def keep_alive(api_url: str, interval_seconds: int):
    #Asynchronously pings the API every 11 minutes.
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    logging.info(f"Pinged API: {response.status}")
        except Exception as e:
            logging.error(f"Error pinging API: {e}")
        await asyncio.sleep(interval_seconds)

@app.on_event("startup")
async def startup_event():
    # Start the first keep_alive task (every 11 minutes)
    asyncio.create_task(keep_alive(API_URL_1, 660))  # 660 seconds = 11 minutes

    # Start the second keep_alive task (every 5 days)
    asyncio.create_task(keep_alive(API_URL_2, 432000))  # 432000 seconds = 5 days
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
            logging.info(f"rows fetched : {rows}")
            if not rows:
                return "No schedule available."
            output = [dict(row._mapping) for row in rows]
            logging.info(f"results : {output[0]}")
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
            files = []
            for obj in response.get("Contents", [])[1:]:  # Skipping the first object (if needed)
                file_name = obj["Key"].split("/")[-1]  # Extract filename
                s3_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{obj['Key']}"  # Full S3 URL

                # Get shortened URL from TinyURL
                short_url = requests.get(f"http://tinyurl.com/api-create.php?url={s3_url}").text

                # Store the result
                files.append({
                    "file_name": file_name,
                    "public_url": short_url
                })
                print(short_url)

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
        free_rooms = [dict(row._mapping) for row in result.fetchall() if "&" not in row[0]]
        room=randint(0,len(free_rooms)-1)
    return {"day": day, "time": free_rooms[room]["Time Slot"], "free_room": free_rooms[room]["Room No"]}


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

        file_name = object_key.split("/")[-1]
        short_url=requests.get(f"http://tinyurl.com/api-create.php?url={url}").text
# Extract file name from path
        print(short_url)
        return {"file_name": file_name, "presigned_url": short_url}
    except Exception as e:
        logging.error(f"Error generating pre-signed URL: {e}")
        return {"error": str(e)}
def clean_filename(filename):
    if filename:
        decoded_filename, encoding = decode_header(filename)[0]
        if isinstance(decoded_filename, bytes):
            decoded_filename = decoded_filename.decode(encoding or "utf-8", errors="ignore")
        decoded_filename = re.sub(r'^[^a-zA-Z]+', '', decoded_filename)
        return decoded_filename
    return None

# --- Upload to S3 from memory (streaming) ---
async def upload_to_s3_streaming(payload_bytes: bytes, s3_key: str, metadata: dict):
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    session = aioboto3.Session()
    async with session.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
    ) as s3_client:
        stream = io.BytesIO(payload_bytes)
        await s3_client.upload_fileobj(
            Fileobj=stream,
            Bucket=bucket_name,
            Key=s3_key,
            ExtraArgs={"Metadata": {k.lower(): v for k, v in metadata.items()}}
        )
        logging.info(f"✅ Uploaded: {s3_key}")

# --- Email Processing Logic ---
async def process_recent_emails():
    mail = imaplib.IMAP4_SSL(os.getenv('IMAP_SERVER'))
    mail.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
    mail.select("inbox")
    target=os.getenv('SENDER_EMAIL')
    status, email_ids = mail.search(None, f'(UNSEEN FROM "{target}")')

    email_ids = email_ids[0].split() # Last 100 emails

    tasks = []

    for num in reversed(email_ids):
        email_id = num.decode() if isinstance(num, bytes) else str(num)
        sstatus, data = mail.fetch(email_id, "(RFC822)")
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])

                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding or "utf-8", errors="ignore")
                logging.info(f"📩 Processing Email: {subject}")

                for part in msg.walk():
                    if part.get_content_maintype() != "multipart" and part.get("Content-Disposition"):
                        filename = clean_filename(part.get_filename())
                        if not filename:
                            continue

                        raw_date = msg["Date"]
                        parsed_date = parsedate_to_datetime(raw_date)
                        date_str = parsed_date.strftime("%B-%Y-%d")
                        month_str = parsed_date.strftime("%B")

                        payload = part.get_payload(decode=True)
                        metadata = {"month": month_str, "date": date_str}
                        s3_key = f"Circulars/{filename}"
                        logging.info(s3_key)
                        tasks.append(upload_to_s3_streaming(payload, s3_key, metadata))
        mail.store(num, '+FLAGS', '\\Seen')
    await asyncio.gather(*tasks)
    return {"status": "✅ All emails processed and uploaded"}

# --- FastAPI Endpoint ---
@app.post("/upload-emails")
async def trigger_email_upload():
    try:
        result = await process_recent_emails()
        return {"content": result}
    except Exception as e:
        logging.exception("❌ Error processing emails")
        return {"error": str(e)}


async def generate_s3_file_info():
    """Generate streaming data of S3 file information"""
    session = aioboto3.Session()
    bucket_name = os.getenv("AWS_BUCKET_NAME")

    async with session.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
    ) as s3_client:
        response = await s3_client.list_objects_v2(Bucket=bucket_name, Prefix="Circulars")

        if 'Contents' not in response:
            yield json.dumps({"message": "No files found in Circulars/"}) + "\n"
            return

        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue

            head = await s3_client.head_object(Bucket=bucket_name, Key=key)
            metadata = head.get('Metadata', {})

            filename = key.split('/')[-1]
            public_url = f"https://{bucket_name}.s3.amazonaws.com/{key}"

            file_info = {
                "filename": filename,
                "url": public_url,
                "date": metadata.get("date", ""),
                "month": metadata.get("month", "")
            }

            yield f"data:{json.dumps(file_info)}\n\n"  # Each line is a JSON object
            await asyncio.sleep(0.1)  # Small delay to avoid overwhelming clients


@app.get("/watch_inbox")
async def watch_inbox() -> Dict[str, Any]:
    """
    Sets up Gmail API notification watch that will send a signal to Pub/Sub
    when new emails arrive. Does not include email content in the notification.
    """
    try:

        # Now, you can load credentials from the token data
        creds, _ = google.auth.load_credentials_from_file(r'etc/secrets/token.json')

        service = build('gmail', 'v1', credentials=creds)

        # Set up Gmail Watch to get notifications about new emails
        watch_request = {
            'topicName': os.getenv("PUBSUB_TOPIC_NAME"),
            'labelIds': ['INBOX'],
            'labelFilterAction': 'include'
        }
        # Execute the watch request
        response = service.users().watch(userId='me', body=watch_request).execute()

        # Return the historyId and expiration from the watch response
        return {
            'status': 'success',
            'message': 'Email notification watch configured successfully',
            'historyId': response.get('historyId'),
            'expiration': response.get('expiration')
        }

    except Exception as error:
        raise HTTPException(
            status_code=400,
            detail={
                'status': 'error',
                'message': f'An error occurred: {str(error)}'
            }
        )
@app.get("/stream-circulars")
async def stream_circulars(request: Request):
    # Get the user agent to detect client type
   return StreamingResponse(
        generate_s3_file_info(),
        media_type="text/event-stream"  # Using newline-delimited JSON format
    )