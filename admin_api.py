import os
import logging
import re
import boto3
import shutil
import pdfplumber
from PyPDF2 import PdfReader, PdfWriter
from fastapi import FastAPI, File, UploadFile
from dotenv import load_dotenv
from pdf2jpg import pdf2jpg
from utils import Data_extractor,TimeTableProcessor,inverse_course_mapping
from sqlalchemy import create_engine, except_
import pandas as pd

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI()

DATABASE_URL = os.environ.get("supabase_uri_non_async")

# Create Async SQLAlchemy Engine



# AWS S3 Configuration
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)
course_mapping={v:k for k,v in inverse_course_mapping.items()}

image_output="images"
UPLOAD_FOLDER = "uploads"
EXTRACTED_FOLDER = "extracted_pages"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACTED_FOLDER, exist_ok=True)
os.makedirs(image_output, exist_ok=True)


@app.post("/upload-shchedule-to-DB/")
async def upload_pdf(file: UploadFile = File(...)):
    """Endpoint to upload and process a timetable PDF."""
    try:
        pdf_path = os.path.join(UPLOAD_FOLDER, file.filename)
        with open(pdf_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Process PDF

        processor = TimeTableProcessor(
            Data_extractor(pdf_path, course_mapping).extracted, course_mapping
        )
        dbs = processor.process_all()

        # Update database
        engine = create_engine(DATABASE_URL)
        # Use `engine.connect()` in a `with` statement
        with engine.connect() as connection:
            for name, df in dbs.items():
                df.to_sql(name, con=connection, if_exists='replace', index=False)
        os.remove(pdf_path)
        return {"message": "Timetable processed successfully!"}
    except Exception as e:
        return {"message": e}

@app.post("/upload-cabins-to-DB")
def upload_cabin_data(file: UploadFile = File(...)):
    try:
        pdf_path = os.path.join(UPLOAD_FOLDER, file.filename)
        with open(pdf_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        with pdfplumber.open(pdf_path) as pdf_file:
            tables=[]
            for page in pdf_file.pages:
                lst=[]
                table=page.extract_tables()
                for row in table[0][1:]:
                    lst.append({"Faculty":row[1],
                          "cabin":row[-1]})
                tables.append(pd.DataFrame(lst))
        df=pd.concat(tables,ignore_index=True)
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            df.to_sql("cabin_db", con=connection, if_exists='replace', index=False)
        os.remove(pdf_path)
        return {"message":"upload successfull"}
    except Exception as e:
        return {"error": e}


@app.post("/upload-files-to-s3/")
async def upload_pdf(folder: str = "", file: UploadFile = File(...)):
    """Uploads a PDF file, extracts pages, renames them, and uploads to S3."""
    pdf_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(pdf_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    logger.info(f"File uploaded: {file.filename}, saved to {pdf_path}")

    # Process and upload extracted pages
    process_pdf_and_upload(pdf_path, folder)
    os.remove(f"{UPLOAD_FOLDER}/{file.filename}")
    return {"message": "Upload successful"}


def process_pdf_and_upload(pdf_path: str, s3_folder: str):
    """Extracts text, finds patterns, splits into separate PDFs, renames, and uploads."""

    section_pattern = r'SECTION\s*[\-\–\—]\s*S(\d+)'
    year_pattern = r'YEAR:\s*(I{1,3}|IV|V{1,3})'
    calendar_pattern=r'\b(Odd|Even) Semester\b'

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()[:200] if page.extract_text() else ""

                # Extract patterns
                if s3_folder == "Time-Tables":
                    section_match = re.search(section_pattern, text)
                    year_match = re.search(year_pattern, text)
                    logger.info(f"text:{text}")
                    section = section_match.group(1).zfill(2) if section_match else "00"
                    year = year_match.group(1) if year_match else "Unknown"
                    new_file=f"{year}-year-S{section}"
                elif s3_folder=="Calenders":
                    calender_match = re.search(calendar_pattern, text)
                    calender=calender_match.group(1) if calender_match else "Unknown"

                    new_file=f"{calender}-Semester"
                # Extract and save the page as a new PDF
                logger.info("entering save as pdf")
                extracted_page_path = save_page_as_pdf(pdf_path, page_num,new_file)

                # Upload to S3
                logger.info("entering upload")
                upload_to_s3(extracted_page_path, f"{s3_folder}/{os.path.basename(extracted_page_path)}")
                shutil.rmtree(f"{image_output}/{new_file}.pdf_dir")
                os.remove(f"{EXTRACTED_FOLDER}/{new_file}.pdf")

    except Exception as e:
        logger.error(f"Error processing file {pdf_path}: {e}")


def save_page_as_pdf(pdf_path: str, page_number: int,file_path:str) -> str:
    """Extracts a single page from a PDF and saves it as a separate PDF."""
    output_pdf_path = os.path.join(EXTRACTED_FOLDER, f"{file_path}.pdf")
    logger.info("inside")
    try:
        reader = PdfReader(pdf_path)
        logger.info("reading")
        writer = PdfWriter()
        writer.add_page(reader.pages[page_number - 1])

        with open(output_pdf_path, "wb") as output_pdf:
            logger.info("writing")
            writer.write(output_pdf)
        pdf2jpg.convert_pdf2jpg(output_pdf_path,image_output,pages="ALL")
        converted_files = os.listdir(f"{image_output}/{file_path}.pdf_dir")
        logger.info(f"listing: {converted_files}")
        for file in converted_files:
            if file.endswith(".jpg"):
                logger.info(file)
                old_path = os.path.join(f"{image_output}/{file_path}.pdf_dir", file)
                new_path = os.path.join(f"{image_output}/{file_path}.pdf_dir", f"{file_path}.jpg")  # Set custom filename
                os.rename(old_path, new_path)
                logger.info(f"Renamed: {old_path} -> {new_path}")
                return new_path

    except Exception as e:
        logger.error(f"Error saving page {page_number}: {e}")
        return ""


def upload_to_s3(file_path: str, s3_key: str):
    """Uploads a file to S3."""
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    logger.info("inside upload")
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"Uploaded {file_path} to S3 as {s3_key}")
    except Exception as e:
        logger.error(f"Error uploading {file_path} to S3: {e}")
