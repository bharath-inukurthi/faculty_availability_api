import os
import re
import pandas as pd
import numpy as np
import pdfplumber as reader
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
inverse_course_mapping={'Statistics for Engineers': 'Statis',
 'Database Management Systems': 'DBMS',
 'Java Programming': 'Java',
 'Computer Architecture and Organization': 'CAO',
 'Excel Skills': 'EXSEL',
 'Machine Learning': 'ML',
 'Digital Principles and System Design': 'DPSD',
 'University Elective': 'UE',
 'Computer Networks':'CN',
 'Automata and Compiler Design':'ACD',
 'Pattern and Anomaly Detection':'PAD',
 'Foundation on Innovation and Entrepreneurship':'FIE',
 'Design Project II':'EXSEL',
 'Secured Computing':'SC',
 'Smarter City':'SCITY',
 'Big Data Analytics':'BA',
 'Design Project I':'EXSEL',
 'Ethical Hacking & Penetration Testing':'EHPT',
 'Free':'Free'}


class Data_extractor:
    def compatibility(self, page) -> int:
        global length
        d = page.extract_tables()
        length = len(d) - 1
        return length

    def __init__(self, path: str, inverse_course_mapping):
        self.path = path
        self.extracted = []
        self.process()
        self.mapping = inverse_course_mapping

    def process(self) :
        with reader.open(r"{}".format(self.path)) as pdf:
            i = 0
            for page in pdf.pages:
                if self.compatibility(page) > 0:
                    # class_details=self.get_coordinator(page)
                    courses_details = self.get_course_details(page)
                    schedule = self.get_schedule(page, courses_details)
                    i += 1
                    print(i)
                    self.extracted.append({
                        "course_details": courses_details,
                        "schedule": schedule})
        return self.extracted

    def extract_course_room(self, cell):
        if isinstance(cell, str):  # Ensure it's a string
            # Split by newline
            flag = False
            if "Lunch" not in cell and "Free" not in cell:

                # Split words and separate course & room number
                parts = cell.split()

                # Remove "Lab R" completely from the course name
                clean_parts = [p for p in parts if p not in ["Lab", "R"]]

                # Assume the **last** part is always the room number
                if re.match(r"R?\d+", clean_parts[-1]):  # Room numbers start with "R" or digits
                    course_name = " ".join(clean_parts[:-1]).strip()
                    if "CCF" in course_name:
                        course_name = course_name.replace(" CCF", "")
                        flag = True
                    if "T" in course_name:
                        course_name = course_name.replace(" T", "")
                    room_no = clean_parts[-1]
                    if "R" in room_no:
                        room_no = room_no[1:]
                    if flag:
                        room_no = "CCF " + room_no
                else:
                    course_name = " ".join(clean_parts).strip()
                    room_no = "Free"  # No valid room number found
                return course_name, room_no
        return "Free", "Free"

    def convert_to_24hr(self, time_slot):
        if time_slot == "12.00-":
            time_slot = "12.00-1.00"
        time_slot = time_slot.replace("\n", "").strip()
        # Remove newlines and spaces
        start, end = time_slot.split("-")  # Split into start and end times
        start, end = start.strip(), end.strip()  # Trim spaces

        # Convert start time
        start_hour = int(start.split(".")[0])  # Extract hour part
        meridian = "PM" if start_hour < 8 or start_hour == 12 else "AM"
        start_24 = pd.to_datetime(f"{start} {meridian}", format="%I.%M %p").strftime("%H:%M")

        # Convert end time
        end_hour = int(end.split(".")[0])
        meridian = "PM" if end_hour < 8 or end_hour == 12 else "AM"
        end_24 = pd.to_datetime(f"{end} {meridian}", format="%I.%M %p").strftime("%H:%M")

        return f"{start_24}-{end_24}"

    # Apply function to the 'Time Slot' column

    def get_schedule(self, page, courses) -> dict:
        # Convert to Pandas DataFrame
        tables = page.extract_tables()
        time_table = pd.DataFrame(tables[length - 1]).replace(["", "None", "---", "-x-"], np.nan).dropna(how="all")
        time_table = time_table.replace([np.nan], "Free")
        free_counts_col = (time_table == "Free").sum()
        time_table = time_table.drop(columns=free_counts_col[free_counts_col > 5].index)
        free_counts_row = (time_table == "Free").sum(axis=1)
        time_table = time_table.loc[free_counts_row <= 6].reset_index(drop=True)
        if time_table.shape[0] > 6:
            time_table = time_table.iloc[time_table.shape[0] - 6:]
            time_table = time_table.reset_index(drop=True)
        # Drop rows where more than 6 values are "Free"

        time_table.columns = [i for i in range(time_table.shape[1])]
        if time_table.shape[0] == 5:
            header = ['Period / Day', '9.00-10.00', '10.00-11.00', '11.00-12.00', '12.00-\n1.00',
                      '1.00-2.00', '2.00-3.00', '3.00-4.00', '4.00-5.00']

            time_table.loc[-1] = header  # Insert at index -1
            time_table = time_table.sort_index().reset_index(drop=True)
        time_table = time_table.reset_index(drop=True)
        schedule = []

        for index, row in time_table.iterrows():
            rows = []
            for i in range(0, len(row)):
                if row[i] == None:
                    row[i] = "Free"
                rows.append(row[i].replace("\n", " "))
            schedule.append(rows)
        df = pd.DataFrame(schedule)

        time_slots = df.iloc[0, 1:].tolist()
        print(time_slots)
        # Iterate over each row in the DataFrame
        processed_data = []
        for index, row in df.iterrows():
            if index != 0:  # Skip the first row (time slots row)
                day = row[0]
                for i in range(1, len(row)):
                    course, room = self.extract_course_room(row[i])  # Extract course and room number
                    if course:  # Only add valid entries
                        processed_data.append([day, time_slots[i - 1], course, room])  # Assign correct time slot
        final_df = pd.DataFrame(processed_data, columns=["Day", "Time Slot", "Course Name", "Room No"])
        course_details = pd.DataFrame(courses)
        course_details["Course Name"] = course_details["Course Name"].map(inverse_course_mapping)
        final_df = final_df.merge(course_details, on="Course Name", how="inner")
        final_df.drop(columns=["Course Name"], inplace=True)
        final_df["Time Slot"] = final_df["Time Slot"].apply(self.convert_to_24hr)
        return final_df

    def get_coordinator(self, page) -> dict:
        text = page.extract_text()
        pattern = r"SLOT:\s*(SLOT\s*\d+).*?SECTION\s*–\s*(S\d+).*?(?:Class Coordinator|Mr\.|Ms\.)\s*([A-Za-z.\s-]+)"
        # Find matches
        match = re.search(pattern, text, re.DOTALL)
        if match:
            slot = match.group(1)  # SLOT value

            section = match.group(2)  # SECTION value

            coordinator = match.group(3)  # Class Coordinator name
            incharge_details = {
                "slot": slot,
                "section": section,
                "coordinator": coordinator,
            }
        return incharge_details

    def get_course_details(self, page) -> dict:
        course_table = page.extract_tables()
        course_table = pd.DataFrame(course_table[length])
        text = page.extract_text()
        match = re.search(r"DEPARTMENT OF ([A-Z\s]+)\nEVEN SEMESTER", text)
        dept = None
        if match:
            dept = match.group(1).strip()

        courses = [{"course code": 'Free',
                    "Course Name": "Free",
                    "Faculty": "Free",
                    "dept": dept}]
        for index, row in course_table.iterrows():
            if index != 0:
                details = {"course code": row[1].replace("\n", " "),
                           "Course Name": row[2].replace("\n", " "),
                           "Faculty": row[10].replace("\n", " "),
                           "dept": dept}
                courses.append(details)
        return pd.DataFrame(courses)


class TimeTableProcessor:
    def __init__(self, extracted_data, course_mapping):
        self.extracted_data = extracted_data
        self.inverse_course_mapping = course_mapping

    def create_section_db(self):
        section_db = pd.DataFrame([item["class_details"] for item in self.extracted_data])
        section_db = section_db[["section", "slot"]].reset_index().rename(columns={"index": "Section_id"})
        return section_db

    def create_subject_db(self):
        subject_table = [pd.DataFrame(item["course_details"])[["course code", "Course Name"]] for item in
                         self.extracted_data]
        subject_db = pd.concat(subject_table).rename(columns={"Course Name": "Course"}).drop_duplicates().reset_index(
            drop=True)
        subject_db["Course Name"] = subject_db["Course"].map(self.inverse_course_mapping)
        return subject_db

    def create_faculty_db(self):
        faculty_table = [pd.DataFrame(item["course_details"])["Faculty"] for item in self.extracted_data]
        faculty_db = pd.DataFrame(pd.Series([j for i in faculty_table for j in i]).unique(), columns=["Faculty"])
        faculty_db = faculty_db.reset_index().rename(columns={"index": "Faculty_id"})
        return faculty_db

    def create_faculty_subject_db(self, faculty_db):
        faculty_subject_table = [pd.DataFrame(item["course_details"])[["course code", "Faculty"]] for item in
                                 self.extracted_data]
        df = pd.concat(faculty_subject_table, axis=0)
        faculty_subject_data = df.merge(faculty_db, on="Faculty")
        faculty_subject_data = faculty_subject_data.drop_duplicates(subset=["course code", "Faculty"], keep="first")
        faculty_subject_data = faculty_subject_data.reset_index(drop=True).reset_index().rename(
            columns={"index": "fs_id"})
        return faculty_subject_data

    def create_days_db(self):
        day_table = [pd.DataFrame(item["schedule"])["Day"] for item in self.extracted_data]
        days_db = pd.DataFrame(pd.concat(day_table, axis=0).unique(), columns=["Day"]).reset_index().rename(
            columns={"index": "day_id"})
        return days_db

    def create_slots_db(self):
        slots_table = [pd.DataFrame(item["schedule"])["Time Slot"] for item in self.extracted_data]
        slots_db = pd.DataFrame(pd.concat(slots_table, axis=0).unique(), columns=["Time Slot"]).reset_index().rename(
            columns={"index": "Time_slot_id"})
        return slots_db

    def create_room_db(self):
        room_table = [pd.DataFrame(item["schedule"])["Room No"] for item in self.extracted_data]
        room_db = pd.DataFrame(
            pd.Series([j for i in room_table for j in i]).replace({"Comp": "Computer block"}).unique(),
            columns=["Room No"])
        room_db = room_db.reset_index().rename(columns={"index": "Room ID"})
        return room_db

    def create_time_table_db(self, faculty_subject_data, room_db, slots_db, days_db):
        time_table_data = []
        for item in self.extracted_data:
            df = pd.DataFrame(item["schedule"]).merge(faculty_subject_data, on=["course code", 'Faculty'],
                                                      how="left").drop(columns=["course code", 'Faculty', "Faculty_id"])
            df = df.merge(room_db, on="Room No", how="left").drop(columns=["Room No"])
            df = df.merge(slots_db, on="Time Slot", how="left").drop(columns=["Time Slot"])
            df = df.merge(days_db, on="Day", how="left").drop(columns=["Day"])
            time_table_data.append(df)
        time_table_db = pd.concat(time_table_data).reset_index(drop=True).reset_index().rename(
            columns={"index": "Time_table_id"})
        return time_table_db.drop(columns=['dept'])

    def process_all(self):
        # section_db = self.create_section_db()
        subject_db = self.create_subject_db()
        faculty_db = self.create_faculty_db()
        faculty_subject_db = self.create_faculty_subject_db(faculty_db)
        days_db = self.create_days_db()
        slots_db = self.create_slots_db()
        room_db = self.create_room_db()
        time_table_db = self.create_time_table_db(faculty_subject_db, room_db, slots_db, days_db)

        return {
            # "section_db": section_db,
            "subject_db": subject_db,
            "faculty_db": faculty_db,
            "faculty_subject_db": faculty_subject_db,
            "days_db": days_db,
            "slots_db": slots_db,
            "room_db": room_db,
            "time_table_db": time_table_db
        }

