import pandas as pd
import os
import random
from datetime import datetime, timedelta
import sqlite3

# ------------------------------
# 1. CREATE FOLDER
# ------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(BASE_DIR, "student_data")

os.makedirs(DATA_FOLDER, exist_ok=True)

RAW_FILE   = os.path.join(DATA_FOLDER, "students_raw.xlsx")
CLEAN_FILE = os.path.join(DATA_FOLDER, "students_clean.xlsx")
DB_FILE    = os.path.join(DATA_FOLDER, "students.db")

TABLE_NAME = "students_masters"

# ------------------------------
# 2. CREATE DETAILED STUDENT FILE IF NOT EXISTS
# ------------------------------
if not os.path.exists(RAW_FILE):
    print("Creating detailed student dataset...")

    first_names = ["Ali","Sara","Ahmed","Fatima","Hassan","Ayesha","Omar","Zainab","Usman","Hira","Bilal","Noor"]
    last_names  = ["Khan","Shah","Iqbal","Malik","Ahmed","Raza","Siddiqui","Chaudhry","Hussain","Yousaf","Farooq","Javed"]
    genders = ["Male", "Female"]
    ages = list(range(21, 31))
    countries_origin = ["Pakistan","India","Bangladesh","Nepal","Sri Lanka"]
    dest_countries = ["USA","UK","Canada","Australia","Germany","Netherlands","France","Sweden","Switzerland"]
    degrees = ["Masters","MS","MSc","MBA"]
    universities = ["MIT","Oxford","Harvard","Cambridge","University of Toronto","Sydney Uni","LMU Munich","Sorbonne","ETH Zurich","TU Berlin","Monash University","McGill University","UCL","National University of Singapore","HKUST"]
    scholarships = ["Full","Partial","Self-funded"]
    fields_of_study = ["Engineering","Business","IT","Science","Arts","Law","Medicine","Economics"]
    admission_status = ["Accepted","Waitlisted","Rejected"]
    application_channel = ["Online","Agent","University Portal"]

    data = []
    num_students = 3000
    start_date = datetime(2019, 1, 1)

    for i in range(1, num_students+1):
        first_name = random.choice(first_names)
        last_name  = random.choice(last_names)
        full_name = f"{first_name} {last_name}"
        gender = random.choice(genders)
        age = random.choice(ages)
        country_of_origin = random.choice(countries_origin)
        destination_country = random.choice(dest_countries)
        degree = random.choice(degrees)
        university = random.choice(universities)
        scholarship = random.choice(scholarships)
        field = random.choice(fields_of_study)
        status = random.choice(admission_status)
        channel = random.choice(application_channel)
        random_days = random.randint(0, 5*365)
        departure_date = start_date + timedelta(days=random_days)
        program_duration = random.randint(1,3)
        gpa = round(random.uniform(2.5,4.0),2)
        email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"

        data.append({
            "student_id": i,
            "full_name": full_name,
            "gender": gender,
            "age": age,
            "country_of_origin": country_of_origin,
            "destination_country": destination_country,
            "degree_level": degree,
            "university": university,
            "scholarship_type": scholarship,
            "departure_date": departure_date,
            "program_duration_years": program_duration,
            "field_of_study": field,
            "gpa": gpa,
            "admission_status": status,
            "application_channel": channel,
            "contact_email": email
        })

    df = pd.DataFrame(data)
    df.to_excel(RAW_FILE, index=False)
    print(f"Raw student file created: {RAW_FILE}")

# ------------------------------
# 3. LOAD & CLEAN DATA
# ------------------------------
df = pd.read_excel(RAW_FILE)

df.columns = df.columns.str.strip().str.lower()
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].str.strip()

required_cols = ["destination_country", "degree_level", "departure_date"]
df = df.dropna(subset=required_cols)

df["degree_level"] = df["degree_level"].str.lower()
df = df[df["degree_level"].isin(["master","masters","ms","msc"])]
df["departure_date"] = pd.to_datetime(df["departure_date"], errors="coerce")
df = df.dropna(subset=["departure_date"])
df["year"] = df["departure_date"].dt.year

if "student_id" in df.columns:
    df = df.drop_duplicates(subset=["student_id","destination_country","departure_date"])
else:
    df = df.drop_duplicates()

# ------------------------------
# 4. SAVE CLEAN EXCEL
# ------------------------------
df_clean = df.fillna("")
df_clean.to_excel(CLEAN_FILE, index=False)
print(f"Clean Excel saved: {CLEAN_FILE}")

# ------------------------------
# 5. SAVE TO SQLITE
# ------------------------------
conn = sqlite3.connect(DB_FILE)
df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
print(f"SQLite database created: {DB_FILE}")

# ------------------------------
# 6. RUN SAMPLE QUERY
# ------------------------------
current_year = datetime.now().year
start_year = current_year - 5

query = f"""
SELECT year, destination_country, COUNT(*) AS total_students
FROM {TABLE_NAME}
WHERE year >= {start_year}
GROUP BY year, destination_country
ORDER BY year, destination_country
"""

result = pd.read_sql(query, conn)
conn.close()

print("\nPreview last 5 years (year + country):")
print(result.head(10))