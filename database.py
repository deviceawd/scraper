import psycopg2
from psycopg2 import extras
from datetime import datetime
import re
import os
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'db_car');")
    table_exists = cursor.fetchone()[0]
    cursor.close()

    if not table_exists:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE db_car (
                id SERIAL PRIMARY KEY,
                url VARCHAR(255),
                title TEXT,
                price_usd NUMERIC,
                odometer INTEGER,
                username VARCHAR(255),
                phone_number VARCHAR(20),
                image_url VARCHAR(255),
                images_count INTEGER,
                car_number VARCHAR(20),
                car_vin VARCHAR(20),
                datetime_found TIMESTAMP
            );""")
        conn.commit()

    return conn


def insert_into_db(data):
    conn = connect_db()
    cur = conn.cursor()

    sql_select = "SELECT * FROM db_car WHERE url = %s OR car_number = %s OR car_vin = %s"
    sql_insert = """INSERT INTO db_car (url, title, price_usd, odometer, username, phone_number, image_url, images_count, car_number, car_vin, datetime_found)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    for car_data in data:
        values_select = (
            car_data.get('url', ''),
            car_data.get('car_number', ''),
            car_data.get('vin_code', '')
        )

        cur.execute(sql_select, values_select)
        existing_record = cur.fetchone()

        if existing_record is None:
            values_insert = (
                car_data.get('url', ''),
                ' '.join(car_data.get('car_title', [])),
                car_data.get('price', 0),
                int(re.sub(r'\D', '', car_data.get('odometer', '0'))),
                car_data.get('username', ''),
                car_data.get('phones', [''])[0],
                car_data.get('image_link', ''),
                car_data.get('images', len([])),
                car_data.get('car_number', ''),
                car_data.get('vin_code', ''),
                datetime.now()
            )
            cur.execute(sql_insert, values_insert)

    print('THE END_______________', cur)
    conn.commit()
    cur.close()
    conn.close()

def select_from_table(table_name):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows