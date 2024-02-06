import os
from datetime import datetime, time as dtime, timedelta
import subprocess
from time import sleep

from dotenv import load_dotenv

load_dotenv()


def dump_database():
    try:
        database_url = os.getenv("DATABASE_URL")
        dump_folder = os.path.join(os.getcwd(), "dumps")

        os.makedirs(dump_folder, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        dump_filename = f"dump_{timestamp}.sql"
        dump_filepath = os.path.join(dump_folder, dump_filename)

        command = [
            "C:\\Program Files\\PostgreSQL\\16\\bin\\pg_dump.exe",
            f'--dbname={database_url}',
            f'--file={dump_filepath}',
            '--no-password',
        ]

        subprocess.run(command, check=True)

        print(f"Database dumped successfully to {dump_filepath}")

    except Exception as e:
        print(f"An error occurred during database dump: {str(e)}")


    except Exception as e:
        print(f"An error occurred during database dump: {str(e)}")


def schedule_daily_dump():
    dump_time_str = os.getenv("DUMP_TIME")
    dump_time = dtime(*map(int, dump_time_str.split(":")))

    while True:
        now = datetime.now().time()
        if now > dump_time:
            tomorrow = datetime.combine(datetime.now().date(), dump_time) + timedelta(days=1)
            time_until_next_dump = (tomorrow - datetime.now()).seconds
            sleep(time_until_next_dump)
        else:
            time_until_next_dump = (datetime.combine(datetime.now().date(), dump_time) - datetime.now()).seconds
            sleep(time_until_next_dump)

        dump_database()


if __name__ == "__main__":
    schedule_daily_dump()