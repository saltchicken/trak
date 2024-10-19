import psycopg2
import os
from dotenv import load_dotenv


class SQL_Cursor:
    def __init__(self):
        load_dotenv()
        self.connection = psycopg2.connect(
            host="localhost",
            database=os.getenv("SQL_DATABASE"),
            user=os.getenv("SQL_USER"),
            password=os.getenv("SQL_PASSWORD"),
        )

        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def run_query(self):
        query = """
        SELECT * FROM trak_dev.connections
        """
        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()

            print("Records from the table")
            for row in records:
                print(row)
        except Exception as e:
            print(f"Error during retrieval: {e}")

    def insert_connection(self, ip, latitude, longitude):
        query = """
        INSERT INTO trak_dev.connections (ip, latitude, longitude) VALUES (%s, %s, %s)
        """
        data_to_insert = (ip, latitude, longitude)

        try:
            self.cursor.execute(query, data_to_insert)
            self.connection.commit()
            print("Record inserted successfully")
        except Exception as e:
            print(f"Error: {e}")
            self.connection.rollback()
