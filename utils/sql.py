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

    def check_if_ip_exists(self, ip):
        if ip == "192.168.1.1" or ip == "10.0.0.1":
            print("Coming from home, skipping")
            return True
        query = f"""
        SELECT ip FROM {os.getenv("CONNECTIONS_TABLE")} WHERE ip = '{ip}'
        """
        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            if len(records):
                print("ip exists")
                return True
            else:
                print("ip doesn't exist")
                return False
        except Exception as e:
            print(f"Error during check: {e}")

    def run_query(self):
        query = f"""
        SELECT * FROM {os.getenv("CONNECTIONS_TABLE")}
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
        query = f"""
        INSERT INTO {os.getenv("CONNECTIONS_TABLE")} (ip, latitude, longitude) VALUES (%s, %s, %s)
        """
        data_to_insert = (ip, latitude, longitude)

        try:
            self.cursor.execute(query, data_to_insert)
            self.connection.commit()
            print("Record inserted successfully")
        except Exception as e:
            print(f"Error: {e}")
            self.connection.rollback()
