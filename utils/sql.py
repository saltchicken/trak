import psycopg2
import os
from dotenv import load_dotenv
import ipaddress


def check_if_ip_is_LAN(ip):
    lan_ranges = [
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("172.16.0.0/12"),
        ipaddress.ip_network("192.168.0.0/16"),
    ]
    # Convert IP to IPv4Address object
    target_ip = ipaddress.ip_address(ip)
    # Check if the IP is in any of the LAN ranges
    return any(target_ip in network for network in lan_ranges)


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
        if check_if_ip_is_LAN(ip):
            print(f"Coming from home, skipping: {ip}")
            return True
        query = f"""
        SELECT ip FROM {os.getenv("CONNECTIONS_TABLE")} WHERE ip = '{ip}'
        """
        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            if len(records):
                return True
            else:
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

    def insert_log(
        self,
        ip,
        remote_user,
        timestamp,
        method,
        url,
        status_code,
        response_size,
        referrer,
        user_agent,
        payload,
    ):
        query = f"""
        INSERT INTO {os.getenv("LOG_MESSAGES_TABLE")} (ip, remote_user, timestamp, method, url, status_code, response_size, referrer, user_agent, payload) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data_to_insert = (
            ip,
            remote_user,
            timestamp,
            method,
            url,
            status_code,
            response_size,
            referrer,
            user_agent,
            payload,
        )

        try:
            self.cursor.execute(query, data_to_insert)
            self.connection.commit()
        except Exception as e:
            print(f"Error: {e}")
            self.connection.rollback()

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
