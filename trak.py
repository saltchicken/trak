import subprocess
import re
import pickle
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

host = "localhost"
database = os.getenv("SQL_DATABASE")
user = os.getenv("SQL_USER")
password = os.getenv("SQL_PASSWORD")

connection = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password,
)

cursor = connection.cursor()


def run_query():
    query = """
    SELECT * FROM trak_dev.connections
    """
    try:
        cursor.execute(query)
        records = cursor.fetchall()

        print("Records from the table")
        for row in records:
            print(row)
    except Exception as e:
        print(f"Error during retrieval: {e}")


def insert_connection(ip, latitude, longitude):
    query = """
    INSERT INTO trak_dev.connections (ip, latitude, longitude) VALUES (%s, %s, %s)
    """
    data_to_insert = (ip, latitude, longitude)

    try:
        cursor.execute(query, data_to_insert)
        connection.commit()
        print("Record inserted successfully")
    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()


def load_seen_ips(file_path):
    """Load the set of seen IPs from a file."""
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return pickle.load(f)
    return set()  # Return an empty set if the file doesn't exist


def save_seen_ips(file_path, seen_ips):
    """Save the set of seen IPs to a file."""
    with open(file_path, "wb") as f:
        pickle.dump(seen_ips, f)


def get_coordinates(ip):
    """Fetch latitude and longitude for the given IP address using GeoIP service."""
    try:
        # Run curl command to get geo information
        command = f"curl -s https://json.geoiplookup.io/{ip} | jq -r '[.latitude, .longitude] | @csv'"
        result = subprocess.check_output(command, shell=True, text=True)
        return result.strip().split(",")  # Return the result as a string
    except subprocess.CalledProcessError as e:
        print(f"Error fetching coordinates for {ip}: {e}")
        return None, None


def tail_f(file_path, seen_ips_file):
    process = subprocess.Popen(
        ["tail", "-f", "-n", "-0", file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Regular expression pattern for extracting fields from Nginx access log
    log_pattern = re.compile(
        r"(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<timestamp>[^\]]+)\] "
        r'"(?P<method>[A-Z]+) (?P<url>[^"]+) HTTP/\d\.\d" '
        r"(?P<status_code>\d+) (?P<response_size>\d+) "
        r'"(?P<referrer>[^"]+)" "(?P<user_agent>[^"]+)"'
    )

    seen_ips = load_seen_ips(seen_ips_file)  # Load previously seen IPs

    try:
        while True:
            line = process.stdout.readline()
            if line:
                line = line.strip()
                match = log_pattern.search(line)
                if match:
                    ip = match.group("ip")
                    timestamp = match.group("timestamp")
                    method = match.group("method")
                    url = match.group("url")
                    status_code = match.group("status_code")
                    response_size = match.group("response_size")
                    referrer = match.group("referrer")
                    user_agent = match.group("user_agent")

                    # Check if the IP address has been seen before
                    if ip in seen_ips:
                        print(f"Duplicate IP detected: {ip} at {timestamp}.")
                    else:
                        # Add the IP to the set
                        seen_ips.add(ip)
                        print(
                            f"New IP: {ip}, Timestamp: {timestamp}, Method: {method}, URL: {url}, "
                            f"Status Code: {status_code}, Response Size: {response_size}, "
                            f"Referrer: {referrer}, User Agent: {user_agent}"
                        )
                        latitude, longitude = get_coordinates(ip)
                        if latitude and longitude:
                            insert_connection(ip, latitude, longitude)
                            # print(f"Coordinates for {ip}: {coordinates}")
                            # print(ip)
                            # print(latitude)
                            # print(longitude)
            else:
                break
    except KeyboardInterrupt:
        print("Stopping tail -f.")
    finally:
        process.terminate()
        process.wait()
        save_seen_ips(seen_ips_file, seen_ips)  # Save seen IPs before exiting


if __name__ == "__main__":
    run_query()
    # insert_query()
    seen_ips_file = "seen_ips.pkl"  # File to save the set of seen IPs
    tail_f("/var/log/nginx/access.log", seen_ips_file)
