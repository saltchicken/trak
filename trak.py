import subprocess
import re
import pickle
import os
import pandas as pd
from utils import SQL_Cursor

from dataclasses import dataclass, asdict


@dataclass
class Connection:
    ip: str
    timestamp: str
    method: str
    url: str
    status_code: str
    response_size: str
    referrer: str
    user_agent: str

    def __str__(self):
        return f"""
        New IP: {self.ip}
        Timestamp: {self.timestamp}
        Method: {self.method}
        URL: {self.url}
        Status Code: {self.status_code}
        Response Size: {self.response_size}
        Referrer: {self.referrer}
        User Agent: {self.user_agent}
        """


sql_cursor = SQL_Cursor()


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


def parse_line(line):
    log_pattern = re.compile(
        r"(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<timestamp>[^\]]+)\] "
        r'"(?P<method>[A-Z]+) (?P<url>[^"]+) HTTP/\d\.\d" '
        r"(?P<status_code>\d+) (?P<response_size>\d+) "
        r'"(?P<referrer>[^"]+)" "(?P<user_agent>[^"]+)"'
    )
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
        connection = Connection(
            ip, timestamp, method, url, status_code, response_size, referrer, user_agent
        )
        return connection
    else:
        print(line)
        print("ERROR: Parse line did not find match")
        return None


def tail_f(file_path, seen_ips_file):
    process = subprocess.Popen(
        ["tail", "-f", "-n", "-0", file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Regular expression pattern for extracting fields from Nginx access log

    seen_ips = load_seen_ips(seen_ips_file)  # Load previously seen IPs

    try:
        while True:
            line = process.stdout.readline()
            if line:
                connection = parse_line(line)
                if connection:
                    # Check if the IP address has been seen before
                    if connection.ip in seen_ips:
                        print(
                            f"Duplicate IP detected: {connection.ip} at {connection.timestamp}."
                        )
                    else:
                        # Add the IP to the set
                        print(connection)
                        seen_ips.add(connection.ip)
                        latitude, longitude = get_coordinates(connection.ip)
                        if latitude and longitude:
                            sql_cursor.insert_connection(
                                connection.ip, latitude, longitude
                            )
            else:
                break
    except KeyboardInterrupt:
        print("Stopping tail -f.")
    finally:
        process.terminate()
        process.wait()
        save_seen_ips(seen_ips_file, seen_ips)  # Save seen IPs before exiting


def log_parser():
    log_file = "/var/log/nginx/access.log"

    with open(log_file, "r") as f:
        log_entries = []
        for line in f:
            connection = parse_line(line)
            if connection:
                log_entries.append(asdict(connection))
            else:
                print("ERROR: Unable to parse log line properly. Skipping")

    logs_df = pd.DataFrame(log_entries)
    print(logs_df.head())  # TODO: Delete this
    return logs_df


if __name__ == "__main__":
    # sql_cursor.run_query()
    # insert_query()
    #
    # seen_ips_file = "seen_ips.pkl"  # File to save the set of seen IPs
    # tail_f("/var/log/nginx/access.log", seen_ips_file)
    log_parser()
