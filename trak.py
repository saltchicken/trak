import subprocess
import time
import re
import pickle
import os
import sys
import argparse
from loguru import logger
import pandas as pd
from utils import SQL_Cursor

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class Connection:
    ip: str
    remote_user: Optional[str]
    timestamp: str
    method: Optional[str]
    url: Optional[str]
    status_code: Optional[str]
    response_size: str
    referrer: str
    user_agent: str
    payload: Optional[str]

    def __str__(self):
        return f"""
        New IP: {self.ip}
        remote_user: {self.remote_user}
        Timestamp: {self.timestamp}
        Method: {self.method}
        URL: {self.url}
        Status Code: {self.status_code}
        Response Size: {self.response_size}
        Referrer: {self.referrer}
        User Agent: {self.user_agent}
        Payload: {self.payload}
        """


sql_cursor = SQL_Cursor()


def get_coordinates(ip):
    """Fetch latitude and longitude for the given IP address using GeoIP service."""
    try:
        # Run curl command to get geo information
        command = f"curl -s https://json.geoiplookup.io/{ip} | jq -r '[.latitude, .longitude] | @csv'"
        result = subprocess.check_output(command, shell=True, text=True)
        return result.strip().split(",")  # Return the result as a string
    except subprocess.CalledProcessError as e:
        logger.error(f"Error fetching coordinates for {ip}: {e}")
        return None, None


valid_request_log_pattern = re.compile(
    r'(?P<ip>\d+\.\d+\.\d+\.\d+) - (?P<remote_user>[A-Z-]+) \[(?P<timestamp>[^\]]+)\] "(?P<method>[A-Z]+) (?P<url>[^"]+) HTTP/\d\.\d" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>[^"]+)" "(?P<user_agent>[^"]+)"'
)
invalid_request_log_pattern = re.compile(
    r'(?P<ip>\d+\.\d+\.\d+\.\d+) - (?P<remote_user>[A-Z-]+) \[(?P<timestamp>[^\]]+)\] "(?P<request>[^"]*)" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>[^"]*)" "(?P<user_agent>[^"]*)"'
)


def parse_line(line):
    line = line.strip()
    match = valid_request_log_pattern.search(line)
    if match:
        ip = match.group("ip")
        remote_user = (
            None if match.group("remote_user") == "-" else match.group("remote_user")
        )
        timestamp = match.group("timestamp")
        method = match.group("method")
        url = match.group("url")
        status_code = match.group("status_code")
        response_size = match.group("response_size")
        referrer = match.group("referrer")
        user_agent = match.group("user_agent")
        payload = None
        connection = Connection(
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
        return connection
    else:
        logger.debug(
            "Line did not match valid_request_log_pattern. Trying invalid_request_log_pattern"
        )
        match = invalid_request_log_pattern.search(line)
        if match:
            ip = match.group("ip")
            remote_user = (
                None
                if match.group("remote_user") == "-"
                else match.group("remote_user")
            )
            timestamp = match.group("timestamp")
            method = None
            url = None
            status_code = None
            response_size = match.group("response_size")
            referrer = match.group("referrer")
            user_agent = match.group("user_agent")
            payload = match.group("request")
            connection = Connection(
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
            return connection
        else:
            logger.error("Line did not match invalid_request_log_pattern")
            print(line)

            return None


def tail_f(file_path):
    process = subprocess.Popen(
        ["tail", "-f", "-n", "-0", file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Regular expression pattern for extracting fields from Nginx access log

    try:
        while True:
            line = process.stdout.readline()
            if line:
                connection = parse_line(line)
                if connection:
                    # Check if the IP address has been seen before
                    if sql_cursor.check_if_ip_exists(connection.ip):
                        logger.info(
                            f"Duplicate IP detected: {connection.ip} at {connection.timestamp}."
                        )
                    else:
                        # Add the IP to the set
                        logger.info(f"Running gps on {connection}")
                        latitude, longitude = get_coordinates(connection.ip)
                        if latitude and longitude:
                            sql_cursor.insert_connection(
                                connection.ip, latitude, longitude
                            )
            else:
                break
    except KeyboardInterrupt:
        logger.debug("Stopping tail -f.")
    finally:
        process.terminate()
        process.wait()


def log_parser(log_file_path):
    log_file = log_file_path
    failed_lines = []

    with open(log_file, "r") as f:
        log_entries = []
        for line in f:
            connection = parse_line(line)
            if connection:
                log_entries.append(asdict(connection))
            else:
                failed_lines.append(line)
                logger.error("Unable to parse log line properly. Skipping")

    logs_df = pd.DataFrame(log_entries)
    with open("failed_lines.txt", "w") as file:
        for line in failed_lines:
            file.write(line + "\n")
    return logs_df


# trak_dev.log_messages


def insert_log_message_into_table(logs_df):
    for index, connection in logs_df.iterrows():
        sql_cursor.insert_log(
            connection.ip,
            connection.remote_user,
            connection.timestamp,
            connection.method,
            connection.url,
            connection.status_code,
            connection.response_size,
            connection.referrer,
            connection.user_agent,
            connection.payload,
        )


def insert_into_table(logs_df):
    for ip in logs_df["ip"].unique():
        if sql_cursor.check_if_ip_exists(ip):
            logger.info(f"Record already exists for {ip}")
        else:
            logger.info(f"Running GPS on {ip}")
            latitude, longitude = get_coordinates(ip)
            time.sleep(0.5)
            if latitude and longitude:
                sql_cursor.insert_connection(ip, latitude, longitude)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description="Process log files from nginx access log"
    )
    arg_parser.add_argument("--realtime", action="store_true", help="Run with tail -f")
    arg_parser.add_argument(
        "--print",
        choices=["all", "unique_ips", "status_code"],
        help="Choose which data to print: all, unique_ips, status_code",
    )
    arg_parser.add_argument("--code", help="Specific status_code")
    arg_parser.add_argument("--debug", action="store_true", help="Print debug messages")
    arg_parser.add_argument(
        "--update_db", action="store_true", help="This will parse log and update DB"
    )

    arg_parser.add_argument(
        "--update_logs", action="store_true", help="This will parse log and update DB"
    )
    args = arg_parser.parse_args()

    if not args.debug:
        logger.remove()
        logger.add(sys.stdout, level="INFO")

    if args.realtime:
        tail_f("/var/log/nginx/access.log")

    elif args.update_db:
        logs_df = log_parser("/var/log/nginx/access.log")
        insert_into_table(logs_df)

    elif args.update_logs:
        logs_df = log_parser("/var/log/nginx/access.log")
        insert_log_message_into_table(logs_df)

    else:
        if args.print:
            logs_df = log_parser("/var/log/nginx/access.log")
            if args.print == "all":
                print(logs_df)
            elif args.print == "unique_ips":
                print(logs_df["ip"].unique())
            elif args.print == "status_code":
                if args.code:
                    print(logs_df.loc[logs_df["status_code"] == args.code])
                else:
                    print(logs_df["status_code"])
        else:
            print("Argument for --print is required for non-realtime analysis")
