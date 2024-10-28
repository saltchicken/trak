#!/bin/bash

source venv/bin/activate

while true; do
  python trak.py --update_logs
  python trak.py --update_db
  sleep 5
done
