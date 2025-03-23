import requests
import csv
import json
import time
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
import os
import random
import uuid



def generate_ymd_dates(start_dt_string, end_dt_string, format_string):
    start_dt = datetime.strptime(start_dt_string, format_string)
    end_dt = datetime.strptime(end_dt_string, format_string)

    interval = timedelta(days=1)
    current_dt = start_dt
    dates = []
    
    while current_dt <= end_dt:
        dates.append(current_dt)
        current_dt += interval
    return dates


def generate_intervals(start_dt_string, end_dt_string, minute):
    format_string = "%Y-%m-%d %H%M%S"
    start_dt = datetime.strptime(start_dt_string, format_string)
    end_dt = datetime.strptime(end_dt_string, format_string)

    interval = timedelta(minutes=minute)
    current_dt = start_dt
    intervals = []

    while current_dt <= end_dt:
        from_dt_string = current_dt.strftime(format_string)
        to_dt = current_dt + interval
        to_dt_string = to_dt.strftime(format_string)

        if to_dt <= end_dt:
            intervals.append((from_dt_string, to_dt_string))
        else:
            intervals.append((from_dt_string, end_dt_string))

        current_dt += interval

    return intervals


def convert_to_epoch_time(interval_list):
    epoch_intervals = []

    for from_dt_string, to_dt_string in interval_list:
        from_dt = datetime.strptime(from_dt_string, "%Y-%m-%d %H%M%S")
        to_dt = datetime.strptime(to_dt_string, "%Y-%m-%d %H%M%S")
        from_epoch = int(from_dt.timestamp())
        to_epoch = int(to_dt.timestamp())
        epoch_intervals.append([from_epoch, to_epoch])

    return epoch_intervals


def generate_iso8601_dates(start_dt_string, end_dt_string, dt_format, minute):
    start_dt = datetime.strptime(start_dt_string, dt_format)
    end_dt = datetime.strptime(end_dt_string, dt_format)
    
    interval = timedelta(minutes=minute)
    current_dt = start_dt
    intervals = []
    
    while current_dt <= end_dt:
        from_dt_string = current_dt.strftime(dt_format)
        to_dt = current_dt + interval
        to_dt_string = to_dt.strftime(dt_format)
        
        if to_dt <= end_dt:
            intervals.append([from_dt_string, to_dt_string])
        else:
            intervals.append([from_dt_string, end_dt_string])
        
        current_dt += interval
    
    return intervals


def generate_uuid_list(quantity):
    uuid_list = [str(uuid.uuid4()) for i in range(quantity + 1)]
    return uuid_list
    
    

if __name__ == '__main__':
    
    start_dt_string = "2020-01-01 000000"
    end_dt_string = "2023-07-31 235900"
    intervals = generate_intervals(start_dt_string, end_dt_string, 1440)
    print("Intervals (formatted):", intervals)
    with open('raw_dt_intervals.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for row in intervals:
            writer.writerow(row)
        file.close()

    epoch_intervals = convert_to_epoch_time(intervals)
    print("Intervals (epoch time):", epoch_intervals)
    with open('epoch_dt_intervals.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for row in epoch_intervals:
            writer.writerow(row)
        file.close()

    uuid_list = generate_uuid_list(1000)
    print(uuid_list)

