import requests
from bs4 import BeautifulSoup
import csv
import json
import re
import time
from datetime import date, datetime, timedelta
import math
import pandas as pd
import os
import random
import urllib3
import socket
import itertools
import hashlib
from zipfile import ZipFile
import multiprocessing
from multiprocessing import Manager
from requests.exceptions import ChunkedEncodingError, ConnectionError
from http.client import IncompleteRead
from epoch_converter import generate_intervals, convert_to_epoch_time, generate_ymd_dates, generate_iso8601_dates, generate_uuid_list


def download_job(url, sub, fn):
    response = requests.get(url)
    if response.status_code == 200:
        filepath = os.path.join(sub, fn)
        with open(filepath, 'wb') as file:
            file.write(response.content)
        result_text = f'File {fn} successfully downloaded!'
    elif response.status_code in [403, 404]:
        result_text = f'The specified file {fn} does not exist. Try downloading the next one...'
    else:
        result_text = f'Unknown error code {response.status_code} encountered!'

    time.sleep(0.5)
    return result_text


def get_hsioptions_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hsi options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsio{date}.zip' for date in dates]
    reg_fns = [f'hsio{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsioa{date}.zip' for date in dates]
    night_fns = [f'hsioa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')
    
    
def get_hsifutures_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hsi futures reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsif{date}.zip' for date in dates]
    reg_fns = [f'hsif{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsifa{date}.zip' for date in dates]
    night_fns = [f'hsifa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_hsifutures_options_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hsifutures options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/phso{date}.zip' for date in dates]
    reg_fns = [f'phso{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/phsoa{date}.zip' for date in dates]
    night_fns = [f'phsoa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_weekly_hsioptions_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'weekly hsi options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsiwo{date}.zip' for date in dates]
    reg_fns = [f'hsiwo{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hsiwoa{date}.zip' for date in dates]
    night_fns = [f'hsiwoa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_minihsioptions_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'mini hsi options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/mhio{date}.zip' for date in dates]
    reg_fns = [f'mhio{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/mhioa{date}.zip' for date in dates]
    night_fns = [f'mhioa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_minihsifutures_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'mini hsi futures reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/mhif{date}.zip' for date in dates]
    reg_fns = [f'mhif{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/mhifa{date}.zip' for date in dates]
    night_fns = [f'mhifa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_htifutures_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hti futures reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/htif{date}.zip' for date in dates]
    reg_fns = [f'htif{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/htifa{date}.zip' for date in dates]
    night_fns = [f'htifa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_htioptions_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hti options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/htio{date}.zip' for date in dates]
    reg_fns = [f'htio{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/htioa{date}.zip' for date in dates]
    night_fns = [f'htioa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_htifutures_options_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'htifutures options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/pteo{date}.zip' for date in dates]
    reg_fns = [f'pteo{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/pteoa{date}.zip' for date in dates]
    night_fns = [f'pteoa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_hhifutures_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hhi futures reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hhif{date}.zip' for date in dates]
    reg_fns = [f'hhif{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hhifa{date}.zip' for date in dates]
    night_fns = [f'hhifa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_hhioptions_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hhi options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hhio{date}.zip' for date in dates]
    reg_fns = [f'hhio{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hhioa{date}.zip' for date in dates]
    night_fns = [f'hhioa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_hhifutures_options_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hhifutures options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    reg_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/phho{date}.zip' for date in dates]
    reg_fns = [f'phho{date}.zip' for date in dates]
    for url, fn in zip(reg_urls, reg_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()

    night_urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/phhoa{date}.zip' for date in dates]
    night_fns = [f'phhoa{date}.zip' for date in dates]
    for url, fn in zip(night_urls, night_fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_vhsifutures_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'vhsi futures reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/vhsf{date}.zip' for date in dates]
    fns = [f'vhsf{date}.zip' for date in dates]
    for url, fn in zip(urls, fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_hiborfutures_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'hibor futures reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/hibor{date}.zip' for date in dates]
    fns = [f'hibor{date}.zip' for date in dates]
    for url, fn in zip(urls, fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')



def get_stockfutures_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'stock futures reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/stock{date}.zip' for date in dates]
    fns = [f'stock{date}.zip' for date in dates]
    for url, fn in zip(urls, fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')


def get_stockoptions_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    sub = 'stock options reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    urls = [f'https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/dqe{date}.zip' for date in dates]
    fns = [f'dqe{date}.zip' for date in dates]
    for url, fn in zip(urls, fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports zip files successfully downloaded. Now unzipping all downloaded files...')
    files = os.listdir(sub)
    for file in files:
        if file[-3:] == 'zip':
            filepath = os.path.join(sub, file)
            print(f'Extracting {file}...')
            with ZipFile(filepath, 'r') as z:
                z.extractall(path=sub)
            os.remove(filepath)

    print('All files successfully unzipped.')



def get_mainboard_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    urls = [f'https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{date}e.htm' for date in dates]
    fns = [f'mainboard_{date}.htm' for date in dates]
    print(urls)
    sub = 'mainboard reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    for url, fn in zip(urls, fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports htm files successfully downloaded.')


def get_gem_daily_reports(start, end, dt_format, core_num):
    raw_dates = generate_ymd_dates(start, end, dt_format)
    dates = [date.strftime(dt_format)[2:] for date in raw_dates if date.weekday() not in set([5, 6])]
    urls = [f'https://www.hkex.com.hk/eng/stat/smstat/dayquot/GEM/e_G{date}.htm' for date in dates]
    fns = [f'e_G{date}.htm' for date in dates]

    sub = 'gem reports'
    os.makedirs(sub, exist_ok=True)

    def callback(result):
        print(result)

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []

    for url, fn in zip(urls, fns):
        job = pool.apply_async(download_job, args=(url, sub, fn), callback=callback)
        jobs.append(job)

    for job in jobs:
        job.get()

    print('All reports htm files successfully downloaded.')

    
        
        
if __name__ == '__main__':
    
    dt_format = '%Y%m%d'
    start = '20231128'
    end = '20240219'  #datetime.today().strftime(dt_format)
    core_num = 10

    get_hsioptions_daily_reports(start, end, dt_format, 10)
    get_hsifutures_daily_reports(start, end, dt_format, 10)
    get_hsifutures_options_daily_reports(start, end, dt_format, 10)
    get_weekly_hsioptions_daily_reports(start, end, dt_format, 10)
    get_minihsioptions_daily_reports(start, end, dt_format, 10)
    get_minihsifutures_daily_reports(start, end, dt_format, 10)
    get_htifutures_daily_reports(start, end, dt_format, 10)
    get_htioptions_daily_reports(start, end, dt_format, 10)
    get_htifutures_options_daily_reports(start, end, dt_format, 10)
    get_hhifutures_daily_reports(start, end, dt_format, 10)
    get_hhioptions_daily_reports(start, end, dt_format, 10)
    get_hhifutures_options_daily_reports(start, end, dt_format, 10)

    get_vhsifutures_daily_reports(start, end, dt_format, 10)
    get_hiborfutures_daily_reports(start, end, dt_format, 10)

    get_stockfutures_daily_reports(start, end, dt_format, 10)
    get_stockoptions_daily_reports(start, end, dt_format, 10)
    get_mainboard_daily_reports(start, end, dt_format, 10)
    get_gem_daily_reports(start, end, dt_format, 10)
        
