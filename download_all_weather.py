#!/usr/bin/env python
# coding: utf-8

import requests
from collections import defaultdict
import sqlite3
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
            description = """Download all weather stations information and
            store in a database.""")
    parser.add_argument(
            'database',
            help='Path to a database.',)
    parser.add_argument(
            '--verbose','-v',
            help='increase output verbosity',
            action='store_true')
    args = parser.parse_args()
    return args


def download_stations(url='https://apex.oracle.com/pls/apex/raspberrypi/weatherstation/getallstations', retry = 10):
    """Download weather station info and return a list of dictionaries.
    
    Args:
        url (str): Base of the URL to retrieve stations from (REST API).
        retry (int): Number of retries on failed downloads.
    Returns:
        list: [{'weather_stn_id': int, 'weather_stn_name': str, 'weather_stn_lat': int, 'weather_stn_long': int}]
    """
    attempts = 0
    while attempts <= retry:
        attempts += 1
        with requests.Session() as session:
            try:
                response = session.get(url)
                stations = eval(response.text)
                break
            except:
                response = session.get(url)
                stations = eval(response.text)
    return stations['items']


def populate_stations(stations, db):
    """Enter stations info into a database.
    
    Args:
        stations (list): [{'weather_stn_id': int, 'weather_stn_name': str, 'weather_stn_lat': int, 'weather_stn_long': int}]
        db (str): Path to the database
    Returns:
        sqlite3.connection: sqlite3 connection to the database 
    """
    with sqlite3.connect(db) as conn:
        conn.executescript("""
        PRAGMA foreign_keys = ON;
        
        CREATE TABLE IF NOT EXISTS Stations(
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Measurements(
        air_pressure REAL,
        air_quality REAL,
        ambient_temp REAL,
        created_by TEXT,
        created_on TEXT,
        ground_temp REAL,
        humidity REAL,
        id INTEGER PRIMARY KEY,
        rainfall INTEGER,
        reading_timestamp TEXT,
        updated_by TEXT,
        updated_on TEXT,
        stations_id INT,
        wind_direction INTEGER,
        wind_gust_speed INTEGER,
        wind_speed INTEGER,
        FOREIGN KEY (stations_id) REFERENCES Stations(id)
        );
        """)
        for station in stations:
            conn.execute("""
            INSERT OR IGNORE INTO Stations
            VALUES(:weather_stn_id, :weather_stn_name, :weather_stn_lat, :weather_stn_long)""", station)
    return conn    


def download_measurement(url, conn, session):
    with session:
        response = session.get(url)
    measurements = eval(response.text)
    for data in measurements['items']:
        measurement = defaultdict(type(None))
        for reading, value in data.items():
            measurement[reading] = value
        query = """
        INSERT OR IGNORE INTO Measurements(
            air_pressure,
            air_quality,
            ambient_temp,
            created_by,
            created_on,
            ground_temp,
            humidity,
            id,
            rainfall,
            reading_timestamp,
            updated_by,
            updated_on,
            stations_id,
            wind_direction,
            wind_gust_speed,
            wind_speed)
        VALUES(
            :air_pressure,
            :air_quality,
            :ambient_temp,
            :created_by,
            :created_on,
            :ground_temp,
            :humidity,
            :id,
            :rainfall,
            :reading_timestamp,
            :updated_by,
            :updated_on,
            :weather_stn_id,
            :wind_direction,
            :wind_gust_speed,
            :wind_speed)"""
        conn.execute(query, measurement)
    return measurements.get('next', dict()).get('$ref', None)

        
def download_all_weather():
    args = parse_arguments()
    db = args.database
    stations = download_stations()
    populate_stations(stations, db)
    with requests.Session() as session:
        for station in stations:
            stations_id = station['weather_stn_id']
            print(f"Downloading data for station {stations_id}.")
            url = 'https://apex.oracle.com/pls/apex/raspberrypi/weatherstation/getlatestmeasurements/' + str(stations_id)
            with sqlite3.connect(db) as conn:
                while True:
                    if url:
                        print(url)
                        url = download_measurement(url, conn, session)
                    else:
                        break

if __name__ == '__main__':
    download_all_weather()
