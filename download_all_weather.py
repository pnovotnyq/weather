#!/usr/bin/env python
# coding: utf-8

from urllib.request import urlopen
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
        try:
            response = urlopen(url)
            stations = eval(response.read().decode())
            break
        except:
            response = urlopen(url)
            stations = eval(response.read().decode())
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


def download_measurements(station, db):
    """Download all available measurements for a station and store them in a database.
    
    Args:
        station (int/str): Weather station id
        db (str): Path to a database
        retry (int): How many times should it try to download data.
    """
    
    
    def download_measurement(url, retry = 10):
        print(url)
        attempts = 0
        while attempts <= retry:
            attempts += 1
            try:
                response = urlopen(url)
                measurements = eval(response.read().decode())
                break
            except:
                response = urlopen(url)
                measurements = eval(response.read().decode())
        if measurements:
            with sqlite3.connect(db) as conn:
                for measurement in measurements['items']:
                    for key in measurement.keys():
                        if key == 'weather_stn_id':
                            conn.execute("INSERT OR IGNORE INTO Measurements(stations_id) VALUES(:weather_stn_id)", measurement)
                        else:
                            query = f"INSERT OR IGNORE INTO Measurements({key}) VALUES (?)"
                            conn.execute(query, (measurement[key],))
        return measurements
    
    
        url = 'https://apex.oracle.com/pls/apex/raspberrypi/weatherstation/getlatestmeasurements/' + str(station)
        while True:
            try:
                next_page = download_measurement(url)['next']
            except KeyError:
                break
    return


def download_all_weather():
    args = parse_arguments()
    db = args.database
    stations = download_stations()
    populate_stations(stations, db)
    for station in stations:
        stations_id = station['weather_stn_id']
        print(f'Downloading data for station {station}.')
        download_measurements(stations_id, db)


if __name__ == '__main__':
    download_all_weather()