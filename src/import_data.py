import requests
import gc
import psycopg2
import os
from models import Provider, Station, StationStatus, FreeBike, FreeBikeStatus

database_host = os.getenv('DB_HOST')
database_name = os.getenv('DB_NAME')
database_user = os.getenv('DB_USER')
database_password = os.getenv('DB_PASSWORD')

def import_data(data, context):
    conn = None
    try:
        conn = psycopg2.connect(host=database_host, database=database_name, user=database_user, password=database_password)
        create_tables(conn)
        import_providers(conn)
        import_stations(conn)
        import_free_bikes(conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    gc.collect()

def create_tables(conn):
        try:
            # Create table if not exists
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS providers (   id SERIAL PRIMARY KEY,   provider_id text,    lang text,   provider_name text,   vehicle_type text,   operator text,   provider_url text,   email text,   phone_number text,   timezone text );  CREATE TABLE stations (     id SERIAL PRIMARY KEY,     station_id text,     station_name text,     geolocation point,     station_address text,     region_id text,     post_code text,     provider_id integer REFERENCES providers(id) );  CREATE TABLE station_status (     id SERIAL PRIMARY KEY,     last_reported bigint,     station_id integer REFERENCES stations(id),     is_installed boolean,     is_renting boolean,     is_returning boolean,     num_bikes_available integer,     num_docks_available integer    );  CREATE TABLE free_bikes (   id SERIAL PRIMARY KEY,   bike_id text,   provider_id integer REFERENCES providers(id) );  CREATE TABLE free_bike_status (   id SERIAL PRIMARY KEY,   bike_id integer REFERENCES free_bikes(id),   last_reported bigint,   geolocation point,   is_disabled boolean,   is_reserved boolean );")
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

def import_providers(conn):
    # Get new stations
    response = requests.get('https://sharedmobility.ch/providers.json')
    current_providers = {}
    new_providers = []
    new_providers_json = response.json()['data']['providers']
    for new_provider_json in new_providers_json:
        new_providers.append(Provider(json_entry=new_provider_json))
    try:
        # Get current station information
        cur = conn.cursor()

        cur.execute('SELECT * FROM providers')

        for row in cur:
            current_providers[row[1]] = Provider(db_entry=row)
        cur.close()

        for new_provider in new_providers:
            if(new_provider.provider_id in current_providers and new_provider == current_providers[new_provider.provider_id]):
                # Provider is already up to date in the db
                pass
            elif(new_provider.provider_id not in current_providers):
                # Provider is not yet registered
                cur = conn.cursor()
                cur.execute("INSERT INTO providers (provider_id, lang, provider_name,vehicle_type,operator,provider_url,email,phone_number,timezone) VALUES(%s, %s, %s,%s, %s, %s,%s, %s,%s)",
                            (new_provider.provider_id, new_provider.lang, new_provider.provider_name, new_provider.vehicle_type, new_provider.operator, new_provider.provider_url, new_provider.email, new_provider.phone_number, new_provider.timezone))
                conn.commit()
                cur.close()
            elif(new_provider != current_providers[new_provider.provider_id]):
                cur = conn.cursor()
                cur.execute("UPDATE providers SET provider_id=%s, lang=%s, provider_name=%s,vehicle_type=%s,operator=%s,provider_url=%s,email=%s,phone_number=%s,timezone=%s WHERE id=%s",
                            (new_provider.provider_id, new_provider.lang, new_provider.provider_name, new_provider.vehicle_type, new_provider.operator, new_provider.provider_url, new_provider.email, new_provider.phone_number, new_provider.timezone,current_providers[new_provider.provider_id].id))
                conn.commit()
                cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def import_stations(conn):
    current_stations = {}
    current_stations_status = {}
    new_stations = []
    new_stations_status = []

    # Get new stations
    response_stations = requests.get('https://sharedmobility.ch/station_information.json')
    new_stations_json = response_stations.json()['data']['stations']
    for new_station_json in new_stations_json:
        new_stations.append(Station(json_entry=new_station_json))
    
    # Get new station status
    response_stations_status = requests.get('https://sharedmobility.ch/station_status.json')
    new_stations_status_json = response_stations_status.json()['data']['stations']
    for new_station_status_json in new_stations_status_json:
        new_stations_status.append(StationStatus(json_entry=new_station_status_json))
    
    try:
        # Get current station information
        cur_info = conn.cursor('station_info')
        cur_info.execute('SELECT stations.id,stations.station_id,stations.station_name,stations.geolocation,stations.station_address,stations.region_id,stations.post_code,providers.provider_id FROM stations INNER JOIN providers ON stations.provider_id = providers.id')
        for row in cur_info:
            current_stations[row[1]] = Station(db_entry=row)
        cur_info.close()

        # Get current status information
        cur_status = conn.cursor('station_status')
        cur_status.execute('SELECT DISTINCT ON (station_status.station_id) station_status.id,station_status.last_reported,station_status.is_installed,station_status.is_renting,station_status.is_returning,station_status.num_bikes_available,station_status.num_docks_available,stations.station_id FROM station_status INNER JOIN stations ON station_status.station_id = stations.id ORDER BY station_status.station_id, station_status.last_reported DESC')
        for row in cur_status:
            current_stations_status[row[7]] = StationStatus(db_entry=row)
        cur_status.close()

        # Update station info
        for new_station in new_stations:
            if(new_station.station_id in current_stations and new_station == current_stations[new_station.station_id]):
                # Provider is already up to date in the db
                pass
            elif(new_station.station_id not in current_stations):
                # Provider is not yet registered
                cur = conn.cursor()
                cur.execute("INSERT INTO stations (station_id, station_name, geolocation, station_address,region_id,post_code,provider_id) VALUES(%s, %s, point(%s,%s),%s, %s, %s, (SELECT id from providers WHERE provider_id=%s))",
                            (new_station.station_id, new_station.station_name, new_station.lat, new_station.lon, new_station.station_address, new_station.region_id, new_station.post_code, new_station.provider_id))
                conn.commit()
                cur.close()
            elif(new_station != current_stations[new_station.station_id]):
                cur = conn.cursor()
                cur.execute("UPDATE stations SET station_id=%s, station_name=%s, geolocation=point(%s,%s), station_address=%s,region_id=%s,post_code=%s,provider_id=(SELECT id from providers WHERE provider_id=%s) WHERE id=%s",
                            (new_station.station_id, new_station.station_name, new_station.lat, new_station.lon, new_station.station_address, new_station.region_id, new_station.post_code, new_station.provider_id,current_stations[new_station.station_id].id))
                conn.commit()
                cur.close()
        
        # Update station status
        for new_station_status in new_stations_status:
            if(new_station_status.station_id in current_stations_status and new_station_status == current_stations_status[new_station_status.station_id]):
                # Status is already up to date in the db
                pass
            else:
                # Status is not yet registered
                cur = conn.cursor()
                cur.execute("INSERT INTO station_status (last_reported, is_installed, is_renting, is_returning,num_bikes_available,num_docks_available,station_id) VALUES(%s, %s, %s,%s,%s, %s, (SELECT id from stations WHERE station_id=%s))",
                            (new_station_status.last_reported, new_station_status.is_installed, new_station_status.is_renting, new_station_status.is_returning, new_station_status.num_bikes_available, new_station_status.num_docks_available, new_station_status.station_id))
                conn.commit()
                cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def import_free_bikes(conn):
    current_bikes = {}
    current_bikes_status = {}
    new_bikes = []
    new_bikes_status = []

    # Get new stations
    response_bikes = requests.get('https://sharedmobility.ch/free_bike_status.json')
    json_data_bikes = response_bikes.json()
    update_time = json_data_bikes['last_updated']
    new_bikes_json = json_data_bikes['data']['bikes']
    for new_bike_json in new_bikes_json:
        # Exclude bird because the ids change over time which leads to a lot of useless data
        if new_bike_json.get('provider_id').startswith('bird'):
            continue
        new_bikes.append(FreeBike(json_entry=new_bike_json))
        new_bikes_status.append(FreeBikeStatus(json_entry=new_bike_json,last_reported=update_time))
    
    try:
        # Get current bike information
        cur_info = conn.cursor('free_info')
        cur_info.execute('SELECT free_bikes.id, free_bikes.bike_id,providers.provider_id FROM free_bikes INNER JOIN providers ON free_bikes.provider_id = providers.id')
        for row in cur_info:
            current_bikes[row[1]] = FreeBike(db_entry=row)
        cur_info.close()

        # Get current status information
        cur_status = conn.cursor('free_status')
        cur_status.execute('SELECT DISTINCT ON (free_bike_status.bike_id) free_bike_status.id,free_bike_status.last_reported,free_bike_status.geolocation,free_bike_status.is_disabled,free_bike_status.is_reserved,free_bikes.bike_id FROM free_bike_status INNER JOIN free_bikes ON free_bike_status.bike_id = free_bikes.id ORDER BY free_bike_status.bike_id, free_bike_status.last_reported DESC')
        for row in cur_status:
            current_bikes_status[row[5]] = FreeBikeStatus(db_entry=row)
        cur_status.close()

        # Update station info
        for new_bike in new_bikes:
            if(new_bike.bike_id in current_bikes and new_bike == current_bikes[new_bike.bike_id]):
                # Bike is already up to date in the db
                pass
            elif(new_bike.bike_id not in current_bikes):
                # Provider is not yet registered
                cur = conn.cursor()
                cur.execute("INSERT INTO free_bikes (bike_id, provider_id) VALUES(%s, (SELECT id from providers WHERE provider_id=%s))",
                            (new_bike.bike_id, new_bike.provider_id))
                conn.commit()
                cur.close()
        # Update station status
        for new_bike_status in new_bikes_status:
            if(new_bike_status.bike_id in current_bikes_status and new_bike_status == current_bikes_status[new_bike_status.bike_id]):
                # Status is already up to date in the db
                pass
            else:
                # Status is not yet registered
                cur = conn.cursor()
                cur.execute("INSERT INTO free_bike_status (last_reported, geolocation, is_disabled, is_reserved,bike_id) VALUES(%s, point(%s,%s), %s,%s, (SELECT id from free_bikes WHERE bike_id=%s))",
                            (new_bike_status.last_reported, new_bike_status.lat, new_bike_status.lon, new_bike_status.is_disabled, new_bike_status.is_reserved, new_bike_status.bike_id))
                conn.commit()
                cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)