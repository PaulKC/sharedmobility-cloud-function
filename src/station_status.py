import os
import psycopg2
import json
from json import JSONEncoder
from models import StationStatus, FreeBikeStatus

database_host = os.getenv('DB_HOST')
database_name = os.getenv('DB_NAME')
database_user = os.getenv('DB_USER')
database_password = os.getenv('DB_PASSWORD')

class StatusEncoder(JSONEncoder):
        def default(self, o):
            return o.__dict__
def get_historic_station_status(station_id,from_date=None,to_date=None):
    conn = None
    try:
        # Get current station information
        conn = psycopg2.connect(
            host=database_host, database=database_name, user=database_user, password=database_password)
        cur = conn.cursor()
        query = 'SELECT station_status.id,station_status.last_reported,station_status.is_installed,station_status.is_renting,station_status.is_returning,station_status.num_bikes_available,station_status.num_docks_available,stations.station_id FROM station_status INNER JOIN stations ON station_status.station_id = stations.id WHERE stations.station_id=%s'
        if(from_date and to_date):
            query+=' AND last_reported > %s AND last_reported < %s'
            query+=' ORDER BY last_reported ASC;'
            cur.execute(query,(station_id,from_date,to_date))
        elif(from_date):
            query+=' AND last_reported > %s'
            query+=' ORDER BY last_reported ASC;'
            cur.execute(query,(station_id,from_date))
        elif(to_date):
            query+=' AND last_reported < %s'
            query+=' ORDER BY last_reported ASC;'
            cur.execute(query,(station_id,to_date))
        else:
            query+=' ORDER BY last_reported ASC;'
            cur.execute(query,(station_id,))
        station_history = []
        # If no entry was available in the defined timespan return the newest entry
        if cur.rowcount <= 0:
            cur.execute('SELECT station_status.id,station_status.last_reported,station_status.is_installed,station_status.is_renting,station_status.is_returning,station_status.num_bikes_available,station_status.num_docks_available,stations.station_id FROM station_status INNER JOIN stations ON station_status.station_id = stations.id WHERE stations.station_id=%s ORDER BY last_reported DESC LIMIT 1;',(station_id,))
        for row in cur:
            station_history.append(StationStatus(db_entry=row))
        cur.close()
        cur = conn.cursor()
        # If available get the entry right before the requested timespan
        if len(station_history) > 0:
            cur.execute('SELECT station_status.id,station_status.last_reported,station_status.is_installed,station_status.is_renting,station_status.is_returning,station_status.num_bikes_available,station_status.num_docks_available,stations.station_id FROM station_status INNER JOIN stations ON station_status.station_id = stations.id WHERE stations.station_id=%s AND station_status.id < %s ORDER BY station_status.id DESC LIMIT 1;',(station_id,station_history[0].id))
            if cur.rowcount > 0:
                station_history.insert(0,StationStatus(db_entry=cur.fetchone()))
        cur.close()
        return StatusEncoder().encode(station_history)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_status(request):
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    id_in_path = request.path[1:]
    if "/" in id_in_path:
        raise ValueError("Station id not valid")
    station_id = id_in_path
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    headers = {
        'Access-Control-Allow-Origin': '*'
    }
    return (get_historic_station_status(station_id,from_date,to_date),200,headers)
