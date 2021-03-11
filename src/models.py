from ast import literal_eval

class Provider:
    def __eq__(self, other):
        return (self.provider_id == other.provider_id and 
        self.lang == other.lang and 
        self.provider_name == other.provider_name and 
        self.vehicle_type == other.vehicle_type and
        self.operator == other.operator and 
        self.provider_url == other.provider_url and 
        self.email == other.email and
        self.phone_number == other.phone_number and
        self.timezone == other.timezone
        )

    def __str__(self):
        return self.provider_name

    def __init__(self, db_entry=None, json_entry=None):
        if db_entry:
            self.id = db_entry[0]
            self.provider_id = db_entry[1]
            self.lang = db_entry[2]
            self.provider_name = db_entry[3]
            self.vehicle_type = db_entry[4]
            self.operator = db_entry[5]
            self.provider_url = db_entry[6]
            self.email = db_entry[7]
            self.phone_number = db_entry[8]
            self.timezone = db_entry[9]
        if json_entry:
            self.provider_id = json_entry.get('provider_id')
            self.lang = json_entry.get('language')
            self.provider_name = json_entry.get('name')
            self.vehicle_type = json_entry.get('vehicle_type')
            self.operator = json_entry.get('operator')
            self.provider_url = json_entry.get('url')
            self.email = json_entry.get('email')
            self.phone_number = json_entry.get('phone_number')
            self.timezone = json_entry.get('timezone')

class Station:
    def __eq__(self, other):
        return (self.station_id == other.station_id and 
        self.station_name == other.station_name and 
        self.lat == other.lat and 
        self.lon == other.lon and 
        self.station_address == other.station_address and
        self.region_id == other.region_id and 
        self.post_code == other.post_code and 
        self.provider_id == other.provider_id
        )

    def __str__(self):
        return self.station_name

    def __init__(self, db_entry=None, json_entry=None):
        if db_entry:
            self.id = db_entry[0]
            self.station_id = db_entry[1]
            self.station_name = db_entry[2]
            geocoordinates = literal_eval(db_entry[3])
            self.lat = geocoordinates[0]
            self.lon = geocoordinates[1]
            self.station_address = db_entry[4]
            self.region_id = db_entry[5]
            self.post_code = db_entry[6]
            self.provider_id = db_entry[7]
        if json_entry:
            self.station_id = json_entry.get('station_id')
            self.station_name = json_entry.get('name')
            self.lat = json_entry.get('lat')
            self.lon = json_entry.get('lon')
            self.station_address = json_entry.get('address')
            self.region_id = json_entry.get('region_id')
            self.post_code = json_entry.get('post_code')
            self.provider_id = json_entry.get('provider_id')

class StationStatus:
    def __eq__(self, other):
        return (self.station_id == other.station_id and 
        self.is_installed == other.is_installed and 
        self.is_renting == other.is_renting and 
        self.is_returning == other.is_returning and 
        self.num_bikes_available == other.num_bikes_available and
        self.num_docks_available == other.num_docks_available
        )

    def __init__(self, db_entry=None, json_entry=None):
        if db_entry:
            self.id = db_entry[0]
            self.last_reported = db_entry[1]
            self.is_installed = db_entry[2]
            self.is_renting = db_entry[3]
            self.is_returning = db_entry[4]
            self.num_bikes_available = db_entry[5]
            self.num_docks_available = db_entry[6]
            self.station_id = db_entry[7]
        if json_entry:
            self.station_id = json_entry.get('station_id')
            self.last_reported = json_entry.get('last_reported')
            self.is_installed = json_entry.get('is_installed')
            self.is_renting = json_entry.get('is_renting')
            self.is_returning = json_entry.get('is_returning')
            self.num_bikes_available = json_entry.get('num_bikes_available')
            self.num_docks_available = json_entry.get('num_docks_available')

class FreeBike:
    def __eq__(self, other):
        return (self.bike_id == other.bike_id and 
        self.provider_id == other.provider_id
        )

    def __str__(self):
        return self.bike_id

    def __init__(self, db_entry=None, json_entry=None):
        if db_entry:
            self.id = db_entry[0]
            self.bike_id = db_entry[1]
            self.provider_id = db_entry[2]
        if json_entry:
            self.bike_id = json_entry.get('bike_id')
            self.provider_id = json_entry.get('provider_id')

class FreeBikeStatus:
    def __eq__(self, other):
        return (self.bike_id == other.bike_id and 
        abs(self.lat - other.lat) < 0.0001 and
        abs(self.lon - other.lon) < 0.0001 and
        self.is_disabled == other.is_disabled and
        self.is_reserved == other.is_reserved
        )

    def __str__(self):
        return self.bike_id

    def __init__(self, db_entry=None, json_entry=None, last_reported=None):
        if db_entry:
            self.id = db_entry[0]
            self.last_reported = db_entry[1]
            geocoordinates = literal_eval(db_entry[2])
            self.lat = geocoordinates[0]
            self.lon = geocoordinates[1]
            self.is_disabled = db_entry[3]
            self.is_reserved = db_entry[4]
            self.bike_id = db_entry[5]
        if json_entry:
            self.last_reported = last_reported
            self.bike_id = json_entry.get('bike_id')
            self.lat = json_entry.get('lat')
            self.lon = json_entry.get('lon')
            self.is_disabled = json_entry.get('is_disabled')
            self.is_reserved = json_entry.get('is_reserved')
