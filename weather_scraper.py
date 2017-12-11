"""
Example script that scrapes data from the IEM ASOS download service
"""
from __future__ import print_function
import json
import time
import datetime
import numpy as np
import pandas as pd
from urllib.request import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 6
SERVICE = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"


def download_data(uri):
    """Fetch the data from the IEM

    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.

    Args:
      uri (string): URL to fetch

    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(url = uri, timeout = 300).read()
            if data is not None:
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""

#From CSV with airport names, generate a dict of stations to pull data for
def generate_stations(filename):
    stations_by_state = {}
    air_df = pd.read_csv(filename)
    states = air_df.groupby('STATE')
    for state in states.groups.keys():
        station_list = states.get_group(state)['IATA_CODE'].values
        station_list = [x for x in station_list]
        if state == 'IA':
            stations_by_state['AWOS'] = station_list
        else:
            stations_by_state[state + 's_ASOS'] = station_list
    return stations_by_state 

def main():
    # timestamps in UTC to request data for
    startts = datetime.datetime(2015, 1, 1)
    endts = datetime.datetime(2016, 1, 1)

    service = SERVICE + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"

    service += startts.strftime('year1=%Y&month1=%m&day1=%d&')
    service += endts.strftime('year2=%Y&month2=%m&day2=%d&')
    
    stations = generate_stations('airports.csv')
    for network in stations.keys():
        # Get metadata
        uri = ("https://mesonet.agron.iastate.edu/"
               "geojson/network/%s.geojson") % (network,)
        
        for faaid in stations[network]:
            uri = '%s&station=%s' % (service, faaid)
            print(('Network: %s Downloading: %s'
                   ) % (network, faaid))
            data = download_data(uri)
            outfn = 'weather_data/%s_%s_%s.txt' % (faaid, startts.strftime("%Y%m%d%H%M"),
                                      endts.strftime("%Y%m%d%H%M"))
            out = open(outfn, 'wb')
            out.write(data)
            out.close()


if __name__ == '__main__':
    main()
