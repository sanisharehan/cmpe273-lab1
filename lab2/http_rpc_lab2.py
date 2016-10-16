#!/usr/bin/python

"""
Simple HTTP-RPC application to check crime report for a location. The application
is built using spyne toolkit for RPC with HttpRpc as input protocol and JSON
as output.

Author: Sanisha Rehan

Example Call: http://localhost:8000/checkcrime?lat=37.334164&lon=-121.884301&radius=0.002
http://localhost:8000/say_hello?name=Sanisha&times=8
"""

import logging
logging.basicConfig(level=logging.DEBUG)

from spyne import Application
from spyne.decorator import srpc
from spyne.model.complex import Iterable
from spyne.model.primitive import Integer, String, Float, AnyDict
from spyne.service import ServiceBase

from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication

from urllib2 import urlopen
import json
import datetime

# Define URL String
URL_TO_OPEN = 'https://api.spotcrime.com/crimes.json?lat=%f&lon=%f&radius=%f&key=.'
ADDRESS_KEY = 'address'
TIME_KEY = 'date'
TYPE_KEY = 'type'

# Define keys for final JSON/result
TOTAL_CRIME = "total_crime"
THE_MOST_DANGEROUS_STREETS = "the_most_dangerous_streets"
CRIME_TYPE_COUNT = "crime_type_count"
EVENT_TIME_COUNT = "event_time_count"


def generate_crime_report(crimes_list):
    '''
    Function to generate crime report in desired format.
    '''
    address_map_count = {}
    type_map_count = {}
    time_map_count = {
        "12:01am-3am" : 0,
        "3:01am-6am" : 0,
        "6:01am-9am" : 0,
        "9:01am-12noon" : 0,
        "12:01pm-3pm" : 0,
        "3:01pm-6pm" : 0,
        "6:01pm-9pm" : 0,
        "9:01pm-12midnight" : 0
    }

    final_result_json = {}
    number_crimes = len(crimes_list)

    for item in crimes_list:
        for key, val in item.iteritems():
            # Get address counts
            if key == ADDRESS_KEY:
                str_addr = []
                # Split by OF
                split_addr = val.split('OF')
                if len(split_addr) > 1:
                    str_addr.append(split_addr[1])
                else:
                    # Split by BLOCK
                    split_addr = val.split('BLOCK')
                    if len(split_addr) > 1:
                        str_addr.append(split_addr[len(split_addr) - 1])
                    else:
                        # Split by &
                        split_addr = val.split('&')
                        if len(split_addr) > 1:
                            for street in split_addr:
                                str_addr.append(street)
                        else:
                            # Split by AND
                            split_addr = val.split('AND')
                            if len(split_addr) > 1:
                                for street in split_addr:
                                    str_addr.append(street)          
                if len(split_addr) == 1:
                    str_addr.append(val)
                
                # Add final result into map
                for street in str_addr:
		    # Final split to remove any character following the street 
                    # name e.g. ESCALON AV #2027
                    street = street.split('#')[0]
                    street = street.lstrip().rstrip().encode('ascii','ignore')
                    if address_map_count.has_key(street):
                        address_map_count[street] += 1
                    else:
                        address_map_count[street] = 1
        
            # Get crime type counts
            if key == TYPE_KEY:
                val = val.encode('ascii','ignore')
                if type_map_count.has_key(val):
                    type_map_count[val] += 1
                else:
                    type_map_count[val] = 1
        
            # Get time counts
            if key == TIME_KEY:
                time = datetime.datetime.strptime(val, "%m/%j/%y %I:%M %p").time()
                # Time updates based on crime time
                if (time.hour >= 0 and time.hour <= 3):
                    if (time.hour == 0 and time.minute == 0) or (time.hour == 3 and time.minute > 0):
                        pass
                    else:
                        time_map_count["12:01am-3am"] += 1
            
                if (time.hour >= 3 and time.hour <= 6):
                    if (time.hour == 3 and time.minute == 0) or (time.hour == 6 and time.minute > 0):
                        pass
                    else:
                        time_map_count["3:01am-6am"] += 1
                    
                if (time.hour >= 6 and time.hour <= 9):
                    if (time.hour == 6 and time.minute == 0) or (time.hour == 9 and time.minute > 0):
                        pass
                    else:
                        time_map_count["6:01am-9am"] += 1
                    
                if (time.hour >= 9 and time.hour <= 12):
                    if (time.hour == 9 and time.minute == 0) or (time.hour == 12 and time.minute > 0):
                        pass
                    else:
                        time_map_count["9:01am-12noon"] += 1

                if (time.hour >= 12 and time.hour <= 15):
                    if (time.hour == 12 and time.minute == 0) or (time.hour == 15 and time.minute > 0):
                        pass
                    else:
                        time_map_count["12:01pm-3pm"] += 1
            
                if (time.hour >= 15 and time.hour <= 18):
                    if (time.hour == 15 and time.minute == 0) or (time.hour == 18 and time.minute > 0):
                        pass
                    else:
                        time_map_count["3:01pm-6pm"] += 1
                    
                if (time.hour >= 18 and time.hour <= 21):
                    if (time.hour == 18 and time.minute == 0) or (time.hour == 21 and time.minute > 0):
                        pass
                    else:
                        time_map_count["6:01pm-9pm"] += 1
                    
                if (time.hour >= 21 and time.hour < 24) or (time.hour == 0 and time.minute == 0):
                    if (time.hour == 21 and time.minute == 0) or (time.hour == 0 and time.minute > 0):
                        pass
                    else:
                        time_map_count["9:01pm-12midnight"] += 1

    # Get the final results
    dangerous_streets = sorted(address_map_count, key=address_map_count.get, reverse=True)
    final_result_json[TOTAL_CRIME] = number_crimes
    final_result_json[THE_MOST_DANGEROUS_STREETS] = dangerous_streets[:3]
    final_result_json[CRIME_TYPE_COUNT] = type_map_count
    final_result_json[EVENT_TIME_COUNT] = time_map_count
    return final_result_json


# Simple Hello World class for easy debug/application check
class HelloWorldService(ServiceBase):
    @srpc(String, Integer, _returns=Iterable(String))
    def say_hello(name, times):
        for i in range(times):
            yield 'Hello, %s' %name


# Class for reporting crimes for a location
class CheckCrimeService(ServiceBase):
    @srpc(Float, Float, Float, _returns=AnyDict)
    def checkcrime(lat, lon, radius):
        '''
        This method takes all float parameters from the http-rpc call
        '''
        url = (URL_TO_OPEN % (lat, lon, radius))
        response = urlopen(url)
        response_json = json.load(response)
        # Get crimes list
        crimes_list = []
        for key, val in response_json.iteritems():
            crimes_list = val
        return generate_crime_report(crimes_list)


application = Application([HelloWorldService, CheckCrimeService], 
        tns='spyne.examples.hello', 
        in_protocol=HttpRpc(validator='soft'), 
        out_protocol=JsonDocument()
)


# Main function for launching application
if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()
