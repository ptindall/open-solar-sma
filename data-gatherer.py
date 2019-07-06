#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pymodbus.client.sync import ModbusTcpClient
from influxdb import InfluxDBClient
import json
import time
import requests
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import datetime

from common.const import CONFIG_FILE_ARRAYS, CONFIG_DIR

MIN_SIGNED = -2147483648
MAX_UNSIGNED = 4294967295

requests.packages.urllib3.disable_warnings()

with open(CONFIG_FILE_ARRAYS, 'r') as fp:
    configuration = json.load(fp)

# SMA datatypes and their register lengths
# S = Signed Number, U = Unsigned Number, STR = String
sma_mod_data_type = {
    'S16': 1,
    'U16': 1,
    'S32': 2,
    'U32': 2,
    'U64': 4,
    'STR16': 8,
    'STR32': 16
}

print("Connecting to inverters")
arrays = []
for array in configuration["arrays"]:
    client = ModbusTcpClient(array['inverter']['ip_address'],
                             timeout=30,
                             RetryOnEmpty=True,
                             retries=3,
                             port=array['inverter']['ip_port'])
    client.connect()

    mod_map_file = f"modbus-{array['inverter']['config_model']}.json"

    with open(f"{CONFIG_DIR}/{mod_map_file}") as fp:
        mod_bus_config = json.load(fp)

    arrays.append({
        "array_name": array['name'],
        "inverter_client": client,
        "inverter_slave": array['inverter']['slave'],
        "mod_bus_config": mod_bus_config
    })

print("Connecting to influx")
flux_client = InfluxDBClient(configuration['influx']['host'],
                             configuration['influx']['port'],
                             configuration['influx']['user'],
                             configuration['influx']['password'],
                             configuration['influx']['database'],
                             ssl=configuration['influx']['ssl'],
                             verify_ssl=configuration['influx']['verify_ssl'])

weather_url = f"http://api.openweathermap.org/data/2.5/weather?zip={configuration['location']['zip']},{configuration['location']['country']}&units=imperial&APPID={configuration['weather_api_key']}"

def load_sma_register(inverter_client, inverter_slave, mod_bus_config):
    register_data = {}

    # request each register from datasets, omit first row which contains only column headers
    for register_def in mod_bus_config['registers']:
        name = register_def['description']
        register_id = register_def['number']
        register_type = register_def['type']
        register_format = register_def['format']

        # if the connection is somehow not possible (e.g. target not responding)
        #  show a error message instead of excepting and stopping
        try:
            received = inverter_client.read_input_registers(address=register_id,
                                                            count=sma_mod_data_type[register_type],
                                                            unit=inverter_slave)
        except Exception as err:
            this_date = str(datetime.datetime.now()).partition('.')[0]
            this_error_message = this_date + ': Connection not possible. Check settings or connection.'
            print(this_error_message)
            return register_data

        try:
            message = BinaryPayloadDecoder.fromRegisters(received.registers, byteorder=Endian.Big)
            # provide the correct result depending on the defined datatype
            if register_type in ['S32','U32']:
                interpreted = message.decode_32bit_int()
            elif register_type == 'U64':
                interpreted = message.decode_64bit_uint()
            elif register_type == 'STR16':
                interpreted = message.decode_string(16)
            elif register_type == 'STR32':
                interpreted = message.decode_string(32)
            else:
                interpreted = message.decode_16bit_uint()

            # check for "None" data before doing anything else
            if (interpreted == MIN_SIGNED) or (interpreted == MAX_UNSIGNED):
                display_data = None
            else:
                # put the data with correct formatting into the data table
                if register_format == 'FIX3':
                    display_data = float(interpreted) / 1000
                elif register_format == 'FIX2':
                    display_data = float(interpreted) / 100
                elif register_format == 'FIX1':
                    display_data = float(interpreted) / 10
                elif register_format == 'UTF8':
                    display_data = str(interpreted)
                else:
                    display_data = interpreted

            register_data[name] = display_data

            # Add timestamp
            register_data["Timestamp"] = str(datetime.datetime.now()).partition('.')[0]

        except Exception as err:
            this_date = str(datetime.datetime.now()).partition('.')[0]
            this_error_message = this_date + ': Unable to process response data.'
            print(this_error_message)

    return register_data


def publish_influx(metrics):
    try:
        flux_client.write_points([metrics])
        print("[INFO] Sent to InfluxDB")
    except Exception as err:
        print(f"[ERROR] Could not send to InfluxDB.  {err}")


while True:
    try:
        # get weather data first
        response = requests.get(weather_url, headers={"Accept": "application/json"})
        if response.status_code != 200:
            print(response.content)
            break
        else:
            body = json.loads(response.content)
            temperature = body['main']['temp']
            cloudiness = body['clouds']['all']

        for array in arrays:
            registers = load_sma_register(array['inverter_client'], array['inverter_slave'], array['mod_bus_config'])

            registers["Cloudiness (%)"] = cloudiness
            registers["Temperature (F)"] = temperature
            registers["Location type"] = configuration['location']['type']
            print(registers)

            metrics = {}
            tags = {}
            fields = {}
            metrics['measurement'] = array['array_name']
            tags['location'] = configuration['location']['address_one']
            metrics['tags'] = tags
            metrics['fields'] = registers
            publish_influx(metrics)

    except Exception as err:
        print("[ERROR] %s" % err)

    time.sleep(15)
