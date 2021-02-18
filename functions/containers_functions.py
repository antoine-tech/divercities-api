from bson.json_util import dumps
import json as JSON
# from collections import Counter
# import random
# import pandas as pd
# import numpy as np


def get_traffic_routier_data(db, annee_mesure="2018"):
    collection = db[
        'Trafic moyen journalier annuel sur le réseau routier national (2008 à 2018)']
    pipeline = [

        {
            "$match": {
                "anneemesur": annee_mesure
            }
        },
        {
            "$project": {
                "_id": 0,
                "geo_shape": 1,
                "cumuld": 1,
                "cumulf": 1
            }
        }
    ]

    traffic_routier_data = collection.aggregate(pipeline)
    final_traffic_routier_data = []
    for element in traffic_routier_data:
        tmp_dict = {}
        for k, v in element["geo_shape"].items():
            tmp_dict[k] = v
        tmp_dict["properties"] = {
            "cumuld": element["cumuld"], "cumulf": element["cumulf"]}
        final_traffic_routier_data.append(tmp_dict)
    return dumps(final_traffic_routier_data)


def get_shocks_containers(db):
    collection = db[
        'Traxens iot sensors Shocks']
    pipeline = [
        {
            "$project": {
                "_id": 0,
                "coordinates": ["$lon", "$lat"],
                "sensor_values": 1
            }
        }
    ]
    shocks_containers = collection.aggregate(pipeline)
    return dumps(shocks_containers)


def get_sensors_types(db):
    collection = db['Traxens iot sensors']
    sensor_types = collection.distinct('sensor_name')
    return dumps(sensor_types)


def get_sensors_positions_by_sensor_type(db, sensor_type):
    collection = db['Traxens iot sensors']
    sensor_types = collection.distinct('sensor_name')
    pipeline = [
        {
            "$match": {
                "sensor_name": sensor_type
            }
        },
        {
            "$project": {
                "_id": 0,
                "equipment_number": 1,
                "coordinates": ["$gps.lon", "$gps.lat"],
                "sensor_values": 1,
                "sensor_value": 1,
                "sensor_name": 1
            }
        }
    ]
    sensors_containers = collection.aggregate(pipeline)
    return dumps(sensors_containers)


def get_sensors_details(db, equipment_number):
    collection = db['Traxens iot sensors groupée par equipment_number']
    results = list(collection.find(
        {'_id': {'$regex': equipment_number}}
    ).limit(25))

    final_results = []
    for e in results:
        temp_sensor_details = e["sensor_details"]
        for el in temp_sensor_details:
            temp_dict = JSON.loads(el['sensor_values'])
            el['sensor_values'] = temp_dict
        e["sensor_details"] = temp_sensor_details
        final_results.append(e)

    return dumps(final_results)


def get_trajets_container(db):
    collection = db['Traxens iot sensors Trajets']
    return dumps(collection.find({}))
