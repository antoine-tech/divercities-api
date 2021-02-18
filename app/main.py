from flask import Flask, request, jsonify
from flask_pymongo import pymongo
from pymongo import MongoClient
from flask_cors import CORS, cross_origin

from functions import ports_functions
from functions import ships_functions
from functions import commons_functions
from functions import containers_functions


MONGO_URI = "mongodb+srv://vladtheimpalor:loki666@scm.xl8o8.mongodb.net/Voyage_Conteneur?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)

db = client.Voyage_Conteneur
app = Flask(__name__)
CORS(app)


# /continent

@app.route('/api/continents', methods=['GET'])
def continents():
    return commons_functions.get_continents(db)

# /ports


@app.route('/api/ports', methods=['GET'])
def ports():
    region = request.args.get(
        'region') if request.args.get('region') else "ALL"
    return ports_functions.get_ports(db, region)


@app.route('/api/ports/trajets', methods=['GET'])
def ports_trajets():
    start_region = request.args.get(
        'start_region') if request.args.get('start_region') else "ALL"
    end_region = request.args.get(
        'end_region') if request.args.get('end_region') else "ALL"
    arrival = request.args.get(
        'arrival') if request.args.get('arrival') else "ALL"
    return ships_functions.get_trajets(db, start_region, end_region, arrival)


@app.route('/api/ports/count', methods=['GET'])
def port_count():
    region = request.args.get('region') if request.args.get(
        'region') else "Europe"
    return ports_functions.get_port_count(db, region)


@app.route('/api/ports/relations', methods=['GET'])
def ports_relations():
    return ports_functions.get_ports_relations(db)


@app.route('/api/ports/goods/exportations', methods=['GET'])
def ports_exportations():
    country = request.args.get('country')
    data_type = request.args.get(
        'type') if request.args.get('type') else "values"
    if country and data_type == "values":
        return ports_functions.get_exportations_values(db, country)
    elif country and data_type == "masses":
        return ports_functions.get_exportations_masses(db, country)
    else:
        if data_type == "values":
            return ports_functions.get_exportations_values(db)
        else:
            return ports_functions.get_exportations_masses(db)


@app.route('/api/ports/goods/importations', methods=['GET'])
def ports_importations():
    country = request.args.get('country')
    data_type = request.args.get(
        'type') if request.args.get('type') else "values"
    if country and data_type == "values":
        return ports_functions.get_importations_values(db, country)
    elif country and data_type == "masses":
        return ports_functions.get_importations_masses(db, country)
    else:
        if data_type == "values":
            return ports_functions.get_importations_values(db)
        else:
            return ports_functions.get_importations_masses(db)

@app.route('/api/ports/analytics', methods=['GET'])
def ports_relations_analytics():
    return ports_functions.get_ports_relations_analytics(db)

# GOODS


@app.route('/api/ports/goods/marseille/out', methods=['GET'])
def ports_goods_marseille_out():
    return ports_functions.get_out_goods_GPMM_grouped_by_categorie_and_subcategorie(db)


@app.route('/api/ports/goods/marseille/in', methods=['GET'])
def ports_goods_marseille_in():
    return ports_functions.get_in_goods_GPMM_grouped_by_categorie_and_subcategorie(db)


@app.route('/api/ports/goods/marseille/monthly-evolution', methods=['GET'])
def ports_goods_marseille_terminals():
    data_type = request.args.get('type') if request.args.get('type') else "in"
    return ports_functions.get_in_out_goods_GPMM_monthly_evolution(db, data_type)


@app.route('/api/ports/trajets/analytics', methods=['GET'])
def ports_trajets_analysis():
    return ports_functions.get_trajets_analytics(db)

# PASSENGERS


@app.route('/api/ports/passengers/marseille/out', methods=['GET'])
def ports_passengers_marseille_out():
    return ports_functions.get_out_passengers_GPMM_grouped_by_categorie_and_subcategorie(db)


@app.route('/api/ports/passengers/marseille/in', methods=['GET'])
def ports_passengers_marseille_in():
    return ports_functions.get_in_passengers_GPMM_grouped_by_categorie_and_subcategorie(db)


@app.route('/api/ports/passengers/marseille/monthly-evolution', methods=['GET'])
def ports_passengers_marseille_terminals():
    data_type = request.args.get('type') if request.args.get('type') else "in"
    return ports_functions.get_in_out_passengers_GPMM_monthly_evolution(db, data_type)

# /ships


@app.route('/api/ships/count', methods=['GET'])
def ship_count():
    region = request.args.get('region') if request.args.get(
        'region') else "Europe"
    return ships_functions.get_ship_count(db, region)


@app.route('/api/ports/hinterlands', methods=['GET'])
def ports_hinterlands():
    return ports_functions.get_hinterlands(db)


@app.route('/api/ports/marseille/toile-industrialo-portuaire', methods=['GET'])
def ports_toile_industrialo_portuaire():
    return ports_functions.get_toile_industrialo_portuaire(db)


# /containers
@app.route('/api/containers/traffic-routier', methods=['GET'])
def container_traffic_routier():
    annee_mesure = request.args.get(
        "year") if request.args.get("year") else "2018"
    return containers_functions.get_traffic_routier_data(db, annee_mesure)


@app.route('/api/containers/sensors/types', methods=['GET'])
def container_sensor_types():
    return containers_functions.get_sensors_types(db)


@app.route('/api/containers/sensors/shocks', methods=['GET'])
def container_sensors_shocks():
    return containers_functions.get_shocks_containers(db)


@app.route('/api/containers/sensors', methods=['GET'])
def container_sensors():
    sensor_type = request.args.get(
        'type') if request.args.get('type') else "GSM"
    return containers_functions.get_sensors_positions_by_sensor_type(db, sensor_type)


@app.route('/api/containers/sensors/details')
def container_sensors_details():
    equipment_number = request.args.get(('equipment_number'))
    return containers_functions.get_sensors_details(db, equipment_number)

@app.route('/api/containers/trajets', methods=['GET'])
def container_trajets():
    return containers_functions.get_trajets_container(db)


@app.route('/api/ports/ai', methods=['GET'])
def ports_ai():
    temperature = request.args.get('temperature') # °C
    wind_speed = request.args.get('wind_speed') # m/s
    visibility = request.args.get('visibility') # m
    pressure = request.args.get('pressure') # bar
    snow_height =  request.args.get('snow_height') # m
    return ports_functions.get_forecast(db,temperature, wind_speed, visibility, pressure, snow_height)
