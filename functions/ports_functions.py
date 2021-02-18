from bson.json_util import dumps
import json as JSON
from collections import Counter
import random
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split


def get_ports(db, region):
    region = {"$exists": True} if region == "ALL" else region
    collection = db["Global Ports"]
    pipeline = [
        {
            "$lookup":
            {
                "localField": "country",
                "from": "ISO-3166-Countries-with-Regional-Codes",
                "foreignField": "name",
                "as": "country"
            }
        },
        {
            "$match": {"country.region": region, "lat": {"$exists": True}, "lon": {"$exists": True}, "portname":  {"$exists": True}}
        }
    ]
    ports = list(collection.aggregate(
        pipeline
    ))
    ports = list({port['portname']: port for port in ports}.values())
    return dumps(ports)


def get_port_count(db, region):
    collection = db["Global Ports"]
    pipeline = [
        {
            "$lookup":
            {
                "localField": "country",
                "from": "ISO-3166-Countries-with-Regional-Codes",
                "foreignField": "name",
                "as": "country"
            }
        },
        {
            "$match": {"country.region": region, "lat": {"$exists": True}, "lon": {"$exists": True}, "portname":  {"$exists": True}}
        },
        {"$group": {"_id": "_id", "count": {"$sum": 1}}},
        {
            "$project":  {
                "_id": 0,
                "count": 1
            }
        }
    ]
    port_count = collection.aggregate(
        pipeline
    )
    return dumps(port_count)


def get_ports_relations_analytics(db):
    collection = db["trajet_navire_coordonees_geo_with_countries_final_with_arrivals_and_departures"]
    pipeline_following_port = [
        {
            "$group": {
                "_id": "$following_port.region",
                "ports": {
                    "$addToSet": "$following_port.label"
                }
            }
        }
    ]
    pipeline_previous_port = [
        {
            "$group": {
                "_id": "$previous_port.region",
                "ports": {
                    "$addToSet": "$previous_port.label"
                }
            }
        }
    ]
    following_port_by_region = list(collection.aggregate(pipeline_following_port))
    previous_port_by_region =  list(collection.aggregate(pipeline_previous_port)) 

    following_port_by_region_dict = {}
    for element in following_port_by_region:
        following_port_by_region_dict[element["_id"]] = element["ports"]

    previous_port_by_region_dict = {}
    for element in previous_port_by_region:
        previous_port_by_region_dict[element["_id"]] = element["ports"]

    final_dict = {}
    for k,v in following_port_by_region_dict.items():
        final_dict[k] = list(set(v + following_port_by_region_dict[k]))


    collection = db['Global Ports']
    
    ports_by_region_related_to_marseille = {}

    for k,v in final_dict.items():
        results = list(collection.find({"portname" : {"$in" : v}}))
        ports_by_region_related_to_marseille[k] =  results

    return dumps(ports_by_region_related_to_marseille)




def get_ports_relations(db):
    collection = db["AP+ et CI5"]
    pipeline = [
        {
            "$match": {
                "port_suivant": {'$ne': None},
                "port_precedent": {'$ne': None}
            }
        }
    ]
    lst = list(collection.aggregate(pipeline))
    nodes = set(["pre_" + l['port_precedent']
                 for l in lst] + ["post_" + l['port_suivant'] for l in lst])
    nodes = [{"id": n} for n in nodes if n]
    nodes.append({"id": "FRFOS"})
    nodes.append({"id": "FRMRS"})
    links = [{"source": "pre_" + l['port_precedent'], "target": "FRFOS"}
             for l in lst] + [{"source": "FRFOS", "target":  "post_" + l['port_suivant']} for l in lst]
    c = Counter(dumps(l) for l in links)
    final_links = []
    for i, v in c.items():
        tmp_key = JSON.loads(i)
        if tmp_key["source"] and tmp_key["target"]:
            final_links.append(
                {
                    "source": tmp_key["source"],
                    "target": tmp_key["target"],
                    "value": v
                }
            )
    json = {
        'nodes': nodes,
        'links': final_links,
    }
    return dumps(json)


def get_importations_values(db, country=None):
    if country:
        collection = db['Statistiques du commerce extérieur des douanes - importations 2019 - groupées par pays - en valeur']
        pipeline = [
            {"$match": {"name": country}},
            {
                "$project":
                {
                    "_id": 0,
                    "children": 1
                }
            }
        ]
        final_list = []
        tmp = list(collection.aggregate(pipeline))
        tmp = tmp[0]["children"]
        for e in tmp:
            final_list.append(
                {
                    "id": e["name"],
                    "label": e["name"],
                    "value": e["loc"],
                    "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
                }
            )
        return dumps(final_list)
    else:
        collection = db['Statistiques du commerce extérieur des douanes - importations 2019']
        pipeline = [
            {"$group": {"_id": "$pays", "value": {"$sum": "$valeur"}}},
            {"$project": {
                "_id": 0,
                "id": "$_id",
                "label": "$_id",
                "value": 1,
                "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
            }}
        ]
        return dumps(collection.aggregate(pipeline))


def get_exportations_values(db, country=None):
    if country:
        collection = db['Statistiques du commerce extérieur des douanes - exportations 2019 - groupées par pays - en valeur']
        pipeline = [
            {"$match": {"name": country}},
            {
                "$project":
                {
                    "_id": 0,
                    "children": 1
                }
            }
        ]
        final_list = []
        tmp = list(collection.aggregate(pipeline))
        tmp = tmp[0]["children"]
        for e in tmp:
            final_list.append(
                {
                    "id": e["name"],
                    "label": e["name"],
                    "value": e["loc"],
                    "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
                }
            )
        return dumps(final_list)
    else:
        collection = db['Statistiques du commerce extérieur des douanes - exportations 2019']
        pipeline = [
            {"$group": {"_id": "$pays", "value": {"$sum": "$valeur"}}},
            {"$project": {
                "_id": 0,
                "id": "$_id",
                "label": "$_id",
                "value": 1,
                "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
            }}
        ]
        return dumps(collection.aggregate(pipeline))


def get_importations_masses(db, country=None):
    if country:
        collection = db['Statistiques du commerce extérieur des douanes - importations 2019 - groupées par pays - en masse']
        pipeline = [
            {"$match": {"name": country}},
            {
                "$project":
                {
                    "_id": 0,
                    "children": 1
                }
            }
        ]
        final_list = []
        tmp = list(collection.aggregate(pipeline))
        tmp = tmp[0]["children"]
        for e in tmp:
            final_list.append(
                {
                    "id": e["name"],
                    "label": e["name"],
                    "value": e["loc"],
                    "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
                }
            )
        return dumps(final_list)

    else:
        collection = db['Statistiques du commerce extérieur des douanes - importations 2019']
        pipeline = [
            {"$group": {"_id": "$pays", "value": {"$sum": "$masse"}}},
            {"$project": {
                "_id": 0,
                "id": "$_id",
                "label": "$_id",
                "value": 1,
                "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
            }}
        ]
        return dumps(collection.aggregate(pipeline))


def get_exportations_masses(db, country=None):
    if country:
        collection = db['Statistiques du commerce extérieur des douanes - exportations 2019 - groupées par pays - en masse']
        pipeline = [
            {"$match": {"name": country}},
            {
                "$project":
                {
                    "_id": 0,
                    "children": 1
                }
            }
        ]
        final_list = []
        tmp = list(collection.aggregate(pipeline))
        tmp = tmp[0]["children"]
        for e in tmp:
            final_list.append(
                {
                    "id": e["name"],
                    "label": e["name"],
                    "value": e["loc"],
                    "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
                }
            )
        return dumps(final_list)
    else:
        collection = db['Statistiques du commerce extérieur des douanes - exportations 2019']
        pipeline = [
            {"$group": {"_id": "$pays", "value": {"$sum": "$masse"}}},
            {"$project": {
                "_id": 0,
                "id": "$_id",
                "label": "$_id",
                "value": 1,
                "color": "hsl(" + str(random.randint(0, 360))+", 70%, 50%)"
            }}
        ]
        return dumps(collection.aggregate(pipeline))


def set_collection_importations_values(db):
    collection = db['Statistiques du commerce extérieur des douanes - importations 2019']
    importations_repartition_by_country_and_type = []
    countries = list(collection.distinct("pays"))
    for country in countries:
        pipeline = [
            {"$match": {"pays": country}},
            {
                "$group": {
                    "_id": "$libelle_cpf4",
                    "loc": {
                        "$sum": "$valeur"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0, "loc": 1, "name": "$_id"
                }
            }
        ]
        importations_repartition_by_country_and_type.append(
            {
                "name": country,
                "children": list(collection.aggregate(pipeline))
            }
        )

    db["Statistiques du commerce extérieur des douanes - importations 2019 - groupées par pays - en valeur"].insert_many(
        importations_repartition_by_country_and_type)
    return dumps(importations_repartition_by_country_and_type)


def set_collection_importations_masses(db):
    collection = db['Statistiques du commerce extérieur des douanes - importations 2019']
    importations_repartition_by_country_and_type = []
    countries = list(collection.distinct("pays"))
    for country in countries:
        pipeline = [
            {"$match": {"pays": country}},
            {
                "$group": {
                    "_id": "$libelle_cpf4",
                    "loc": {
                        "$sum": "$masse"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0, "loc": 1, "name": "$_id"
                }
            }
        ]
        importations_repartition_by_country_and_type.append(
            {
                "name": country,
                "children": list(collection.aggregate(pipeline))
            }
        )

    db["Statistiques du commerce extérieur des douanes - importations 2019 - groupées par pays - en masse"].insert_many(
        importations_repartition_by_country_and_type)
    return dumps(importations_repartition_by_country_and_type)


def set_collection_exportations_values(db):
    collection = db['Statistiques du commerce extérieur des douanes - exportations 2019']
    exportations_repartition_by_country_and_type = []
    countries = list(collection.distinct("pays"))
    for country in countries:
        pipeline = [
            {"$match": {"pays": country}},
            {
                "$group": {
                    "_id": "$libelle_cpf4",
                    "loc": {
                        "$sum": "$valeur"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0, "loc": 1, "name": "$_id"
                }
            }
        ]
        exportations_repartition_by_country_and_type.append(
            {
                "name": country,
                "children": list(collection.aggregate(pipeline))
            }
        )

    db["Statistiques du commerce extérieur des douanes - exportations 2019 - groupées par pays - en valeur"].insert_many(
        exportations_repartition_by_country_and_type)
    return dumps(exportations_repartition_by_country_and_type)


def set_collection_exportations_masses(db):
    collection = db['Statistiques du commerce extérieur des douanes - exportations 2019']
    exportations_repartition_by_country_and_type = []
    countries = list(collection.distinct("pays"))
    for country in countries:
        pipeline = [
            {"$match": {"pays": country}},
            {
                "$group": {
                    "_id": "$libelle_cpf4",
                    "loc": {
                        "$sum": "$masse"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0, "loc": 1, "name": "$_id"
                }
            }
        ]
        exportations_repartition_by_country_and_type.append(
            {
                "name": country,
                "children": list(collection.aggregate(pipeline))
            }
        )

    db["Statistiques du commerce extérieur des douanes - exportations 2019 - groupées par pays - en masse"].insert_many(
        exportations_repartition_by_country_and_type)
    return dumps(exportations_repartition_by_country_and_type)


def get_in_goods_GPMM_grouped_by_categorie_and_subcategorie(db):
    collection = db['Mouvements des entrées et sorties GPMM (Année 2019)']
    categories = collection.distinct('categories')
    passengers_categories = [
        "NUMBER OF LOCAL AND FERRY PASSENGERS", "CRUISE PASSENGERS"]
    categories = filter(lambda x: x not in passengers_categories, categories)
    in_out_movement_grouped_by_categorie_and_subcategorie = {
        "name": "GOODS IN GPMM 2019", "children": []}
    for category in categories:
        pipeline = [
            {"$match": {"categories": category}},
            {"$group": {"_id": "$under_categories", "loc": {"$sum": "$in"}}},
            {"$project": {"_id": 0, "name": "$_id", "loc": 1}}
        ]
        in_out_movement_grouped_by_categorie_and_subcategorie["children"].append(
            {
                "name": category,
                "children": list(collection.aggregate(pipeline))
            }
        )
    return dumps(in_out_movement_grouped_by_categorie_and_subcategorie)


def get_in_passengers_GPMM_grouped_by_categorie_and_subcategorie(db):
    collection = db['Mouvements des entrées et sorties GPMM (Année 2019)']
    categories = ["NUMBER OF LOCAL AND FERRY PASSENGERS", "CRUISE PASSENGERS"]
    in_out_movement_grouped_by_categorie_and_subcategorie = {
        "name": "PASSENGERS IN GPMM 2019", "children": []}
    for category in categories:
        pipeline = [
            {"$match": {"categories": category}},
            {"$group": {"_id": "$under_categories", "loc": {"$sum": "$in"}}},
            {"$project": {"_id": 0, "name": "$_id", "loc": 1}}
        ]
        in_out_movement_grouped_by_categorie_and_subcategorie["children"].append(
            {
                "name": category,
                "children": list(collection.aggregate(pipeline))
            }
        )
    return dumps(in_out_movement_grouped_by_categorie_and_subcategorie)


def get_out_goods_GPMM_grouped_by_categorie_and_subcategorie(db):
    collection = db['Mouvements des entrées et sorties GPMM (Année 2019)']
    categories = collection.distinct('categories')
    passengers_categories = [
        "NUMBER OF LOCAL AND FERRY PASSENGERS", "CRUISE PASSENGERS"]
    categories = filter(lambda x: x not in passengers_categories, categories)
    in_out_movement_grouped_by_categorie_and_subcategorie = {
        "name": "GOODS OUT GPMM 2019", "children": []}
    for category in categories:
        pipeline = [
            {"$match": {"categories": category}},
            {"$group": {"_id": "$under_categories", "loc": {"$sum": "$out"}}},
            {"$project": {"_id": 0, "name": "$_id", "loc": 1}}
        ]
        in_out_movement_grouped_by_categorie_and_subcategorie["children"].append(
            {
                "name": category,
                "children": list(collection.aggregate(pipeline))
            }
        )
    return dumps(in_out_movement_grouped_by_categorie_and_subcategorie)


def get_out_passengers_GPMM_grouped_by_categorie_and_subcategorie(db):
    collection = db['Mouvements des entrées et sorties GPMM (Année 2019)']
    categories = ["NUMBER OF LOCAL AND FERRY PASSENGERS", "CRUISE PASSENGERS"]
    in_out_movement_grouped_by_categorie_and_subcategorie = {
        "name": "PASSENGERS OUT GPMM 2019", "children": []}
    for category in categories:
        pipeline = [
            {"$match": {"categories": category}},
            {"$group": {"_id": "$under_categories", "loc": {"$sum": "$out"}}},
            {"$project": {"_id": 0, "name": "$_id", "loc": 1}}
        ]
        in_out_movement_grouped_by_categorie_and_subcategorie["children"].append(
            {
                "name": category,
                "children": list(collection.aggregate(pipeline))
            }
        )
    return dumps(in_out_movement_grouped_by_categorie_and_subcategorie)


def get_in_out_goods_GPMM_monthly_evolution(db, data_type):
    collection = db['Mouvements des entrées et sorties GPMM (Année 2019)']
    categories = list(collection.distinct('categories'))
    passenger_categories = [
        "NUMBER OF LOCAL AND FERRY PASSENGERS", "CRUISE PASSENGERS"]
    categories = filter(lambda e: e not in passenger_categories, categories)
    movement_montly_evolution = []
    for category in categories:
        if data_type == "in":
            pipeline = [
                {"$match": {"categories": category}},
                {"$group": {"_id": "$mois", "sum": {"$sum": "$in"}}},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "x": "$_id",
                              "y":  {"$round": ["$sum", 1]}}}
            ]
            category = "PAS DE CATEGORIE" if category == None else category
            tmp = {
                "id": category,
                "data": list(collection.aggregate(pipeline)),
            }
        else:
            pipeline = [
                {"$match": {"categories": category}},
                {"$group": {"_id": "$mois", "sum": {"$sum": "$out"}}},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "x": "$_id",
                              "y": {"$round": ["$sum", 1]}}}
            ]
            category = "PAS DE CATEGORIE" if category == None else category
            tmp = {
                "id": category,
                "data": list(collection.aggregate(pipeline)),
                "type": "OUT"
            }

        movement_montly_evolution.append(tmp)
    return dumps(movement_montly_evolution)


def get_in_out_passengers_GPMM_monthly_evolution(db, data_type):
    collection = db['Mouvements des entrées et sorties GPMM (Année 2019)']
    categories = ["CRUISE PASSENGERS", "NUMBER OF LOCAL AND FERRY PASSENGERS"]
    movement_montly_evolution = []
    for category in categories:
        if data_type == "in":
            pipeline = [
                {"$match": {"categories": category}},
                {"$group": {"_id": "$mois", "sum": {"$sum": "$in"}}},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "x": "$_id",
                              "y":  {"$round": ["$sum", 1]}}}
            ]
            category = "PAS DE CATEGORIE" if category == None else category
            tmp = {
                "id": category,
                "data": list(collection.aggregate(pipeline)),
            }
        else:
            pipeline = [
                {"$match": {"categories": category}},
                {"$group": {"_id": "$mois", "sum": {"$sum": "$out"}}},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "x": "$_id",
                              "y": {"$round": ["$sum", 1]}}}
            ]
            category = "PAS DE CATEGORIE" if category == None else category
            tmp = {
                "id": category,
                "data": list(collection.aggregate(pipeline)),
                "type": "OUT"
            }

        movement_montly_evolution.append(tmp)
    return dumps(movement_montly_evolution)


def get_marseille_terminals_analytics(db):
    return ''


def get_hinterlands(db):
    pipeline = [
        {
            "$match": {
                "lat": {"$ne": None},
                "lon": {"$ne": None},
                "ville": {"$ne": None},
                "axe_fluvial": {"$ne": None}
            }
        },
        {
            "$project": {
                "lat": 1,
                "lon": 1,
                "ville": 1,
                "axe_fluvial": 1
            }
        }
    ]
    return dumps(db['Hinterland Fluvial'].aggregate(pipeline))


def get_toile_industrialo_portuaire(db):
    pipeline = [
        {
            "$project": {
                "activites": 1,
                "4_filieres": 1,
                "tranche_ca": 1,
                "lon": 1,
                "ville": 1,
                "axe_fluvial": 1,
                "lat": "$geo_point_2d.lat",
                "lon": "$geo_point_2d.lon"
            }
        }
    ]
    return dumps(db['Toile industrialo-portuaire'].aggregate(pipeline))


def get_trajets_analytics(db):
    collection = db['AP+ et CI5']
    pipeline = [

        {"$project": {
            "navire": 1,
            "port_precedent": 1,
            "port_suivant": 1,
            "etd": {"$convert": {"input": "$etd", "to": "date"}},
            "eta": {"$convert": {"input": "$eta", "to": "date"}},
            "rta": {"$convert": {"input": "$rta", "to": "date"}},
            "rtd": {"$convert": {"input": "$etd", "to": "date"}}
        }},
        {"$project": {
            "_id": 0,
            "navire": 1,
            "retard_departure": {"$divide": [{"$subtract": ["$rtd", "$etd"]}, (60*1000)]},
            "retard_arrival": {"$divide": [{"$subtract": ["$rta", "$eta"]}, (60*1000)]}
        }}
    ]


    bin_values = np.arange(start=-100, stop=100, step=1)
    data = list(collection.aggregate(pipeline))
    df = pd.DataFrame.from_dict(data)

    df_description_retard_departure = df['retard_departure'].describe(
    ).to_dict()
    result_df_departure = pd.DataFrame(
        df['retard_departure'].value_counts(bins=bin_values, sort=False))

    df_description_retard_arrival = df['retard_arrival'].describe().to_dict()
    result_df_arrival = pd.DataFrame(
        df['retard_arrival'].value_counts(bins=bin_values, sort=False))

    return dumps(
        {
            "retard_departure": {
                "plot": JSON.loads(result_df_departure["retard_departure"].to_json()),
            },
            "retard_arrival": {
                "plot": JSON.loads(result_df_arrival["retard_arrival"].to_json()),
            }
        }
    )


def get_forecast(db, temperature, wind_speed, visibility, pressure, snow_height):
    collection = db["prediction ai"]
    data = collection.find({})
    df = pd.DataFrame(data)
    y_1 = df['eta_rta']
    y_2 = df['etd_rtd']
    x = df[['vv', 'pmer', 'tc', 'ht_neige', 'ff']]
    x_train, x_test, y_train, y_test = train_test_split(
        x, y_1, test_size=0.2, random_state=0)
    regressor = LinearRegression()
    regressor.fit(x_train, y_train)
    y_pred = regressor.predict(x_test)
    prediction = regressor.predict(
        [[wind_speed, pressure, temperature, snow_height, visibility]])
    return {"forecast": prediction[0]}
