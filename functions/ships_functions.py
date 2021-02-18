from bson.json_util import dumps
import json as JSON


def get_trajets(db, start_region, end_region, arrival):
    start_region = {"$exists": True} if start_region == "ALL" else start_region
    end_region = {"$exists": True} if end_region == "ALL" else end_region
    trajets = []
    collection = db['trajet_navire_coordonees_geo_with_countries_final_with_arrivals_and_departures']
    for trajet in collection.find({"following_port.region": end_region, "previous_port.region": start_region}):
            trajets.append(trajet["following_port"])
            trajets.append(trajet["previous_port"])
    return dumps(trajets)


def get_ship_count(db, region):
    collection = db["trajets_navires_coordonnees_geo_with_countries"]
    pipeline = [
        {
            "$match": {"following_port.region": region}
        },
        {"$group": {"_id": "_id", "count": {"$sum": 1}}},
        {
            "$project":  {
                "_id": 0,
                "count": 1
            }
        }
    ]
    navire_count= collection.aggregate(
        pipeline
    )
    return dumps(navire_count)
