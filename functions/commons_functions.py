from bson.json_util import dumps
import json as JSON


def get_continents(db):
    collection = db.trajet_navire_coordonees_geo_with_countries
    continents = collection.distinct('following_port.region')
    return dumps(continents)
