from bottle import Bottle, post, get, HTTPResponse, request, response
import argparse
import os
import sys
import sqlite3
import logging
import json
from db import DB
from db import dict_factory
from db import InspError
import clean_restaurants
from datetime import datetime

DB_NAME = "insp.db"
logging.basicConfig(level=logging.INFO)

app = Bottle()

# Adding default values
app.config.setdefault('myapp.txnsize', 1)
app.config.setdefault('myapp.counter', 0)

@app.get("/hello")
def hello():
    return "Hello, World!"

@app.get("/reset")
@app.get("/create")
def create():
    db = DB(app.db_connection)
    db.create_script()
    return "Created"


@app.get("/seed")
def seed():
    db = DB(app.db_connection)
    db.seed_data()
    return "Seeded"

@app.get("/restaurants/<restaurant_id:int>")
def find_restaurant(restaurant_id):
    """
    Returns a restaurant and all of its associated inspections.
    """
    db = DB(app.db_connection)
    output = {}
    restaurant = db.find_restaurant(restaurant_id)
    output['restaurant'] = restaurant
    
    if not restaurant:
        raise HTTPResponse(status=404)
    inspections = db.find_inspections(restaurant_id)
    output['inspections'] = inspections
    response.content_type = 'application/json'
    return json.dumps(output)

@app.get("/restaurants/by-inspection/<inspection_id>")
def find_restaurant_by_inspection_id(inspection_id):
    """
    Returns a restaurant associated with a given inspection.
    """
    db = DB(app.db_connection)
    rest = db.find_restaurant_withinspection(inspection_id)
    if rest is None:
        raise HTTPResponse(status=404)
    response.content_type = 'application/json'
    return json.dumps(rest[0])

@app.post("/inspections")
def load_inspection():
    """
    Loads a new inspection (and possibly a new restaurant) into the database.
    """
    db = DB(app.db_connection)
    
    # Get current values
    transaction_size = app.config['myapp.txnsize']
    curr_counter = app.config['myapp.counter']
    
    # If first operation, begin the transactions
    if curr_counter == 0:
        db.begin_transaction()
    curr_counter += 1
    app.config['myapp.counter'] = curr_counter
    
    if not request.json:
        raise HTTPResponse(status=400)
    response_code, r_id = db.add_inspection_for_restaurant(request.json)
    
    if response_code:
        response.status = response_code    
        # Checking if max size exceeded
        if curr_counter == transaction_size:
            # Reseting the counter
            app.config['myapp.counter'] = 0
            # Committing active transactions
            db.commit_active()
        return json.dumps({'restaurant_id': r_id})

@app.get("/txn/<txnsize:int>")
def set_transaction_size(txnsize):
    app.config['myapp.txnsize'] = txnsize
    raise HTTPResponse(status=200)

@app.get("/commit")
def commit_txn():
    logging.info("Committing active transactions")
    db = DB(app.db_connection)
    try:
        db.commit_active()
        logging.info("Success!")
        response.status = 200
    except:
        logging.info("Fail!")
        response.status = 501

@app.get("/abort")
def abort_txn():
    logging.info("Aborting/rolling back active transactions")
    db = DB(app.db_connection)
    try:
        db.rollback_active() 
        response.status = 200
        logging.info("Success!")
    except:
        logging.info("Fail!")
        response.status = 501

@app.get("/count")
def count_insp():
    logging.info("Counting Inspections")
    db = DB(app.db_connection)
    cnt = db.count_inspections()[0]['count(*)'] 
    if cnt>=0:
        response.status = 200
        logging.info("Found {} inspections".format(cnt))
        return json.dumps(cnt)
    raise HTTPResponse(status=501)

@app.post("/tweet")
def tweet():
    logging.info("Checking Tweet")
    db = DB(app.db_connection)
    rest_id_list = db.match_and_add_tweet(request.json)
    rest_id_list.sort()
    response.status = 201 # Change to 201 no matter what
    return json.dumps({'matches': rest_id_list})

@app.get("/tweets/<restaurant_id:int>")
def find_restaurant_tweets(restaurant_id):
    """
    Returns a restaurant's associated tweets (tkey and match).
    """
    logging.info("Checking tweets matching restaurant")
    db = DB(app.db_connection)
    tweets = db.find_tweets(restaurant_id)
    if tweets:
        response.status = 200
        return json.dumps([tweets])
    raise HTTPResponse(status=404)

@app.get("/clean")
def clean():
    '''
    Clean all restaurant records by matching any duplicates in ri_linked table.
    '''
    logging.info("Cleaning Restaurants")
    db = DB(app.db_connection)
    # Uses blocking by zip code if app.scaling is True, otherwise all restaurants
    start_time = datetime.now()
    if app.scaling:
        clean_restaurants.clean_by_block(db)
    else:
        clean_restaurants.clean_all_restaurants(db)
    clean_restaurants.mark_as_clean(db)
    end_time = datetime.now()
    logging.info(f'Cleaning time: {end_time - start_time}')
    raise HTTPResponse(status=200)

@app.get("/restaurants/all-by-inspection/<inspection_id>")
def find_all_restaurants_by_inspection_id(inspection_id):
    logging.info("Getting all restaurants for the inspection_id:{}".format(inspection_id))
    db = DB(app.db_connection)
    output = clean_restaurants.create_json_output(db,inspection_id)
    if output:
        response.status = 200
        response.content_type = 'application/json'
        return json.dumps(output)
    else:
        raise HTTPResponse(status=404)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        help="Server hostname (default localhost)",
        default="localhost"
    )
    parser.add_argument(
        "-p","--port",
        help="Server port (default 30235)",
        default=30235,
        type=int
    )
    parser.add_argument(
        "-s","--scaling",
        help="Enable large scale cleaning",
        default=False,
        action="store_true"
    )

    # Create the parser argument object
    args = parser.parse_args()
    # Create the database connection and store it in the app object
    app.db_connection = sqlite3.connect(DB_NAME)
    # See https://stackoverflow.com/questions/3300464/how-can-i-get-dict-from-sqlite-query
    app.db_connection.row_factory = dict_factory
    app.scaling = False
    if args.scaling:
        logging.info("Set to use large scale cleaning")
        app.scaling = True
    try:
        logging.info("Starting Inspection Service")
        app.run(host=args.host, port=args.port, debug=True)
    finally:
        app.db_connection.close()
