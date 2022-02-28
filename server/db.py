from os import path
import json
import sqlite3
import string

# Error class for when request data is bad
class InspError(Exception):
    def __init__(self, message=None, error_code=400):
        Exception.__init__(self)
        if message:
            self.message = message
        else:
            self.message = "Bad Request"
        self.error_code = error_code

    def to_dict(self):
        rv = dict()
        rv['message'] = self.message
        return rv

# Utility factor to allow results to be used like a dictionary
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

"""
Wraps a single connection to the database with higher-level functionality.
"""
class DB:
    def __init__(self, connection):
        self.conn = connection
        
        
    def execute_script(self, script_file):
        with open(script_file, "r") as script:
            c = self.conn.cursor()
            # Only using executescript for running a series of SQL commands.
            c.executescript(script.read())
            self.conn.commit()

    def create_script(self):
        """
        Calls the schema/create.sql file
        """
        script_file = path.join("schema", "create.sql")
        if not path.exists(script_file):
            raise InspError("Create Script not found")
        self.execute_script(script_file)

    def seed_data(self):
        """
        Calls the schema/seed.sql file
        """
        script_file = path.join("schema", "seed.sql")
        if not path.exists(script_file):
            raise InspError("Seed Script not found")
        self.execute_script(script_file)
    
    def begin_transaction(self):
        """
        Begins the transaction.
        """
        c = self.conn.cursor()
        query = "BEGIN;"
        c.execute(query)    
        c.close()
        
    def commit_active(self):
        """
        Commits active transactions
        """
        self.conn.commit()
        
    def rollback_active(self):
        """
        Rolls back active transactions
        """
        self.conn.rollback()

    def find_restaurant(self, restaurant_id):
        """
        Searches for the restaurant with the given ID. Returns None if the
        restaurant cannot be found in the database.
        """
        # Load connection
        c = self.conn.cursor()
        
        # Performing the SQL query
        params = [str(restaurant_id)]
        query = '''SELECT * 
                  FROM ri_restaurants 
                  WHERE id = (?)'''
        c.execute(query,params)
        restaurant = c.fetchall()

        if not restaurant:
            return None
        return restaurant
    
    def find_restaurant_withinspection(self, inspection_id):
        """
        Finds restaurant given an inspection_id. Returns None if the
        restaurant is not found.
        """
        # Load connection
        c = self.conn.cursor()
        params = [str(inspection_id)]
        query = '''
                SELECT * 
                FROM ri_restaurants
                WHERE ri_restaurants.id IN (
                    SELECT restaurant_id 
                    FROM ri_inspections
                    WHERE ri_inspections.id = (?)
                )
                '''
        c.execute(query, params)
        rest_inspections = c.fetchall()
        
        if not rest_inspections:
            return None
        return rest_inspections
    
    def count_inspections(self):
        """
        Counts the number of records in the r_i inspections table.
        """
        # Load connection
        c = self.conn.cursor()
        
        # Performing the SQL query
        query = '''SELECT count(*) 
                FROM ri_inspections
                '''
        c.execute(query)
        cnt = c.fetchall()
        c.close()
        return cnt         

    def find_inspection(self, inspection_id):
        """
        Searches for the inspection with the given ID. Returns None if the
        inspection cannot be found in the database.
        """
        # Load connection
        c = self.conn.cursor()
        
        # Performing the SQL query
        params = [str(inspection_id)]
        query = '''SELECT * 
                  FROM ri_inspections 
                  WHERE id = (?)'''
        c.execute(query, params)
        inspection = c.fetchall()
        
        if not inspection:
            return None
        return inspection
 
    def find_inspections(self, restaurant_id):
        """
        Searches for all inspections associated with the given restaurant.
        Returns an empty list if no matching inspections are found.
        """
        # Load connection
        c = self.conn.cursor()
        
        # Performing the SQL query
        params = [str(restaurant_id)]
        query = '''SELECT * 
                  FROM ri_inspections 
                  WHERE restaurant_id = (?)
                  '''
        c.execute(query, params)
        inspections = c.fetchall()
        
        if not inspections:
            return []
        return inspections

    def check_restaurant(self, inspection):
        '''
        returns id if restuarant is in db (based on name/address),
        otherwise returns None
        '''
        # Load connection
        c = self.conn.cursor()
        
        # Check if restaurant exists
        params = [inspection['name'], inspection['address']]
        query = '''SELECT id 
                  FROM ri_restaurants 
                  WHERE name = ?
                  AND address = ?'''
        c.execute(query, params)        
        return c.fetchall()

    def insert_restaurant(self, inspection):
        '''
        Inserts new restaurant into restaurant table
        '''
        # Load connection
        c = self.conn.cursor()
        
        params = [inspection['name'], 
                  'Restaurant',
                  inspection['address'],
                  inspection['city'],
                  inspection['state'],
                  inspection['zip'],
                  inspection['latitude'],
                  inspection['longitude']]
        query = '''
        INSERT INTO ri_restaurants (
            name, facility_type, address, city, state, zip, latitude, longitude
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        '''
        c.execute(query, params)
    
    def insert_inspection(self, inspection, r_id):
        '''
        Inserts new inspection into inspections table
        '''
        # Load connection
        c = self.conn.cursor()

        params = [inspection['inspection_id'], 
                  inspection['risk'],
                  inspection['date'],
                  inspection['inspection_type'],
                  inspection['results'],
                  inspection['violations'],
                  r_id] 
        query = '''
        INSERT INTO ri_inspections (
            id, risk, inspection_date, inspection_type, results, violations,
            restaurant_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?);'''
        c.execute(query, params)

    def add_inspection_for_restaurant(self, inspection):
        """
        Finds or creates the restaurant then inserts the inspection and
        associates it with the restaurant.
        """        
        # Check if inspection in db
        if not self.find_inspection(inspection['inspection_id']):
            # Check if restuarant is in db
            rest_id = self.check_restaurant(inspection)
            if not rest_id:
                self.insert_restaurant(inspection)
                rest_id = self.check_restaurant(inspection)
                response_code = 201
            else:
                response_code = 200
            r_id = rest_id[0]['id']
            
            # Insert new inspection associated with rest_id
            self.insert_inspection(inspection, r_id)
            
            return response_code, r_id
            
        # Inspection already in DB - return none, no response
        return None, None      

    def check_tweet_location(self, tweet_lat, tweet_lon):
        '''
        Check if tweet is within a certain distance of a restaurant in the DB.
        Return the list of all restaurant_ids that match.
        '''
        # Load connection
        c = self.conn.cursor()

        params = [tweet_lat, tweet_lon]

        query = '''
        SELECT id FROM ri_restaurants 
        WHERE ABS(latitude - ?)  <= 0.00225001 
        AND ABS(longitude - ?) <= 0.00302190 
        ;'''
        c.execute(query, params)
        
        return [dict['id'] for dict in c.fetchall()]

    def check_tweet_name(self, tweet_text):
        '''
        Check if a tweet's text matches the name of a restuarant in the DB.
        Return the list of all matching restaurant_ids.
        '''
        # Load connection
        c = self.conn.cursor()

        ngram_list = get_all_ngrams(tweet_text)

        questionmarks = '?' * len(ngram_list)
        query = '''SELECT id 
                    FROM ri_restaurants 
                    WHERE name in (%s)''' % (",").join(questionmarks)

        params = ngram_list
        c.execute(query, params)
        
        return [dict['id'] for dict in c.fetchall()]


    def insert_tweet_match(self, tkey, r_id, match):
        '''
        Inserts new inspection into ri_tweetmatch table
        '''
        # Load connection
        c = self.conn.cursor()

        params = [tkey, r_id, match]
        query = '''
        INSERT INTO ri_tweetmatch (
            tkey, restaurant_id, match
        ) VALUES (?, ?, ?);'''
        c.execute(query, params)
        self.conn.commit() 

    def match_and_add_tweet(self, tweet):
        '''
        Checks tweet for matching restaurant, adds tweets to DB, and returns
        list of corresponding restaurant_ids.
        '''
        loc_match_list = self.check_tweet_location(tweet['lat'], tweet['long'])
        name_match_list = self.check_tweet_name(tweet['text'])

        # Combine lists to dictionary with correct labels
        tweet_match_dict = {r_id:  'geo' for r_id in loc_match_list}
        for r_id in name_match_list:
            if r_id in tweet_match_dict:
                tweet_match_dict[r_id] = 'both'
            else:
                tweet_match_dict[r_id] = 'name'

        # Add tweet to DB
        for r_id, match in tweet_match_dict.items():
            self.insert_tweet_match(tweet['key'], r_id, match)

        return [r_id for r_id in tweet_match_dict]

    def find_tweets(self, restaurant_id):
        """
        Searches for all tweets associated with the given restaurant.
        Returns an empty dict if no matching tweets are found.
        """
        # Load connection
        c = self.conn.cursor()
        
        # Performing the SQL query
        params = [str(restaurant_id)]
        query = '''SELECT * 
                  FROM ri_tweetmatch 
                  WHERE restaurant_id = (?)
                  '''
        c.execute(query, params)
        tweets = c.fetchall()
        if tweets:
            return {key: val for key, val in tweets[0].items() 
                        if key != 'restaurant_id'}
        return {}

def ngrams(tweet, n):
    single_word = tweet.translate(str.maketrans('', '', 
                                            string.punctuation)).upper().split()
    output = []
    for i in range(len(single_word) - n + 1):
        output.append(' '.join(single_word[i:i + n]))
    return output

def get_all_ngrams(tweet, max_n=4):
    ngrams_list = []
    for n in range(1, max_n+1):
        ngrams_list.extend(ngrams(tweet, n))
    return ngrams_list
