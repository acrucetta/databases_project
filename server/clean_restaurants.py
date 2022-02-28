# Implement cleaning for MS3
import jellyfish
from statistics import mean
from datetime import date, datetime

SIM_SCORE_THRESHOLD = 0.9
SIMILARITY_EQ_INPUTS = {'name': 0.5, 'address': 0.5}

def get_restaurants(db):
    '''
    Get all restaurant records.
    '''
    # Load connection
    c = db.conn.cursor()
    # Performing the SQL query
    query = '''SELECT id, name, address, city, state, zip, clean 
                FROM ri_restaurants;'''
    c.execute(query)
    restaurants = c.fetchall()
    return restaurants


def get_temp_restaurants(db):
    '''
    Get all temp restaurant records.
    '''
    # Load connection
    c = db.conn.cursor()
    # Performing the SQL query
    query = '''SELECT *
                FROM restaurant_block;'''
    c.execute(query)
    restaurants = c.fetchall()
    return restaurants


def get_zip_codes(db):
    '''
    Get list of unique zip codes
    '''
    # Load connection
    c = db.conn.cursor()
    # Performing the SQL query
    query = '''SELECT DISTINCT zip 
                FROM ri_restaurants;'''
    c.execute(query)
    zips = c.fetchall()
    return zips


def clean_by_block(db):
    '''
    Creates a block of data given an existing list of zip codes.
    '''
    # get all unique zip codes
    zips = get_zip_codes(db)

    # Start with blank ri_linked table
    clear_linked_records(db)

    for zip_code in zips:
        # Create temp table on zip blocks (#restaurant_blocks)
        create_block(db, zip_code['zip'])
        # Assign index to zip block
        create_index(db)
        # Cleaning temp restaurants
        clean_all_restaurants(db, True)


def create_block(db, zip_code):
    '''
    Creates a single temp table with all restaurants with a certain zip code.
    '''
    c = db.conn.cursor()
    query1 = "DROP TABLE IF EXISTS restaurant_block;"
    c.execute(query1)
    query2 = '''
            CREATE TEMP TABLE restaurant_block AS
            SELECT id, name, address, clean
            FROM ri_restaurants
            WHERE zip = ?;
            '''
    params = [zip_code]
    c.execute(query2, params)
    db.conn.commit()


def create_index(db):
    '''
    Create index for block
    '''
    c = db.conn.cursor()
    query = '''
            CREATE INDEX NameIndex ON restaurant_block(name);
            '''
    c.execute(query)
    db.conn.commit()


def get_similarity(record1, record2):
    """
    Computes similarity between two records

    Args:
        record1 (tuple): tuple with combination of name, address, city,  state, or zip
        record2 (tuple): tuple with combination of name, address, city,  state, or zip
    
    E.g.,record1 = (NAME, ADDRESS, CITY, STATE, ZIP) 
    """ 
    compound_score = 0
    total_pct = 0
    sim_attr_dict =  SIMILARITY_EQ_INPUTS
    for attr, pct in sim_attr_dict.items():
        score = jellyfish.jaro_winkler_similarity(record1[attr], record2[attr])
        compound_score += score * pct
        total_pct += pct
        # if first score is low enough, don't check other attributes
        remaining_pct = 1 - total_pct
        if compound_score + remaining_pct < SIM_SCORE_THRESHOLD:
            return 0
        
    return compound_score


def compute_similarities(db, restaurants):
    """
    Iterates over all restaurant records and computes similarity scores

    Args:
        restaurants (list): [description]
    Returns:
        list of tuples (id1, id2, similarity score), list of unmatched records (ids)
    """    
    sim_scores = []

    for i, record1 in enumerate(restaurants):
        for record2 in restaurants[i+1:]:
            sim_score = get_similarity(record1,record2)
            if sim_score >= SIM_SCORE_THRESHOLD:  
                id1, id2 = record1['id'], record2['id']
                sim_scores.append((id1, id2, sim_score))               
    return sim_scores 


def select_primary_record(sim_scores):
    """
    Given list of all sim scores, create a new dict of the form 
    key (primary record), and values (set of matching records).
    If there are only two records, use the first.
    If there are >2, use the record with highest avg sim score.

    Args:
        sim_scores (list): list of tuples (id1, id2, similarity score)

    Returns:
        primary_records: {2: {2, 5}, 10: {1, 4, 10}} where 2 and 10 are the primary records
        with the highest avg sim score
    """    
    # create dict of each rest id and list of all matching scores
    score_dict = {}
    for id1, id2, score in sim_scores:
        score_dict[id1] = score_dict.get(id1, []) + [(id2, score)]
        score_dict[id2] = score_dict.get(id2, []) + [(id1, score)]

    # calculate avg score for each rest id
    for id1, score_list in score_dict.items():
        avg_score = mean([score for id, score in score_list])
        score_dict[id1] = avg_score

    # get unique groups of matching records
    record_groups = []
    for id1, id2, score in sim_scores:
        ind = 0
        for group in record_groups:
            if id1 in group or id2 in group:
                record_groups.remove(group)
                new_group = group.union({id1, id2})
                record_groups.append(new_group)
                ind = 1
                break
        if ind==0:
            record_groups.append({id1, id2})

    # assign primary record as record with highest avg sim score
    primary_records = {}
    for group in record_groups:
        max_score = 0
        for id in group:
            score = score_dict[id]
            if score > max_score:
                primary_records = {key:val for key, val in primary_records.items() 
                                   if val!=group}
                primary_records[id] = group
                max_score = score
    return primary_records


def clear_linked_records(db):
    '''
    Clears all linked records from ri_linked
    '''
    # Load connection
    c = db.conn.cursor()
    query = '''DELETE FROM ri_linked;'''
    c.execute(query)
    db.conn.commit()


def insert_linked_record(db, primary_records):
    """
    Insert records into linked inspections table.

    Args:
        db ([type]): [description]
        primary_records: e.g., {2: {2, 5}, 10: {1, 4, 10}}
    """
    
    # Load connection
    c = db.conn.cursor()
    
    # Performing the SQL query
    for primary_id,value_set in primary_records.items():
        for linked_id in value_set:
            params = [primary_id, linked_id]
            query = '''
            INSERT INTO ri_linked (
                primary_rest_id, original_rest_id
            ) VALUES (?, ?);'''
            c.execute(query, params)
    db.conn.commit()


def update_ri_inspections(db,primary_records):
    '''
    Update the ri_inspections for all linked records to 
    point to the selected primary record.
    '''
    # Load connection
    c = db.conn.cursor()
    
    for primary_id,value_set in primary_records.items():
        for linked_id in value_set:
            params = [primary_id, linked_id]
            query = '''
                    UPDATE ri_inspections
                    SET restaurant_id = ?
                    WHERE restaurant_id = ?;
                    '''
            c.execute(query, params)
    db.conn.commit()
            
    
def get_cleaned_records(primary_records):
    '''
    Parses the primary records dictionary
    and outputs it as a list.
    '''
    clean_records = []
    for record, linked_set in primary_records.items():
        for linked_id in linked_set:
            clean_records.append(linked_id)
    return clean_records
    

def mark_as_clean(db):
    '''
    Mark all restaurants as clean
    '''
    # Load connection
    c = db.conn.cursor()
    query = '''
                UPDATE ri_restaurants
                SET clean = 1;
                '''
    c.execute(query)
    db.conn.commit()


def clean_all_restaurants(db, temp=False):
    '''
    Cleans all restaurants if any restaurants are dirty
    '''
    if temp is False:
        restaurants = get_restaurants(db)
        # Start with blank table
        clear_linked_records(db)
    else:
        restaurants = get_temp_restaurants(db)
    
    dirty_restaurants = [r for r in restaurants if not r['clean']]
    if dirty_restaurants:
        sim_scores = compute_similarities(db, restaurants)
        primary_records = select_primary_record(sim_scores)
        insert_linked_record(db, primary_records)
        update_ri_inspections(db, primary_records)


# STEP 2 BELOW
def get_single_restaurant(db,restaurant_id):
    '''
    Returns information for single restaurant given an inspection id
    '''
    # Load connection
    c = db.conn.cursor()
    params = [restaurant_id['original_rest_id']]
    query = '''SELECT * 
                FROM ri_restaurants
                WHERE id = ?'''
    c.execute(query,params)
    return c.fetchall()

def get_linked_restaurants(db,inspection_id):
    '''
    Returns all linked ids associated with a restaurant ID
    '''
    # Load connection
    c = db.conn.cursor()
    params = [inspection_id]
    query = '''SELECT original_rest_id 
                FROM ri_linked
                WHERE primary_rest_id IN (
                    SELECT restaurant_id 
                    FROM ri_inspections
                    WHERE id = ?)'''
    c.execute(query,params)
    return c.fetchall()

def get_primary_rest_id(db,inspection_id):
    '''
    Returns data for main restaurant
    '''
    # Load connection
    c = db.conn.cursor()
    params = [inspection_id]
    query = '''SELECT * 
                FROM ri_restaurants
                WHERE id IN (
                    SELECT restaurant_id 
                    FROM ri_inspections
                    WHERE id = ?)'''
    c.execute(query,params)
    return c.fetchall()

def create_json_output(db,inspection_id):
    """
    Modify the primary_records output so that we have as output:
    { "primary" : { <primary rest JSON>},
     "linked" : [ {<rest JSON> }, {<rest JSON>} ],
     "ids" : [ id1, id2, id3]}
    
    Args:
        db ([type]): [description]
        primary_records: e.g., {2: {2, 5}, 10: {1, 4, 10}}
    Returns:
        [type]: [description]
    """ 
    linked_rests = []
    ids = []   
    main_restaurant = get_primary_rest_id(db,inspection_id)
    linked_ids = get_linked_restaurants(db,inspection_id)
    # Get linked restaurants
    for linked_id in linked_ids:
        rest_linked = get_single_restaurant(db,linked_id)
        linked_rests.append(rest_linked)
        ids.append(linked_id['original_rest_id'])
    return {"primary":main_restaurant,"linked":linked_rests,"ids":ids}