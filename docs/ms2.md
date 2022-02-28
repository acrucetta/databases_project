# Milestone 2 - Bulk and Transactional Loading

In this milestone you are extending the end points of your web service to support two new major changes.  
 - The first major change is enabling transactional loading of inspections. New endpoints allow the user to set the number of inspection requests to batch together (e.g. number of requests to put together in a transaction). New end points also exist for committing or rolling back pending transactions.
 - The second major change is to integrate a partially fake twitter dataset. A new endpoint will take a tweet and potentially match it to restaurants if the name is in the tweet or if the location of the tweet is near a restaurant. A new endpoint will list the tweet keys/ids that matched the given restaurant id.
 - A new minor endpoint returns the count of loaded inspections.
 - A new minor endpoint reset maps to your /create endpoint.  This should just work 'out of the box' but is worth checking. 

See the documentation on the endpoints at http://people.cs.uchicago.edu/~aelmore/class/30235/docs/ to see what functionality you will need to implement. The new endpoints are tagged with MS2.


The code in server/ has many instances of `#TODO MS2` that represent various pieces of functionality that you need to implement to complete the milestone. If the TODO is followed by a return or raise, you will need to replace the following line with the appropriate return value. These can be found in server.py (the main web/app service). You will need add new functions/methods in db.py (the data access object/layer that will interface with the DBMS). You can add new classes/files to help with abstracting functionality if you want. Remember to check these in! 

## New client
This milestone has a new client program loader2.py.  This client takes as input a script json file that can invoke endpoints and/or another script file.  Each endpoint has expected response code(s) and optionally a body that will be returned.

## Step 1 Count Inspection Records
Implement the function
```
@app.get("/count")
def count_insp():
```
This endpoint simply counts the number of records in the ri_inspections table and returns a simple integer N  where N is the number of records in the table.

### Test 1.1
We have provided a simple test case data/MS2/ms2.json that will test your create, reset, inspections, and count. Take a look at the file to get an understanding of how the new client operates. 

To run this test, start your server and in a new shell/terminal:
```
python3 client/loader2.py -f data/MS2/ms2.json
```

Your client output should look like:
```
Running script data/MS2/ms2.json
Called http://localhost:30235/create
Called http://localhost:30235/count
Expected content matched
Ran file small-insp.json Successful 9
Called http://localhost:30235/count
Expected content matched
Called http://localhost:30235/reset
Called http://localhost:30235/count
Expected content matched
Done
```

## Step 2 Transactional Loading

Read through the API documentation on transactional loading.  You will need to do several things to make this work.  While this step looks complicated the amount of code you will need to write will be very small!  Note this is not how you would typically use transactions in a real system.

You will need to do several things:
 - Understand how to commit and rollback a transaction (be sure you are not using autocommit). Look at when you are committing in your current code, you will likely need to remove some of it.
 - Create variable to track the transaction size (I would suggest putting this in app)
 - Track the number of times inspections are called  with a counter variable. (I would suggest putting this in app)
 - Determine where you should call commit based on the above information. Make sure you update your counter.


Implement the function
```
@app.get("/txn/<txnsize:int>")
def set_transaction_size(txnsize):
```
This endpoint should set the transaction size, such that `<txnsize>` inspection post requests are batched into a single transaction commit. 
Note this may result in more than `<txnsize>` inserts to the DB, as single post may result in two inserts (rest and inspection). Here, we are 
counting the number of times `/inspections` is called. It is advisable that when the txnsize is changed you log the new value and the prior value 
to ensure that you are correctly storing the value. Please read the documentation on this endpoint.

### Test 2.1
We have provided a couple tests to check if your transaction code is working, however you will likely need to implement the next step to fully test (due to Sqlite3 and sharing a single connection to the DB).

```
python3 client/loader2.py -f data/MS2/ms2t2.json
```
and 
```
python3 client/loader2.py -f data/MS2/ms2t7commit.json
```

## Step 3 Abort/ Rollback
Implement the function
```
@app.get("/abort")
def abort_txn():
```
This endpoint will reset/rollback any active transaction.  As a result any pending inspection operations should be undone.

### Test 3.1
```
python3 client/loader2.py -f data/MS2/ms2t7abort.json
```

Your output should look like 
```
Called http://localhost:30235/create
Called http://localhost:30235/txn/7
Ran file small-insp.json Successful 9
Called http://localhost:30235/abort
Called http://localhost:30235/count
Expected content matched
Done
```

## Step 4 - Tweet Matcing

In this step you are extending the end points of your web service to support matching tweets against restaurants, either by the tweet mentioning a restaurant by name or if the tweet is geo-tagged (eg has lat/long) and is within a box ~500 meters.


**Warning the Twitter data came from a crawl and is not filtered. I have not checked the Tweets for offensive language or content. If you want me to provide a sanitized version, we can look into trying to filter out certain content.**

Implement the function
```
@app.post("/tweet")
def tweet():
```
This endpoint takes a tweet as a JSON object via a post request. This endpoint checks if the tweet matches any restaurants in the database, by name or location. If so, the endpoint stores the tweet key and restaurant id in the new ri_tweetmatch table. This record should also note how the match was made using the string variable match :
```
match varchar(20) CHECK( match IN ('geo','name','both'))  NOT NULL,
```

 - To match by location, if the tweet has a lat and long value, then you see if the lat/long falls within ~500 meters of any restaurant in the database. Normally the distance calculation has some cost associated with it as the Earth is a sphere, but we will use the very simple and innacurate calculation of checking if the tweet's location falls within +/- 0.00225001 latitude and +/- 0.00302190 longitude of a restaurants location. Note, never use this method in the real world.
 - To match by name we are going to split the text of tweet into words by spaces, commas, hyphens, and periods. The name match should be case *insensitive* We will then consider all 1,2,3, and 4 gram words for candidate restaurant names. We have provided you with a function that will split the words and generate an N-gram for you in the function `ngrams(tweet, n)` . For example passing the tweet text `"Chowing at Dragon Bowl. Yum!"` to the ngram function with n as 1 gives: `['Chowing', 'at', 'Dragon', 'Bowl', 'Yum']` and n as 2 gives: `['Chowing at', 'at Dragon', 'Dragon Bowl', 'Bowl Yum']`. Here the 2gram 'Dragon Bowl' matches a restaurant name and so the key and restaurant ID should be added to the ri_tweetmatch table with the match as name.

This endpoint should return a list of restaurant IDs that matched the tweet eg `matches: [100,131,4131]` or an empty list if none were matched `matches: []`

Hint: for generating query to check if a name in list of values you can use something like the following code. This will generate the SQL with the right number of ? in the string.
```
questionmarks = '?' * len(rest_names)
sql = "select id from ri_restaurants where name in (%s)" % (",").join(questionmarks)
```
This is taken from https://stackoverflow.com/questions/31473451/sqlite3-in-clause


## Step 4.1 Get Tweets for a Restaurant
Next implement the function:
```
@app.get("/tweets/<restaurant_id:int>")
def find_restaurant_tweets(restaurant_id):
```

This should simply return a list of tweets that matched the restaurant. Each item should have a tweet id/key and the method how the tweet was matched to the restaurant.  

## Step 4.2 Test Tweets
For this milestone there is a simple test that loads several tweets, 2 of which match (1 via name and the other via geo).
```
 python3 client/loader2.py -f data/MS2/ms2tweet1.json
```

You should see an output like
```
Running script data/MS2/ms2tweet1.json
Called http://localhost:30235/create
Ran file small-insp.json Successful 9
Called http://localhost:30235/count
Expected content matched
Ran file tweet-very-small.json Successful 6
Called http://localhost:30235/tweets/7
Expected content matched
Called http://localhost:30235/tweets/1
Expected content matched
Done
```

Feel free to add more tests. We will add larger test cases for a later MS.
If you think your output on the read tweets by restaurant is right, please let us know. We have only tested this on
our reference solution thus far.