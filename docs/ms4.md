# Milestone 4 - Scaling Data Cleaning

In this milestone you are extending your cleaning from MS3 to handle larger datasets. In particular, you will:
 - Enable blocking of the restaurant data to limit the candidate set of potential pairs.
 - Within a block/umbrella set, you will use at least one index to accelerate look ups for at least one attribute.

**For testing, server.py now has a flag -s that will set `app.scaling=True`. In your cleaning function/endpoint. Check if this -s flag is set (`if app.scaling == True`, and if so use the new cleaning approach for this MS. Otherwise, use your cleaning approach from MS3. This will allow you to test your code's results. `python3 server.py -s`**


## Step 0 - Get Updated Datasets and dependencies

We will create a new large dataset for you to test with. The links will be posted on Ed. To start, work with dirty100.

## Step 1 - Enable Blocking

You should modify your cleaning process to now use blocking. Here, when your process begins, you should create a grouping of your records into mutually exclusive "blocks", based on one or more of these attributes: name, address, city, state, and zip. After creating a block of records, when carrying out matching, you will only consider records that match within the same block. For example, I take 100 records and create 3 blocks: AA BB and CC. A record in BB will only be checked against other records in BB. I strongly encourage you to use temporary tables to create these blocks. See https://database.guide/create-temporary-table-sqlite/ Remember that clean may be called multiple times, so your code for creating temp tables must be able to work in this case.

Your process should determine what temporary tables need to be created, and copy records into these temp tables (ideally these temp records have fewer data/attributes than the records in the full ri_inspections table). Your cleaning process should then iterate through each of these blocks, and perform your matching within the blocks. The output of the cleaning process is the same as MS3. Please document in MS4.txt how you created your blocks.


## Step 2 - Indexing within Blocks

For this step you will create an index within each of your blocks. This index should be used when looking for candidate records to match. There are several indexes you could create, but you only need one. It should be simple and it is ok if this reduces the candidate set of records you compare with. Please document in MS4.txt how you created your indexes.

## Step 3 - Evaluate Performance

For this step you should measure and compare the time it takes to run your cleaning process for both MS3 and MS4 (you would control this using the -s flag).  You should average at least 3 runs/processes for each size (100 and the larger file given) and type (MS3 vs MS4). Write up a short description of these results in MS4.txt. I suggest using the time command https://linuxize.com/post/linux-time-command/ to measure the run time for your loading and cleaning process. Altenratively you could instrument your clean function to record the time taken and log or print this time. 