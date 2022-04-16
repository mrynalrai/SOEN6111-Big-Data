import os
import sys
import copy
import time
import random
import pyspark
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import desc, size, max, abs, monotonically_increasing_id
from pyspark.sql import functions as F
from all_states import all_states

'''
INTRODUCTION

With this assignment you will get a practical hands-on of frequent 
itemsets and clustering algorithms in Spark. Before starting, you may 
want to review the following definitions and algorithms:
* Frequent itemsets: Market-basket model, association rules, confidence, interest.
* Clustering: kmeans clustering algorithm and its Spark implementation.

DATASET

We will use the dataset at 
https://archive.ics.uci.edu/ml/datasets/Plants, extracted from the USDA 
plant dataset. This dataset lists the plants found in US and Canadian 
states.

The dataset is available in data/plants.data, in CSV format. Every line 
in this file contains a tuple where the first element is the name of a 
plant, and the remaining elements are the states in which the plant is 
found. State abbreviations are in data/stateabbr.txt for your 
information.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def toCSVLineRDD(rdd):
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row])) \
        .reduce(lambda x, y: '\n'.join([x, y]))
    return a + '\n'

def toCSVLine(data):
    if isinstance(data, RDD):
        if data.count() > 0:
            return toCSVLineRDD(data)
        else:
            return ""
    elif isinstance(data, DataFrame):
        if data.count() > 0:
            return toCSVLineRDD(data.rdd)
        else:
            return ""
    return None


'''
PART 1: FREQUENT ITEMSETS

Here we will seek to identify association rules between states to 
associate them based on the plants that they contain. For instance, 
"[A, B] => C" will mean that "plants found in states A and B are likely 
to be found in state C". We adopt a market-basket model where the 
baskets are the plants and the items are the states. This example 
intentionally uses the market-basket model outside of its traditional 
scope to show how frequent itemset mining can be used in a variety of 
contexts.
'''

def data_frame(filename, n):
    '''
    Write a function that returns a CSV string representing the first 
    <n> rows of a DataFrame with the following columns,
    ordered by increasing values of <id>:
    1. <id>: the id of the basket in the data file, i.e., its line number - 1 (ids start at 0).
    2. <plant>: the name of the plant associated to basket.
    3. <items>: the items (states) in the basket, ordered as in the data file.

    Return value: a CSV string. Using function toCSVLine on the right 
                  DataFrame should return the correct answer.
    Test file: tests/test_data_frame.py
    '''
    spark = init_spark()
    data_frame = spark.read.text(filename)
    # generating 'plant' column with plant data
    # generating 'items' column with reset of the string (containings states with commas)
    df_plant_states = data_frame.withColumn("plant", F.expr("""substring(value,1,instr(value,',')-1)"""))\
      .withColumn("items", F.expr("""substring(value,instr(value,',')+1,length(value))""")).drop("value")
    # adding id column
    df = df_plant_states.withColumn('id', monotonically_increasing_id()).select('id', 'plant', 'items').limit(n)
    # converting string type to list type for column 'items'
    df = df.withColumn('items', F.split('items', ","))
    return toCSVLine(df)

def frequent_itemsets(filename, n, s, c):
    '''
    Using the FP-Growth algorithm from the ML library (see 
    http://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html), 
    write a function that returns the first <n> frequent itemsets 
    obtained using min support <s> and min confidence <c> (parameters 
    of the FP-Growth model), sorted by (1) descending itemset size, and 
    (2) descending frequency. The FP-Growth model should be applied to 
    the DataFrame computed in the previous task. 
    
    Return value: a CSV string. As before, using toCSVLine may help.
    Test: tests/test_frequent_items.py
    '''
    spark = init_spark()
    data_frame = spark.read.text(filename)
    # generating 'plant' column with plant data
    # generating 'items' column with reset of the string (containings states with commas)
    df_plant_states = data_frame.withColumn("plant", F.expr("""substring(value,1,instr(value,',')-1)""")) \
        .withColumn("items", F.expr("""substring(value,instr(value,',')+1,length(value))""")).drop("value")
    # adding id column
    df = df_plant_states.withColumn('id', monotonically_increasing_id()).select('id', 'plant', 'items')
    # converting string type to list type for column 'items'
    df = df.withColumn('items', F.split('items', ","))

    # Using FP-Growth
    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=s)
    model = fpGrowth.fit(df)

    # Generate n frequent itemsets sorted by (1) descending itemset size, and (2) descending frequency
    freq_items_df = model.freqItemsets.orderBy([size('items'), 'freq'], ascending=[0, 0]).limit(n)

    return toCSVLine(freq_items_df)

def association_rules(filename, n, s, c):
    '''
    Using the same FP-Growth algorithm, write a script that returns the 
    first <n> association rules obtained using min support <s> and min 
    confidence <c> (parameters of the FP-Growth model), sorted by (1) 
    descending antecedent size in association rule, and (2) descending 
    confidence.

    Return value: a CSV string.
    Test: tests/test_association_rules.py
    '''
    spark = init_spark()
    data_frame = spark.read.text(filename)
    # generating 'plant' column with plant data
    # generating 'items' column with reset of the string (containings states with commas)
    df_plant_states = data_frame.withColumn("plant", F.expr("""substring(value,1,instr(value,',')-1)""")) \
        .withColumn("items", F.expr("""substring(value,instr(value,',')+1,length(value))""")).drop("value")
    # adding id column
    df = df_plant_states.withColumn('id', monotonically_increasing_id()).select('id', 'plant', 'items')
    # converting string type to list type for column 'items'
    df = df.withColumn('items', F.split('items', ","))

    # Using FP-Growth
    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=s)
    model = fpGrowth.fit(df)

    # Generate n association rules sorted by (1) descending antecedent size in association rule, and (2) descending confidence.
    assoc_rules_df = model.associationRules\
        .select('antecedent', 'consequent', 'confidence')\
        .orderBy([size('antecedent'), 'confidence'], ascending=[0, 0]).limit(n)

    return toCSVLine(assoc_rules_df)

def interests(filename, n, s, c):
    '''
    Using the same FP-Growth algorithm, write a script that computes 
    the interest of association rules (interest = |confidence - 
    frequency(consequent)|; note the absolute value)  obtained using 
    min support <s> and min confidence <c> (parameters of the FP-Growth 
    model), and prints the first <n> rules sorted by (1) descending 
    antecedent size in association rule, and (2) descending interest.

    Return value: a CSV string.
    Test: tests/test_interests.py
    '''
    spark = init_spark()
    data_frame = spark.read.text(filename)
    # generating 'plant' column with plant data
    # generating 'items' column with reset of the string (containings states with commas)
    df_plant_states = data_frame.withColumn("plant", F.expr("""substring(value,1,instr(value,',')-1)""")) \
        .withColumn("items", F.expr("""substring(value,instr(value,',')+1,length(value))""")).drop("value")
    # adding id column
    df = df_plant_states.withColumn('id', monotonically_increasing_id()).select('id', 'plant', 'items')
    # converting string type to list type for column 'items'
    df = df.withColumn('items', F.split('items', ","))
    data_points_count = df.count()

    # Using FP-Growth
    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=s)
    model = fpGrowth.fit(df)

    # Generate n association rules sorted by (1) descending antecedent size in association rule, and (2) descending confidence.
    assoc_rules_df = model.associationRules \
        .select('antecedent', 'consequent', 'confidence')

    # Generate n frequent itemsets
    freq_items_df = model.freqItemsets

    # Joining assoc_rules_df and assoc_rules_df to obtain frequency of consequents
    assoc_rules_df_with_freq = assoc_rules_df.join(freq_items_df, assoc_rules_df.consequent == freq_items_df.items, 'inner')

    # Calculating interests
    assoc_rules_df_with_interest = assoc_rules_df_with_freq.withColumn('interest', assoc_rules_df_with_freq.confidence - (assoc_rules_df_with_freq.freq/data_points_count))\
        .orderBy([size('antecedent'), 'interest'], ascending=[0, 0]).limit(n)

    return toCSVLine(assoc_rules_df_with_interest)

'''
PART 2: CLUSTERING

We will now cluster the states based on the plants that they contain.
We will reimplement and use the kmeans algorithm. States will be 
represented by a vector of binary components (0/1) of dimension D, 
where D is the number of plants in the data file. Coordinate i in a 
state vector will be 1 if and only if the ith plant in the dataset was 
found in the state (plants are ordered alphabetically, as in the 
dataset). For simplicity, we will initialize the kmeans algorithm 
randomly.

An example of clustering result can be visualized in states.png in this 
repository. This image was obtained with R's 'maps' package (Canadian 
provinces, Alaska and Hawaii couldn't be represented and a different 
seed than used in the tests was used). The classes seem to make sense 
from a geographical point of view!
'''


def update_dict(plants, init_dict):
    '''
    This function returns an updated dictionary with the values of plant present in state changed to True
    '''
    init_dict_copy = init_dict.copy()
    # Update value to True if plant is present in plants
    for plant in plants:
        init_dict_copy[plant] = True
    return init_dict_copy


def data_preparation(filename, plant, state):
    '''
    This function creates an RDD in which every element is a tuple with 
    the state as first element and a dictionary representing a vector 
    of plant as a second element:
    (name of the state, {dictionary})

    The dictionary should contains the plant names as keys. The 
    corresponding values should be 1 if the plant occurs in the state 
    represented by the tuple.

    You are strongly encouraged to use the RDD created here in the 
    remainder of the assignment.

    Return value: True if the plant occurs in the state and False otherwise.
    Test: tests/test_data_preparation.py
    '''
    spark = init_spark()
    rdd = spark.sparkContext.textFile(filename).map(lambda row: row.split(','))
    # Collect list of Plants
    all_plants = rdd.map(lambda row: row[0]).collect()
    all_plants.sort()
    # Initialize a dictionary with plants as key and False are values
    init_dict = dict.fromkeys(all_plants, False)
    # Generating pairs of state and plants
    state_plant_pairs = rdd.flatMap(lambda row: [(state, row[0]) for state in row[1:len(row)]])
    # Filtering out states not present in all_states
    # Combining plants for states
    state_plants = state_plant_pairs.filter(lambda row: row[0] in all_states).groupByKey()
    state_plants_dict_rdd = state_plants.map(lambda row: (row[0], update_dict(row[1], init_dict)))

    filtered_plants = state_plants_dict_rdd.filter(lambda row: row[0] == state).collect()
    return filtered_plants[0][1][plant]

def distance2(filename, state1, state2):
    '''
    This function computes the squared Euclidean
    distance between two states.
    
    Return value: an integer.
    Test: tests/test_distance.py
    '''
    spark = init_spark()
    rdd = spark.sparkContext.textFile(filename).map(lambda row: row.split(','))
    # Collect list of Plants
    all_plants = rdd.map(lambda row: row[0]).collect()
    all_plants.sort()
    # Initialize a dictionary with plants as key and False are values
    init_dict = dict.fromkeys(all_plants, False)
    # Generating pairs of state and plants
    state_plant_pairs = rdd.flatMap(lambda row: [(state, row[0]) for state in row[1:len(row)]])
    # Filtering out states not present in all_states
    # Combining plants for states
    state_plants = state_plant_pairs.filter(lambda row: row[0] in all_states).groupByKey()
    state_plants_dict_rdd = state_plants.map(lambda row: (row[0], update_dict(row[1], init_dict)))

    # Filter tuple for each state
    filtered_plants_state_one = state_plants_dict_rdd.filter(lambda row: row[0] == state1).collect()
    filtered_plants_state_two = state_plants_dict_rdd.filter(lambda row: row[0] == state2).collect()

    # Assign dictionary for each state
    dict_one = filtered_plants_state_one[0][1]
    dict_two = filtered_plants_state_two[0][1]

    # Calculate Euclidean distance
    # Coordinate i in a state vector will be 1 if and only if the ith plant in the dataset was found in the state
    # Distance is difference between coordinates i for each plant in a state
    dist = 0
    for plant in all_plants:
        dist += (dict_one[plant] - dict_two[plant]) ** 2

    return dist

def init_centroids(k, seed):
    '''
    This function randomly picks <k> states from the array in answers/all_states.py (you
    may import or copy this array to your code) using the random seed passed as
    argument and Python's 'random.sample' function.

    In the remainder, the centroids of the kmeans algorithm must be
    initialized using the method implemented here, perhaps using a line
    such as: `centroids = rdd.filter(lambda x: x[0] in
    init_states).collect()`, where 'rdd' is the RDD created in the data
    preparation task.

    Note that if your array of states has all the states, but not in the same
    order as the array in 'answers/all_states.py' you may fail the test case or
    have issues in the next questions.

    Return value: a list of <k> states.
    Test: tests/test_init_centroids.py
    '''
    random.seed(seed)
    res = random.sample(all_states, k)
    return res


def get_closest_centroid(src, centroids, all_plants):
    '''
    This function returns the closest centroid to the src
    '''
    # Initialize the closest centroid with first state
    closest_centroid = centroids[0][0]

    closest_dist = 0
    for plant in all_plants:
        closest_dist += (src[plant] - centroids[0][1][plant]) ** 2

    # loop over all the centroids to find the closest centroid
    for centroid in centroids:
        curr_dist = 0
        for plant in all_plants:
            curr_dist += (src[plant] - centroid[1][plant]) ** 2
        if curr_dist < closest_dist:
            closest_dist = curr_dist
            closest_centroid = centroid[0]

    return closest_centroid


def first_iter(filename, k, seed):
    '''
    This function assigns each state to its 'closest' class, where 'closest'
    means 'the class with the centroid closest to the tested state
    according to the distance defined in the distance function task'. Centroids
    must be initialized as in the previous task.

    Return value: a dictionary with <k> entries:
    - The key is a centroid.
    - The value is a list of states that are the closest to the centroid. The list should be alphabetically sorted.

    Test: tests/test_first_iter.py
    '''
    spark = init_spark()
    rdd = spark.sparkContext.textFile(filename).map(lambda row: row.split(','))
    # Collect list of Plants
    all_plants = rdd.map(lambda row: row[0]).collect()
    all_plants.sort()
    # Initialize a dictionary with plants as key and False are values
    init_dict = dict.fromkeys(all_plants, False)
    # Generating pairs of state and plants
    state_plant_pairs = rdd.flatMap(lambda row: [(state, row[0]) for state in row[1:len(row)]])
    # Filtering out states not present in all_states
    # Combining plants for states
    state_plants = state_plant_pairs.filter(lambda row: row[0] in all_states).groupByKey()
    state_plants_dict_rdd = state_plants.map(lambda row: (row[0], update_dict(row[1], init_dict)))

    states_rnd = init_centroids(k, seed)

    # Filter (state, dictionary) for states_rnd
    centroids = state_plants_dict_rdd.filter(lambda row: row[0] in states_rnd).collect()

    # Map each state to the closest state/centroid
    closest_states_pair = state_plants_dict_rdd.map(lambda row: (get_closest_centroid(row[1], centroids, all_plants), row[0]))
    # Group states by centroids
    closest_centroid_states = closest_states_pair.groupByKey()
    # Sort the states for each centroid
    closest_centroid_states_sorted = closest_centroid_states.map(lambda row: (row[0], sorted(row[1]))).collect()
    closest_centroid_states_sorted_dict = dict(closest_centroid_states_sorted)

    return closest_centroid_states_sorted_dict


def combine_plant_tuples(tuple_one, tuple_two, all_plants):
    '''
    This function combines two plant dictionaries to create a new plant dictionary
    '''
    combined_plant_tuples_dict = {}

    for plant in all_plants:
        combined_plant_tuples_dict[plant] = tuple_one[0][plant] + tuple_two[0][plant]

    total_points = tuple_one[1] + tuple_two[1]
    return combined_plant_tuples_dict, total_points

def kmeans(filename, k, seed):
    '''
    This function:
    1. Initializes <k> centroids.
    2. Assigns states to these centroids as in the previous task.
    3. Updates the centroids based on the assignments in 2.
    4. Goes to step 2 if the assignments have not changed since the previous iteration.
    5. Returns the <k> classes.

    Note: You should use the list of states provided in all_states.py to ensure that the same initialization is made.
    
    Return value: a list of lists where each sub-list contains all states (alphabetically sorted) of one class.
                  Example: [["qc", "on"], ["az", "ca"]] has two 
                  classes: the first one contains the states "qc" and 
                  "on", and the second one contains the states "az" 
                  and "ca".
    Test file: tests/test_kmeans.py
    '''
    spark = init_spark()
    rdd = spark.sparkContext.textFile(filename).map(lambda row: row.split(','))
    # Collect list of Plants
    all_plants = rdd.map(lambda row: row[0]).collect()
    all_plants.sort()
    # Initialize a dictionary with plants as key and False are values
    init_dict = dict.fromkeys(all_plants, False)
    # Generating pairs of state and plants
    state_plant_pairs = rdd.flatMap(lambda row: [(state, row[0]) for state in row[1:len(row)]])
    # Filtering out states not present in all_states
    # Combining plants for states
    state_plants = state_plant_pairs.filter(lambda row: row[0] in all_states).groupByKey()
    state_plants_dict_rdd = state_plants.map(lambda row: (row[0], update_dict(row[1], init_dict)))

    states_rnd = init_centroids(k, seed)

    # Filter (state, dictionary) for states_rnd
    centroids = state_plants_dict_rdd.filter(lambda row: row[0] in states_rnd).collect()

    old_centroids = {}

    for itr in range(0, 100):

        # Map each state to the closest state/centroid
        closest_states_pair = state_plants_dict_rdd.map(
            lambda row: (get_closest_centroid(row[1], centroids, all_plants), row[0]))
        # Group states by centroids
        closest_centroid_states = closest_states_pair.groupByKey()
        # Sort the states for each centroid
        closest_centroid_states_sorted = closest_centroid_states.map(lambda row: (row[0], sorted(row[1]))).collect()
        curr_centroids = dict(closest_centroid_states_sorted)

        # Break if kmeans converges
        if curr_centroids == old_centroids:
            break
        # Else find new centroids
        new_centroids = []
        for centroid, plant_dict in centroids:

            # filter out the dictionary for the current state in centroids
            current_dist = state_plants_dict_rdd.filter(lambda row: row[0] in curr_centroids[centroid])
            # obtain tuples of (plant_dictionary, 1)
            plant_dict_one_tuple = current_dist.map(lambda row: (row[1], 1))
            # reduce to obtain the sum of these points and total number of points belonging to the current centroid
            centroid_agg = plant_dict_one_tuple.reduce(lambda tuple_one, tuple_two: combine_plant_tuples(tuple_one, tuple_two, all_plants))

            # Creating centroid vector with each plant as key and value / total number of values
            centroid_vector = {plant: (value / centroid_agg[1]) for (plant, value) in centroid_agg[0].items()}
            # Appending new centroid
            new_centroids.append((centroid, centroid_vector))

        centroids = new_centroids
        old_centroids = curr_centroids

    return list(curr_centroids.values())

