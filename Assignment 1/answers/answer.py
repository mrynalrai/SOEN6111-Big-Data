import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row])) \
        .reduce(lambda x, y: '\n'.join([x, y]))
    return a + '\n'

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, GitHub actions), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    
    # ADD YOUR CODE HERE
    # raise ExceptionException("Not implemented yet")
    row_count = 0
    with open(filename,'r', newline='') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=' ', quotechar='|')
        header = next(csv_reader)
        if header is not None:
            for row in csv_reader:
                row_count += 1
    return row_count

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    # raise ExceptionException("Not implemented yet")
    csv_file = open(filename, 'r')
    # creating dictreader object
    csv_reader = csv.DictReader(csv_file)
    # creating empty lists
    nom_parc = []
    # iterating over each row and append
    # values to empty list
    for col in csv_reader:
        if col['Nom_parc'] != '':
            nom_parc.append(col['Nom_parc'])
    return len(nom_parc)

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")
    csv_file = open(filename, 'r', encoding="ISO-8859-1")
    # creating dictreader object
    csv_reader = csv.DictReader(csv_file)
    # creating empty lists
    nom_parc = []
    # iterating over each row and append
    # values to empty list
    for col in csv_reader:
        if col['Nom_parc'] != '':
            nom_parc.append(col['Nom_parc'])
    sorted_parks = sorted(list(set(nom_parc)))
    sorted_uniq_parks = ""
    for park in sorted_parks:
        sorted_uniq_parks += park + "\n"
    return sorted_uniq_parks

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    csv_file = open(filename, 'r', encoding="ISO-8859-1")
    # creating dictreader object
    csv_reader = csv.DictReader(csv_file)
    # creating empty lists
    nom_parc = []
    # iterating over each row and append
    # values to empty list
    for col in csv_reader:
        if col['Nom_parc'] != '':
            nom_parc.append(col['Nom_parc'])
    nom_parc_count_dict = {}
    for park in nom_parc:
        if park not in nom_parc_count_dict:
            nom_parc_count_dict[park] = 1
        else:
            nom_parc_count_dict[park] += 1
    nom_parc_count = ""
    for parc_count in sorted(nom_parc_count_dict.items()):
        nom_parc_count += parc_count[0] + "," + str(parc_count[1]) + "\n"
    return nom_parc_count

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    csv_file = open(filename, 'r', encoding="ISO-8859-1")
    # creating dictreader object
    csv_reader = csv.DictReader(csv_file)
    # creating empty lists
    nom_parc = []
    # iterating over each row and append
    # values to empty list
    for col in csv_reader:
        if col['Nom_parc'] != '':
            nom_parc.append(col['Nom_parc'])
    nom_parc_count_dict = {}
    for park in nom_parc:
        if park not in nom_parc_count_dict:
            nom_parc_count_dict[park] = 1
        else:
            nom_parc_count_dict[park] += 1
    nom_parc_count = ""
    i = 0
    for parc_count in sorted(nom_parc_count_dict.items(), key=lambda x: (x[1], x[0]), reverse=True):
        nom_parc_count += parc_count[0] + "," + str(parc_count[1]) + "\n"
        i += 1
        if i >= 10:
            break
    return nom_parc_count

def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")
    csv_file1 = open(filename1, 'r', encoding="ISO-8859-1")
    csv_file2 = open(filename2, 'r', encoding="ISO-8859-1")
    # creating dictreader object
    csv_reader1 = csv.DictReader(csv_file1)
    csv_reader2 = csv.DictReader(csv_file2)
    # creating empty lists
    nom_parc1 = set()
    nom_parc2 = set()
    # iterating over each row and append
    # values to empty list
    for col in csv_reader1:
        if col['Nom_parc'] != '' and col['Nom_parc'] not in nom_parc1:
            nom_parc1.add(col['Nom_parc'])
    for col in csv_reader2:
        if col['Nom_parc'] != '' and col['Nom_parc'] not in nom_parc2:
            nom_parc2.add(col['Nom_parc'])
    park_inters = ""
    for park in sorted(nom_parc1.intersection(nom_parc2)):
        park_inters += park + "\n"
    return park_inters

'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    csv_data = spark.sparkContext.textFile(filename)
    header = csv_data.first()  # extract header
    csv_data = csv_data.filter(lambda row: row != header)
    return csv_data.count()

def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    rdd = data_frame.rdd
    parks_rdd = rdd.map(lambda row: row['Nom_parc']).filter(lambda park: park is not None)
    return parks_rdd.count()

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    rdd = data_frame.rdd
    parks_rdd = rdd.map(lambda row: row['Nom_parc']).filter(lambda park: park is not None).distinct().sortBy(lambda row: row).map(lambda row:[row])
    return toCSVLineRDD(parks_rdd)

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    rdd = data_frame.rdd
    parks_rdd = rdd.map(lambda r: (r['Nom_parc'], 1)).filter(lambda r: r[0] is not None)
    parks_rdd = parks_rdd.reduceByKey(lambda a, b: (a + b)).sortByKey().map(lambda row:[row[0],row[1]])
    return toCSVLineRDD(parks_rdd)

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    rdd = data_frame.rdd
    parks_rdd = rdd.map(lambda r: (r['Nom_parc'], 1)).filter(lambda r: r[0] is not None)
    parks_rdd = parks_rdd.reduceByKey(lambda a, b: (a + b)).takeOrdered(10, lambda r: (-r[1], r[0]))
    result_rdd = spark.sparkContext.parallelize(parks_rdd)
    return toCSVLineRDD(result_rdd)

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    rdd1 = spark.read.option("charset", "ISO-8859-1").csv(filename1, header=True).rdd
    rdd2 = spark.read.option("charset", "ISO-8859-1").csv(filename2, header=True).rdd
    rdd1 = rdd1.map(lambda row: row['Nom_parc']).filter(lambda park: park is not None).distinct()
    rdd2 = rdd2.map(lambda row: row['Nom_parc']).filter(lambda park: park is not None).distinct()
    result_rdd = rdd1.intersection(rdd2).sortBy(lambda r: r).map(lambda r: [r])
    return toCSVLineRDD(result_rdd)


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    return data_frame.count()

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    return data_frame.select("Nom_parc").dropna().count()

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    result_df = data_frame.select("Nom_parc").dropna().distinct().sort("Nom_parc")
    return toCSVLine(result_df)

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    result_df = data_frame.select("Nom_parc").groupBy("Nom_parc").count().orderBy("Nom_parc").dropna()
    result_df.show()
    return toCSVLine(result_df)

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    data_frame = spark.read.option("charset", "ISO-8859-1").csv(filename, header=True)
    data_frame = data_frame.select("Nom_parc").dropna()
    result_df = data_frame.groupBy("Nom_parc").count().orderBy(desc("count"), "Nom_parc").limit(10)
    return toCSVLine(result_df)

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    df1 = spark.read.option("charset", "ISO-8859-1").csv(filename1, header=True).select("Nom_parc").dropna()
    df2 = spark.read.option("charset", "ISO-8859-1").csv(filename2, header=True).select("Nom_parc").dropna()
    result_df = df1.intersect(df2).orderBy("Nom_parc")
    return toCSVLine(result_df)

'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    ddf = df.read_csv(filename, dtype={"No_Civiq": str, "Nom_parc": str})
    return len(ddf)

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    # raise Exception("Not implemented yet")
    ddf = df.read_csv(filename, dtype={"No_Civiq": str, "Nom_parc": str})
    return len(ddf.Nom_parc.dropna())

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    ddf = df.read_csv(filename, dtype={"No_Civiq": str, "Nom_parc": str})
    ddf_result = ddf[['Nom_parc']].dropna().drop_duplicates().sort_values("Nom_parc")
    result_str = ""
    for i in ddf_result['Nom_parc']:
        result_str += i + '\n'
    return result_str

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    ddf = df.read_csv(filename, dtype={"No_Civiq": str, "Nom_parc": str})
    ddf_result = ddf[['Nom_parc']].dropna().groupby(['Nom_parc'], sort = True).Nom_parc.count()
    result_str = ""
    for parc, num in ddf_result.iteritems():
        result_str += parc + "," + str(num) + "\n"
    return result_str

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    ddf = df.read_csv(filename, dtype={"No_Civiq": str, "Nom_parc": str})
    ddf_result = ddf[['Nom_parc']].dropna().groupby(['Nom_parc']).Nom_parc.count().nlargest(10).to_frame('Count').map_partitions(lambda df: df.sort_values(['Count', 'Nom_parc'],ascending=[False, True])).compute()
    result_str = ""
    for parc in ddf_result.iterrows():
        result_str += parc[0] + ',' + str(parc[1]['Count']) + '\n'
    return result_str

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    #raise Exception("Not implemented yet")
    ddf1 = df.read_csv(filename1, dtype={"No_Civiq": str, "Nom_parc": str})[['Nom_parc']].dropna().drop_duplicates()
    ddf2 = df.read_csv(filename2, dtype={"No_Civiq": str, "Nom_parc": str})[['Nom_parc']].dropna().drop_duplicates()
    ddf_result = df.merge(ddf1, ddf2, how ='inner', on =['Nom_parc']).sort_values('Nom_parc')
    result_str = ""
    for parc in ddf_result.iterrows():
        result_str += parc[1]['Nom_parc'] + '\n'
    return result_str