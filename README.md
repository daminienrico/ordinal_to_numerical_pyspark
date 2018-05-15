# From nominal values to numerical values -Spark Example (pyspark)

## About

This is a practical example of how to transform nominal values to numerical values using Spark. 

Basically, the main idea is to use the hash function used for the **Spark default partitioning problem** : 
How do I choose which keys go to a reducer and which go to the other? And what is the fastest way?

(It's made it by using Jupyter Notebook)

```python
import findspark
from pyspark.sql import SparkSession

findspark.init("/usr/local/spark")
spark = SparkSession.builder \
   .master("local[*]") \
   .appName("Test") \
   .getOrCreate()
sc = spark.sparkContext
```


```python
import os

file_path = "/Users/Desktop/data"
files = os.listdir(file_path)[1:]
```


```python
def apply_preprocessing(rdd) :
    '''
    This function applies some transformations 
     - **parameters**, **types**, **return** and **return types**::
          :param rdd: RDD to transform
          :type rdd: pyspark.rdd.RDD
          :return: return the transformed RDD 
          :rtype: pyspark.rdd.RDD
    '''
    header = rdd.first()
    rdd = rdd.filter(lambda lines : lines!=header)
    rdd = rdd.map(lambda (a,b) : ((a,b),1))
    rdd = rdd.reduceByKey(lambda a,b : a+b)
    return rdd 
```

```python
def main() :
    for file_path in files : 
        rdd_new = sc.textFile(file_path)
        rdd_new = apply_preprocessing(rdd_new)
    return rdd_info    
```

```python
rdd = main()
```

I need to know the total number of instances for generating an unique ID, which is a number, for each instance.
* *b* is the value to transform:
* *(you can find other ways to count the number of total istances)*
```python 
tot_istances = rdd.map(lambda ((a,b),c) : (b,1)).keys().distinct().count()

```

Look [here](https://gist.github.com/hanleybrand/5224673)for better understanding this function.
```python
def java_string_hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000
```


```python
import sys 

def get_hash(istance) :
    return (java_string_hashcode(istance) & sys.maxint) % tot_istances
    
```


```python
rdd_final  = rdd.map(lambda ((a,b),c) : (a,get_hash(b),c ))
```


```python
rdd_final.take(5)
```




    [(u'10', 12630, 1),
     (u'100000', 13676, 1),
     (u'100001', 13676, 2),
     (u'1000012', 15200, 1),
     (u'1000014', 4931, 1)]




```python
rdd.take(5)
```




    [((u'10', '20-2-2-13-8'), 1),
     ((u'100000', '8-5-25'), 1),
     ((u'100001', '8-5-25'), 2),
     ((u'1000012', '3-13'), 1),
     ((u'1000014', '5-23-10'), 1)]

The two identical string (8-5-25) are assigned at the same ID, which is  13676


