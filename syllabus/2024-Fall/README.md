# Course Information <br/> for <br/> Fall Quarter 2024


## [Santa Clara University](http://scu.edu/)

## [MSIS 2527: Big Data Modeling & Analytics](https://www.scu.edu/business/graduate-degrees/ms-programs/ms-information-systems/curriculum/)

-----------------

## Main Subjects

| Subject              | Percentage     |        
|----------------------|----------------|
|1. MapReduce Paradigm | 25%            |
|2. PySpark and Spark  | 65%            |
|3.Data Partitioning and SQL Queries | 10%|

------

## Course Description & Objectives

* Understand the fundamentals of big data 
* Understand the fundamentals of MapReduce Paradigm
* Use PySpark (Python API for Apache Spark) to solve big data problems
* Use SQL for NoSQL data (DataFrames in Spark and Amazon Athena)
* Understand Amazon Athena & Google BigQuery: Access & Analyze Big Data by SQL

-----------------

## Course Objectives

At the completion of this course, students will be able to understand:

* **Elements of Big Data**: 
	* Cluster Comouting
	* Persistence, Queries, Analytics
	* Data Replication
	* Distributed File System and Fault Tolerance
	* Scale-out Architecture vs. Scale-up Architecture

* **What is MapReduce paradigm?**
	* Data partitioning and partitions
	* Mapper function: `map()`
	* Reducer function: `reduce()`
	* Combiner function: `combine()`
	* Sort & Shuffle: SQL's `GROUP BY`
	* Classic MapReduce Algorithms
	* Data Design Patterns

* **Fundamentals of Spark and PySpark:**
	* Spark Architecture
	* Spark: engine for large-scale data analytics
	* Data Abstractions in Spark and PySpark
	* RDDs and DataFrames
	* Transformations and Actions
	* Running simple programs in PySpark
	
* **NoSQL Databases & Serverless Architectures**
	* SQL for NoSQL data & Relational Algebra
	* Amazon Athena and SQL
	* Google BigQuery and SQL

----------------
## Required Books

#### [1. Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)
for the first 3 weeks of class

#### [2. Data Algorithms with Spark by Mahmoud Parsian](https://www.oreilly.com/library/view/data-algorithms-with/9781492082378/)
for the last 7 weeks of class

------------------

## Required Software, API, and Documentation

* [Apache Spark (main site)](http://spark.apache.org)
* [PySpark API and documentation](https://spark.apache.org/docs/latest/api/python/index.html)
* [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
* [DataFrame Programming Guide](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)

-------------------

## Tentative Course Outline

	The weekly coverage might change as it 
	depends on the  progress of the class. 
	However,  you must  keep up  with  the 
	reading and  programming  assignments.


----------------

## Session-1:  Tuesday, September 24, 2024

TOPIC: Introduction to Big Data and MapReduce

* REQUIRED:
	* [MapReduce: Simplified Data Processing on Large Clusters by Jeffrey Dean and Sanjay Ghemawat](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
	* [Chapter 1 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) 
	* [A Very Brief Introduction to MapReduce](http://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf) 
* OPTIONAL:
	* [Introduction to Big Data](https://lagesoft.files.wordpress.com/2018/11/bd-introduction-to-big-data.pdf)
	* [Introduction to MapReduce](http://lsd.ls.fi.upm.es/lsd/nuevas-tendencias-en-sistemas-distribuidos/IntroToMapReduce_2.pdf) 

----------------

## Session-2: Thursday, September 26, 2024
	
TOPIC: Introduction to Big Data and MapReduce

* REQUIRED:
	* [MapReduce Tutorial Slides by Jimmy Lin](https://cs.uwaterloo.ca/~jimmylin/publications/WWW2013-MapReduce-tutorial-slides.pdf)
	* [Chapter 2 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf) 
	* [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf) 
	* [MapReduce: Simplified Data Processing on Large Clusters by Jeffrey Dean and Sanjay Ghemawat](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

* OPTIONAL:
	* [MapReduce wikipedia](https://en.wikipedia.org/wiki/MapReduce)
	* [A Comprehensive Study on MapReduce Applications](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/A_Comprehensive_Study_on_MapReduce_Applications.pdf)
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf) 

----------------

## Session-3: Tuesday, October 1, 2024

TOPIC: Introduction to MapReduce

* REQUIRED:
	* [Introduction to MapReduce](https://kodu.ut.ee/~srirama/cloud/2011/L3_MapReduce.pdf)
	* [Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   
	* [Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   

* OPTIONAL:
	* [Introduction to MapReduce: Watch a Video](https://www.youtube.com/watch?v=ht3dNvdNDzI&t=250s) 
	* [The Future of Big Data by Matei Zaharia -- Video](https://www.youtube.com/watch?v=oSj2vYw5RLs)   
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)  

----------------

## Session-4: Thursday, October 3, 2024


TOPIC: Introduction to MapReduce

* REQUIRED:
	* [Chapter 3 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   
	* [Chapter 4 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   
	* [Chapter 5 of Data-Intensive Text Processing with MapReduce](http://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)   

	* [MapReduce Algorithms](https://courses.cs.ut.ee/MTAT.08.027/2018_spring/uploads/Main/L5_MapReduceAlgorithms2018.pdf)
	* Classic Join in MapReduce (inner join)
	* [Join Algorithms Using MapReduce](https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.186.2140&rep=rep1&type=pdf)

* OPTIONAL:
	* [Simplifying Big Data Applications with Apache Spark 2.0 by Matei Zaharia](https://www.youtube.com/watch?v=Zb9YW8XjxnE)  
	* [Introduction to MapReduce and Hadoop by Matei Zaharia](https://github.com/mahmoudparsian/big-data-mapreduce-course/blob/master/slides/mapreduce/MapReduce_by_Matei_Zaharia.pdf)   
	* [Relational Operations Using MapReduce](https://medium.com/swlh/relational-operations-using-mapreduce-f49e8bd14e31)
	
----------------

## Session-5: Tuesday, October 8, 2024

TOPIC: Review of MapReduce paradigm with Examples

----------------

## Session-6: Thursday, October 10, 2024

* Exam-1, in-class
* Closed books/notes/software/internet/friends

----------------

## Session-7: Tuesday, October 15, 2024

TOPIC: Introduction to Spark & PySpark

* REQUIRED:
	* [A-Gentle-Introduction-to-Apache-Spark](pages.databricks.com/rs/094-YMS-629/images/A-Gentle-Introduction-to-Apache-Spark.pdf)
	* [Chapters 1, 2 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)  
	* [Classic Word Count in PySpark](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/wordcount)

* OPTIONAL:
	* [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)
	* [Introduction to Apache Spark](https://stanford.edu/~rezab/sparkclass/slides/itas_workshop.pdf)  

----------------

## Session-8: Thursday, October 17, 2024

TOPIC: Introduction to Spark and PySpark (Python API for Spark)

* REQUIRED:
	* [Classic Word Count in PySpark](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/wordcount)
	* [A-Gentle-Introduction-to-Apache-Spark](pages.databricks.com/rs/094-YMS-629/images/A-Gentle-Introduction-to-Apache-Spark.pdf)
	* [Chapters 1, 2, 3, 4 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)

* OPTIONAL:
	* [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)  
	* [Introduction to Spark by Shannon Quinn](http://cobweb.cs.uga.edu/~squinn/mmd_s15/lectures/lecture13_mar3.pdf)  

----------------

## Session-9: Tuesday, October 22, 2024

TOPIC: Spark's Nuts and Bolts

* REQUIRED:   
	* [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)  
	* [Chapters 3, 4, 5 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* [Learning Spark (book)](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)

* OPTIONAL:
	* [Introduction to Spark](http://www.slideshare.net/jeykottalam/spark-sqlamp-camp2014)  
	* [Parallel-Programming-With-Spark-Matei-Zaharia](http://ampcamp.berkeley.edu/wp-content/uploads/2013/02/Parallel-Programming-With-Spark-Matei-Zaharia-Strata-2013.pptx) 

----------------

## Session-10: Thursday, October 24, 2024

TOPIC: Data Design Patterns

* REQUIRED:     
	* [PySpark Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/pyspark_tutorial)  
	* [MinMax Algorithm](https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/map-partitions/README.md)
	* [Top-10 Algorithm](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/Top-N) 

----------------

## Session-11: Tuesday, October 29, 2024

TOPIC: Data Design Patterns

* REQUIRED:  
	* [Chapters 3, 4, 5 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1)    
	* Data Design Patterns: InMapper Combiner, mapPartitions
	* [Top-10 Algorithm](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/Top-N) 
	* [MinMax Algorithm](https://github.com/mahmoudparsian/pyspark-tutorial/blob/master/tutorial/map-partitions/README.md)

* OPTIONAL:
	* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)

----------------

## Session-12: Thursday, October 31, 2024

TOPIC: RDD Design Patterns

* REQUIRED:     
	* Spark's RDD Partitioning
	* [Chapters 3, 4, 5 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
	* Spark's `mapPartitions()` Transformation
	* [mapPartitions() Tutorial](https://github.com/mahmoudparsian/data-algorithms-with-spark/tree/master/code/bonus_chapters/mappartitions)
	* Review reducers: `groupByKey()`, `reduceByKey()`, and `combineByKey()`

----------------

## Session-13: Tuesday, November 5, 2024
	
* Review for  Exam-2
* Problem solving & Q/A session 
	
----------------

## Session-14: Thursday, November 7, 2024
	
* Exam-2, in-class
* Closed books/notes/software/internet/friends


----------------

## Session-15: Tuesday, November 12, 2024
	
* Spark's DataFrames (1)
* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
* [Video: Structuring Spark: SQL, DataFrames, Datasets And Streaming - 28 mins](https://www.youtube.com/watch?v=1a4pgYzeFwE)

----------------

## Session-16: Thursday, November 14, 2024
	
* Spark's DataFrames (2)
* [Chapters 4, 6, 7, 12 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
* [Video: Structuring Spark: SQL, DataFrames, Datasets And Streaming - 28 mins](https://www.youtube.com/watch?v=1a4pgYzeFwE)

----------------

## Session-17: Tuesday, November 19, 2024

* Introduction to Graph data structures
* MapReduce Design Pattern: Graph Algorithms 
* [Chapter 6 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
* [Chapters 11 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
 
-----------------

## Session-18: Thursday, November 21, 2024
	
* MapReduce Design Pattern: Graph Algorithms 
* [Chapter 6 of Data Algorithms with Spark Book by Mahmoud Parsian](https://www.amazon.com/Data-Algorithms-Spark-Recipes-Patterns/dp/1492082384/ref=sr_1_1) 
* [Chapters 11 of PySpark Algorithms Book by Mahmoud Parsian](https://github.com/mahmoudparsian/pyspark-algorithms)
  
----------------

## November 25-29, Thanksgiving Recess

* Academic holiday (no classes)
* No Office Hours

----------------

## Session-19: Tuesday, December 3, 2024
	
* Introduction to Serverless Analytics  
* SQL Access to Big Data  
	* SQL Access: Amazon Athena 
	* SQL Access: Google BigQuery 

---------------

## Session-20: Thursday, December 5, 2024

* Review for Final Exam 
* Q/A session 

----------------

## Session-21: Final Exam 

* In-Class Exam
* Date: Tuesday, December 10, 2024
* Time: 5:45 PM - 7:45 PM PST (TBDL)
* closed book/notes/friend/internet/software


----------------
