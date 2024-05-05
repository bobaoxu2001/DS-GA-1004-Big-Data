# And pyspark.sql to get the spark session
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import avg, col, count

def main(spark, userID):

    '''
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{userID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{userID}/sailors.json')
    reserves = spark.read.json(f'hdfs:/user/{userID}/reserves.json')
    
    print('Printing boats inferred schema')
    boats.printSchema()
    
    print('Printing sailors inferred schema')
    sailors.printSchema()

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Create temporary view in order to run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    reserves.createOrReplaceTempView('reserves')

    ## Example 1
    # Sailors with high ratings - Spark SQL
    query1 = spark.sql('SELECT sailors.sid, sailors.sname, sailors.rating FROM sailors WHERE sailors.rating > 7')
    query1.show()

    # Sailors with high ratings - Spark
    
    results = sailors.filter(col("rating") > 7)\
                       .select("sid", "sname", "rating")
    results.show()


    ## Example 2
    # Get High rating Sailors along with number of boat reservations - Spark SQL
    query2 = spark.sql('SELECT s.sid, COUNT(r.bid) as num_reserves FROM sailors s JOIN reserves r ON s.sid = r.sid WHERE s.rating > 7 GROUP BY s.sid')
    query2.show()

    # Get High rating Sailors along with number of boat reservations - Spark Object Interface
    sailors_boats = sailors.join(reserves, "sid")

    sailors_boats = sailors_boats.filter(col("rating") > 7)\
                 .groupBy("sid")\
                 .agg(count("bid").alias("num_reserves")) \
                 .select("sid", "num_reserves")
    sailors_boats.show()
    
    # Example 3
    print("Select the name, color and average rental age for all boats whose average age of renters is below the mean")

    print("Spark SQL Query:")
    
    example_1_query = spark.sql("""
                                SELECT 
                                    bname, 
                                    color,
                                    AVG(age) as average_renter_age
                                FROM reserves
                                JOIN sailors ON sailors.sid = reserves.sid 
                                JOIN boats ON boats.bid = reserves.bid
                                GROUP BY bname, color
                                HAVING AVG(sailors.age) < (
                                    SELECT AVG(sailors.age)
                                    FROM sailors
                                    JOIN reserves ON reserves.sid = sailors.sid)
                                """)
    example_1_query.show()

    print("Spark Transformations:")

    joined_df = reserves.join(sailors, sailors.sid == reserves.sid).cache()
    avg_per_boat = joined_df.groupBy("bid").agg(avg("age").alias("avg_age"))
    avg_overall = joined_df.select(avg("age")).alias("overall_avg_age").collect()[0][0] # Access element in df
    
    below_avg_boats = avg_per_boat.filter(avg_per_boat.avg_age < avg_overall) 
    
    result = below_avg_boats.join(boats, boats.bid == below_avg_boats.bid). \
            select(boats.bname, boats.color, below_avg_boats.avg_age.alias("average_renter_age"))
    result.show()
        



if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()
    userID = os.environ['USER']

    # Call our main routine
    main(spark, userID)