from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf

warehouse_location = abspath('spark-warehouse')

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("etl-posts-py") \
        .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
        .enableHiveSupport() \
        .getOrCreate()

    #print(SparkConf().getAll())

    spark.sparkContext.setLogLevel("INFO")

    get_users = "./data/user.json"
    get_posts = "./data/posts.json"
    get_comments = "./data/comments.json"

    dataframe_users = spark.read \
                    .format('json') \
                    .option('inferSchema', 'false') \
                    .option("multiline","true") \
                    .option('header', 'true') \
                    .json(get_users)
    
    dataframe_posts = spark.read \
                    .format('json') \
                    .option('inferSchema', 'false') \
                    .option("multiline","true") \
                    .option('header', 'true') \
                    .json(get_posts)

    dataframe_comments = spark.read \
                    .format('json') \
                    .option('inferSchema', 'false') \
                    .option("multiline","true") \
                    .option('header', 'true') \
                    .json(get_comments)

    print(dataframe_posts.rdd.getNumPartitions())

    

    spark.stop()