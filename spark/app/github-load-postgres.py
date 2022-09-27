from pickletools import int4
import sys
from pyspark import SparkContext, SparkFiles
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType, DecimalType

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
import os
from datetime import datetime
import os
from pyspark.sql.functions import col, split, udf


# Create spark session
spark = (SparkSession
         .builder
         .master("local")
         .config("spark.driver.extraJavaOptions", "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'")
         .config("spark.executor.extraJavaOptions", "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'")
         .getOrCreate()
         )
sc = spark.sparkContext

####################################
# Parameters
####################################

postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILES")
print("######################################")

type_mapping = {
    'CreateEvent': 1,
    'CommitCommentEvent': 2,
    'ReleaseEvent': 3,
    'DeleteEvent': 4,
    'MemberEvent': 5,
    'PullRequestEvent': 6,
    'ForkEvent': 7,
    'IssuesEvent': 8,
    'WatchEvent': 9,
    'IssueCommentEvent': 10,
    'PublicEvent': 11,
    'PushEvent': 12,
    'GollumEvent': 13,
    'PullRequestReviewCommentEvent': 14,
    'PullRequestReviewEvent': 15
}


def translate(mapping: dict):
    def translate_(col):
        return mapping.get(col)
    return udf(translate_, IntegerType())


def df_from_file():

    NOW = datetime.now()
    CURRENT_DATE_STR = NOW.strftime('%Y-%m-%d')
    CURRENT_HOUR_STR = str(NOW.hour - 2)

    file = CURRENT_DATE_STR + '-' + CURRENT_HOUR_STR + '.json.gz'
    gh = 'http://data.gharchive.org/'
    sc.addFile(gh + file)
    path = SparkFiles.get(file)
    # print("SparkFiles root: " + SparkFiles.getRootDirectory())
    # print("SparkFiles root contents" +
    #       ','.join(os.listdir(SparkFiles.getRootDirectory())))

    json = spark.read.json(path)
    json.createOrReplaceTempView("github")
    print(json.head())
    # 2022-09-22T00:00:00Z
    # %Y-%m-%dT%H:%M:%SZ
    # , 'yyyy-MM-ddTHH:mm:ssZ'
    # eventsDF = spark.sql("SELECT id,type as type_str,repo.id as repo_id_str,repo.name as owner_repo,actor.id as actor_id_str, actor.login as actor,created_at as created_at_str FROM github") \
    eventsDF = spark.sql("SELECT id as event_id_str,type as event_type,repo.id as repo_id_str,repo.name as repo,\
    actor.id as actor_id_str, actor.login as actor_name,created_at as created_at_str FROM github") \
        .withColumn("owner", split(col("repo"), "/").getItem(0)) \
        .withColumn("repo_name", split(col("repo"), "/").getItem(1)) \
        .withColumn('created_at', to_timestamp(col("created_at_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .drop("created_at_str")\
        .withColumn('actor_id', col("actor_id_str").cast(LongType())) \
        .withColumn('repo_id', col("repo_id_str").cast(LongType())) \
        .withColumn('event_id', col("event_id_str").cast(LongType()))\
        .drop("actor_id_str")\
        .drop("repo_id_str")\
        .drop("event_id_str")\
        .drop("owner_repo") \

    # .withColumn("type", translate(type_mapping)("type_str")) \

    # .drop("type_str")
    return eventsDF


eventsDF = df_from_file()
# print(eventsDF.head())

# namesDF = eventsDF.select('owner').withColumnRenamed('owner', 'name').union(
#     eventsDF.select('actor').withColumnRenamed('actor', 'name')).distinct()


####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

(
    eventsDF.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.events")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

# (
#     df_ratings_csv_fmt
#     .select([c for c in df_ratings_csv_fmt.columns if c != "timestamp_epoch"])
#     .write
#     .format("jdbc")
#     .option("url", postgres_db)
#     .option("dbtable", "public.ratings")
#     .option("user", postgres_user)
#     .option("password", postgres_pwd)
#     .mode("overwrite")
#     .save()
# )
