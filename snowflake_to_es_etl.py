'''
Best performing with => after adding server 55.21 to cluster; completed in 51mins ; command :
spark-submit --master yarn --deploy-mode cluster --num-executors 8 --executor-cores 4  --driver-memory 6g
--archives pyenv.tar.gz#environment --jars sqljdbc_11.2/enu/mssql-jdbc-11.2.0.jre8.jar
--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.4.1  snowflake_to_es_etl.py
'''

#import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Nested
from elasticsearch_dsl import analysis
from elasticsearch_dsl import Date
from elasticsearch_dsl import Document
from elasticsearch_dsl import Integer
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Text
from elasticsearch_dsl import InnerDoc
from elasticsearch_dsl import DenseVector
from elasticsearch_dsl.connections import connections as es_connections
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
import json
#import requests
import warnings
#import BeautifulSoup
import re
import snowflake.connector
import pandas as pd


### THIS IS AN ETL SCRIPT WHERE SOURCE SYSTEM IS SNOWFLAKE AND TARGET SYSTEM IS ELASTICSEARCH


es = Elasticsearch('<elastic_ip_address>:9200')
es_connections.create_connection(hosts=['http://<elastic_ip_address>:9200/'], timeout=20)
# local[6] denotes that you’re using a local cluster, which is another way of saying you’re running in single-machine mode.
# Number "6" tells Spark to create 6 worker threads as logical cores on your machine.
#spark = SparkSession.builder.master("local[4]").appName("Incident Association Data Bulk Indexing").enableHiveSupport().getOrCreate()
appName = "APP => sr_data_indexing => Script that reads from SnowFlake & writes to Elasticsearch "
master = "yarn"
#master = "local"
conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master)


SNOWFLAKE_AUTH_USER = '<sf_auth_user>'
SNOWLFAKE_AUTH_PASSWORD = '<sf_auth_password>'
SNOWFLAKE_AUTH_ROLE = '<sf_auth_role>'
SNOWFLAKE_AUTH_ACCOUNT = '<sf_auth_acct>'
SNOWFLAKE_AUTH_DB = '<sf_auth_db>'
SNOWFLAKE_WAREHOUSE = '<sf_warehouse>'
SNOWFLAKE_SCHEMA = '<sf_schema>'
SNOWFLAKE_ROLE = '<sf_role>'

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

warnings.filterwarnings("ignore")

sr_data_snowflake_query = """select distinct 
                        Incident_Number as Incident_Number, summary as Summary, Notes as Description,
                        submit_date_time as Open_Date, Closed_date_time as Close_Date, assigned_group as Assigned_Group,
                        Status as Status, Mnemonic as Client_Mnemonic, Universal_ID as Sr_Num,
                        Assignee_full_name as Assignee, Resolution_product_name as product_name,
                        Resolution_product_category_tier_3 as solution_family, NULL as Row_ID,
                        Owner_full_name as owner, NULL as source, NULL as severity, NULL as
                        priority, NULL as solution_detail, Status_Reason as sub_status,
                        NULL as area, NULL as sub_area,'Remedy' as data_source, Resolution as Resolution_Txt, Last_Resolved_date_time as last_resolved_date,
                        case when Last_Resolved_date_time is not null then DATEDIFF(DAY, submit_date_time, Last_Resolved_date_time)
                        when Closed_date_time is not null then DATEDIFF(DAY, submit_date_time, Closed_date_time) else
                        DATEDIFF(DAY, submit_date_time, current_timestamp()) end as TAT, LAST_MODIFIED_DATE_TIME as last_modified_date
                    from
                        Service_Management.INCIDENT_PUBLIC
                    WHERE
                        assigned_group not like '%[_]DOD[_]%' and assigned_group not like '%[_]DOD'
                        and assigned_group not like '%[_]VA[_]%' and assigned_group not like '%[_]VA' and assigned_group not like '%[_]HR[_]%'
                        and assigned_group not like '%[_]HR' and Mnemonic not in ('CERN_KCM') and assigned_group not in
                        ('DS_CHLD_DC','DS_ALBR_PA','DS_CHES_NY','DS_LOWE_MA','DS_BH_AL','DS_WIN_MN', 'DS_UNIV_MO','DS_TRUM_MO',
                        'DS_LCOX_MO','DS_YAVA_AZ','DS_NHS_DE','DS_MCH_TX','DS_PALO_CA','DS_SGEN_OH','DS_FTHC_WI','DS_WMMC_MO',
                        'DS_RUTL_VT','DS_GLEN_NY','DS_SANJ_NM','DS_BLTM_WI','DS_STIL_MO','DS_EJEF_LA','Infra_Systems_ALBR_PA',
                        'Infra_Systems_STIL_MO','Infra_Sys_Admin_NWH_WA','Help_Desk_CC','Help_Desk_Accounts_CC')
"""


df = spark.read.format('jdbc') \
.option("url", f"jdbc:snowflake://{SNOWFLAKE_AUTH_ACCOUNT}.snowflakecomputing.com/") \
.option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
.option("user", SNOWFLAKE_AUTH_USER) \
.option("password", SNOWLFAKE_AUTH_PASSWORD) \
.option("warehouse", SNOWFLAKE_WAREHOUSE) \
.option("database", SNOWFLAKE_AUTH_DB) \
.option("role", SNOWFLAKE_ROLE) \
.option("schema", SNOWFLAKE_SCHEMA) \
.option('query', sr_data_snowflake_query) \
.load()


df.cache()
print(f"++++++++++++++++++++++ Spark DF number of rows = {df.count()} =========================================")
df.show(5,False)


class SRDataIndex(Document):
    Incident_Number = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    Sr_Num = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    Summary = Text(analyzer='english')
    Description = Text(analyzer='english')
#    Open_Date = Date(
#        format="yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
#    Close_Date = Date(
#        format="yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
#    last_resolved_date = Date(
#        format="yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
#    last_modified_date = Date(
#        format="yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
    Resolution_Txt = Text(analyzer='english')
    Assigned_Group = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    Status = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    Client_Mnemonic = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    Assignee = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    product_name = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    solution_family = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    Row_ID = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    owner = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    source = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    severity = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    priority = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    solution_detail = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    sub_status = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    area = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    sub_area = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    data_source = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
    reported_release = Keyword(
        normalizer=analysis.normalizer(
            "lowercaseNorm", filter=["lowercase", "asciifolding"],
        ),
    )
#    tat = Integer()

    class Index:

        """
        Sr data Index class
        """
        name = 'sr_data_index'
        using = es

if not es.indices.exists(index='sr_data_index'):
    SRDataIndex.init()

cols_to_drop = ['Open_Date', 'Close_Date', 'last_resolved_date', 'last_modified_date', 'tat']
df = df.drop(*cols_to_drop)

df = df.select(*(col(c).cast("string").alias(c) for c in df.columns))
print(f"Partitions =>>>>>>>>>>>>>>>>>>>>>>>>>   {df.rdd.getNumPartitions()}   <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ")
df = df.repartition(60)
#exit()
df.write.format("org.elasticsearch.spark.sql") \
.option("es.resource", "spark_sr_data_index" ) \
.option("es.nodes", "<elastic_ip_address>") \
.option("es.port", "9200") \
.option("es.mapping.id", "INCIDENT_NUMBER") \
.option("es.write.operation", "upsert") \
.mode("append") \
.save()

