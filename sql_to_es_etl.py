
from elasticsearch_dsl import Boolean
from pyspark.sql.types import *
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

### THIS IS AN ETL SCRIPT WHERE SOURCE SYSTEM IS SQL SERVER AND TARGET SYSTEM IS ELASTICSEARCH

es = Elasticsearch('<elastic_ip_address>:9200')
es_connections.create_connection(hosts=['http://<elastic_ip_address>:9200/'], timeout=20)
# local[6] denotes that you’re using a local cluster, which is another way of saying you’re running in single-machine mode.
# Number "6" tells Spark to create 6 worker threads as logical cores on your machine.
#spark = SparkSession.builder.master("local[4]").appName("Incident Association Data Bulk Indexing").enableHiveSupport().getOrCreate()
appName = "APP => sr_data_indexing => Script that reads from SQL Server & writes to Elasticsearch "
master = "yarn"
#master = "local"
conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master)


dbserver='<db_server>'
dbname='<db_name>'
dbuser='<db_username>'
dbpassword='<db_password>'

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession
spark.sparkContext.setLogLevel('WARN') # default is INFO log-level, it is too much to open on browser for debugging

warnings.filterwarnings("ignore")


sr_data_query = """
    select 
             a.INCDNT_NUM SR_NUM, a.DESC_TXT SR_sum_txt, a.DTL_DESC_TXT as DESC_TXT,  
             a.RPRT_DTTM as OPEN_DTTM, a.CLS_DTTM as CLSE_DTTM, a.ASSGN_GRP as Team, 
             a.STS_CD as Status, b.org_mnem_nm as ORG_MNEM_NM, a.unv_tckt_num, 
             e.PERS_PRF_FULL_NM as Assignee, a.CLSR_RMDY_PROD_NM as Solution, 
             a.CLSR_RMDY_PROD_CAT_3 as SOLTN_FMLY, a.W_INCDNT_ID as Row_ID, 
             d.PERS_PRF_FULL_NM as owner, 'Remedy' as data_source, a.RSLTN_TXT, a.LST_RSLVD_DTTM, 
             case when a.LST_RSLVD_DTTM is not null then DATEDIFF(DAY, a.SBMT_DTTM, a.LST_RSLVD_DTTM) 
             when a.CLS_DTTM is not null then DATEDIFF(DAY, a.SBMT_DTTM, a.CLS_DTTM) else 
             DATEDIFF(DAY, a.SBMT_DTTM, getdate()) end as TAT, a.LST_MDFD_DTTM as LAST_MODIFIED_DATE, 
             case when a.assgn_grp like '%[_]DOD[_]%' or a.assgn_grp like '%[_]DOD' or a.assgn_grp like '%[_]VA[_]%' 
             or a.assgn_grp like '%[_]VA' or a.assgn_grp like '%[_]GOV[_]%' or a.assgn_grp like '%[_]GOV'
             then 1 else 0 end as is_federal 
         from 
             D_INCDNT a 
             left outer join m_org_d b on (a.w_rmdy_clnt_org_id=b.w_org_id) 
             left outer join M_PERS_D e on e.W_Pers_ID=a.W_ASSGN_PERS_ID 
             left outer join M_PERS_D d on d.W_Pers_ID=a.W_OWNR_PERS_ID 
    """


df = spark.read.format('jdbc') \
.option('url',f'jdbc:sqlserver://{dbserver};databaseName={dbname};encrypt=true;trustServerCertificate=true;') \
.option('driver','com.microsoft.sqlserver.jdbc.SQLServerDriver') \
.option('query', sr_data_query) \
.option('user',dbuser) \
.option('password',dbpassword) \
.load()
df.persist()
df = df.repartition(300)

df = df.withColumn("Row_ID", df["Row_ID"].cast(StringType()))
df = df.selectExpr("Row_ID","SR_NUM as Incident_Number","SR_sum_txt as Summary","DESC_TXT as Description","LAST_MODIFIED_DATE as last_modified_date", "Status","RSLTN_TXT as Resolution_Txt",
    "SOLTN_FMLY as solution_family","Solution as product_name","OPEN_DTTM as Open_Date","CLSE_DTTM as Close_Date","Team as Assigned_Group","ORG_MNEM_NM as Client_Mnemonic","unv_tckt_num as Sr_Num","Assignee","owner","data_source",
    "LST_RSLVD_DTTM as last_resolved_date","tat","is_federal")
df = df.withColumn("month_year", concat_ws('-', month('Open_Date'), year('Open_Date')))
df = df.withColumn("month_year", df["month_year"].cast(StringType()))
df = df.withColumn("is_federal", df["is_federal"].cast(BooleanType()))
df.show(50)
print(f' DF shape  is: {df.count()}, {len(df.columns)}')
print(df.printSchema())


class SRDataIndex(Document):
    """
    SR data index
    """
    Incident_Number = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    Sr_Num = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    Summary = Text(analyzer='english')
    Description = Text(analyzer='english')
    Open_Date = Date(format="yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
    Close_Date = Date(format="yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
    last_resolved_date = Date(format="yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
    last_modified_date = Date(format="yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd HH:mm:ss.SS||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis||strict_date_optional_time")
    Resolution_Txt = Text(analyzer='english')
    Assigned_Group = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    Status = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    Client_Mnemonic = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    Assignee = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    product_name = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    solution_family = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    Row_ID = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    owner = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    data_source = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    month_year = Keyword(normalizer=analysis.normalizer("lowercaseNorm", filter=["lowercase", "asciifolding"]))
    tat = Integer()
    is_federal = Boolean()
    word_embedding_500d_sr_data = DenseVector(dims="500")


    class Index:
        """
        Sr data Index class
        """
        name = 'sr-data-index'


if not es.indices.exists(index='sr-data-index'):
    SRDataIndex.init()


df.write.format("org.elasticsearch.spark.sql") \
.option("es.resource", "sr-data-index" ) \
.option("es.nodes", "<elastic_ip_address>") \
.option("es.port", "9200") \
.option("es.mapping.id", "Incident_Number") \
.save()
