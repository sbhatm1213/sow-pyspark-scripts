'''
Executing with command :
spark-submit --master yarn --deploy-mode cluster
 --num-executors 8 --executor-cores 4  --driver-memory 12g
  --archives ../pyenv.tar.gz#environment
   --py-files word-embs/python_preprocessing.py,word-embs/generate_embs.py,word-embs/utils.py,word-embs/python_preprocessing.py
   word-embs/preprocess_and_store_parquets.py
'''

import ssl
import datetime
from python_preprocessing import DocPreProcess,MeanEmbeddings
from pyspark import SparkContext, SparkConf, SQLContext
from elasticsearch import Elasticsearch
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
import warnings
import pandas as pd
import os
import spacy
import re

DATA_PATH = 'media/'
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)

ES_CLIENT = es = Elasticsearch(
    ["http://<elastic_ip_address>:9200/"],
    verify_certs=False
)

ssl._create_default_https_context = ssl._create_unverified_context


appName = "APP => preprocess_sr_data_for_word_embs"
master = "yarn"
#master = "local"
conf = SparkConf() \
        .set('spark.sql.session.timeZone', 'UTC') \
        .set('spark.driver.extraJavaOptions', '-Duser.timezone=GMT') \
        .set('spark.executor.extraJavaOptions', '-Duser.timezone=GMT') \
        .setAppName(appName) \
        .setMaster(master)


sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.caseSensitive", "true")
spark.sparkContext.setLogLevel('WARN') # default is INFO log-level, it is too much to open on browser for debugging

warnings.filterwarnings("ignore")


dbserver = '<db_server>'
dbname = '<db_name>'
dbuser = '<db_username>'
dbpassword = '<db_password>'


nlp = spacy.load('en_core_web_sm')
stop_words = spacy.lang.en.stop_words.STOP_WORDS
nlp.vocab["by"].is_stop = True
nlp.vocab["not"].is_stop = False
nlp.vocab["hi"].is_stop = True


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
         where 
                b.org_mnem_nm not in ('CERN_KCM') and a.assgn_grp not like '%[_]DOD[_]%' and a.assgn_grp not like 
                '%[_]DOD' and a.assgn_grp not like '%[_]VA[_]%' and a.assgn_grp not like '%[_]VA' and a.assgn_grp not like 
                '%[_]HR[_]%' and a.assgn_grp not like '%[_]HR' and ORG_MNEM_NM not in ('CERN_KCM') and a.assgn_grp not in 
                ('DS_CHLD_DC','DS_ALBR_PA','DS_CHES_NY','DS_LOWE_MA','DS_BH_AL','DS_WIN_MN', 'DS_UNIV_MO','DS_TRUM_MO', 
                'DS_LCOX_MO','DS_YAVA_AZ','DS_NHS_DE','DS_MCH_TX','DS_PALO_CA','DS_SGEN_OH','DS_FTHC_WI','DS_WMMC_MO', 
                'DS_RUTL_VT','DS_GLEN_NY','DS_SANJ_NM','DS_BLTM_WI','DS_STIL_MO','DS_EJEF_LA','Infra_Systems_ALBR_PA', 
                'Infra_Systems_STIL_MO','Infra_Sys_Admin_NWH_WA','Help_Desk_CC','Help_Desk_Accounts_CC')
         AND 
         CONCAT(MONTH(a.RPRT_DTTM),'-',YEAR(a.RPRT_DTTM)) = '{month_bucket}'
    """


date1 = "2015-12-01"  # input start date
date2 = datetime.datetime.now().strftime("%Y-%m-%d")  # input end date

done_list = [{'key': i.strftime("%m-%Y")} for i in pd.date_range(start=date1, end=date2, freq='M')]


def normalize_text(text):
    """    :param text: string    :return:    clean string    """
    norm_text = text.lower()
    # Remove html tags from text # bs4 not required actually
    # try:
    #     norm_text = BeautifulSoup(norm_text,'html.parser').get_text()
    # except Exception as exc:
    #     print("BEAUTIFUL-SOUP PARSING has a problem. ignore it.")
    #     pass
    # Replace domain specific keywords
    norm_text = re.sub(r'\bencounter id\b','',norm_text)
    norm_text = re.sub(r'\blong text id\b', '', norm_text)
    norm_text = re.sub(r'\barea affected\b', '', norm_text)
    norm_text = re.sub(r'\baffected applications\b','',norm_text)
    # Collapse multiple spaces
    norm_text = re.sub(r'\s+', ' ', norm_text)
    # Remove email address
    norm_text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', norm_text)
    # Remove file paths if any
    norm_text = re.sub(r"([A-Za-z]:)?(\\[\w-]+)+\\?([\w-]+(\.\w+)*)?",'',norm_text)
    # Remove numbers from text
    norm_text = re.sub(r'\d+', ' ', norm_text)
    # Remove links from text
    # #norm_text = re.sub(r'http\S+', ' ', norm_text, flags=re.MULTILINE)
    norm_text = norm_text.strip()
    # # Pad punctuation with spaces on both sides
    # #norm_text = re.sub(r"([\.\",\(\)!\?;:\*])",r" \1 ", norm_text)
    return norm_text


udf_to_normalize_text_data = udf(normalize_text, StringType())


ids_docs_cols = [f'all_ids', f'all_docs']
ids_docs_schema = StructType([
    StructField("all_ids",ArrayType(StringType()),True),
    StructField("all_docs",ArrayType(StringType()),True)
])


def docpreprocess(doctextlist,docidlist):
    processed_docs = DocPreProcess(nlp, stop_words, doctextlist, docidlist, lemmatization=True,
                                   build_bi=False, build_tri=False, allowed_postags=None)
    return processed_docs.doc_words


docs_resp_schema = StructType([
    StructField('response', ArrayType(ArrayType(StringType())), True)
])
udf_for_docpreprocess = udf(lambda x,y: docpreprocess(x,y), ArrayType(StringType()))


for month_bucket in done_list:

    print(f" <<<<<<<<<< AT MONTH-BUCKET : {month_bucket} >>>>>>>")

    monthDF = spark.read.format('jdbc') \
        .option('url', f'jdbc:sqlserver://{dbserver};databaseName={dbname};encrypt=true;trustServerCertificate=true;') \
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
        .option('query', sr_data_query.format(month_bucket=month_bucket['key'])) \
        .option('user', dbuser) \
        .option('password', dbpassword) \
        .load()

    monthDF.persist()
    monthDF = monthDF.repartition(120)

    monthDF = monthDF.withColumn("Row_ID", monthDF["Row_ID"].cast(StringType()))
    monthDF = monthDF.selectExpr("Row_ID", "SR_NUM as Incident_Number", "SR_sum_txt as Summary", "DESC_TXT as Description",
                       "LAST_MODIFIED_DATE as last_modified_date", "Status", "RSLTN_TXT as Resolution_Txt",
                       "SOLTN_FMLY as solution_family", "Solution as product_name", "OPEN_DTTM as Open_Date",
                       "CLSE_DTTM as Close_Date", "Team as Assigned_Group", "ORG_MNEM_NM as Client_Mnemonic",
                       "unv_tckt_num as Sr_Num", "Assignee", "owner", "data_source",
                       "LST_RSLVD_DTTM as last_resolved_date", "is_federal")

    monthDF.persist()
    monthDF = monthDF.withColumn("is_federal", monthDF["is_federal"].cast(BooleanType()))
    monthDF = monthDF.select("Incident_Number","Description","Summary","is_federal")
    monthDF = monthDF.filter(monthDF.is_federal == False)
    monthDF.persist()
    monthDF = monthDF.repartition(120)
    print(monthDF.printSchema())
    print(monthDF.show(10))
    monthDF = monthDF.select("*", concat_ws('.', monthDF["Summary"], monthDF["Description"]).alias('Text_Data'))

    monthDF = monthDF.withColumn("Text_Data", trim(monthDF.Text_Data))
    udf_to_normalize_text_data = udf(normalize_text, StringType())
    newDF = monthDF.withColumn('result', udf_to_normalize_text_data(col('Text_Data')))
    newDF.persist()
    newDF = newDF.repartition(120)
    newDF = newDF.withColumn("result", when(col("result")=='',None).otherwise(col("result")))
    newDF = newDF.filter(newDF.result.isNotNull())

    onelakh = 10000
    if monthDF.count() > onelakh:
        uptolimit = int(monthDF.count() / onelakh) + 1
    else:
        uptolimit = 2

    newDF = newDF.withColumn("new_column",lit("ABC")) # dummy new-column created just to partitionby
    w = Window().partitionBy('new_column').orderBy(lit('A'))
    newDF = newDF.withColumn("row_num", row_number().over(w)).drop("new_column")
    print(newDF.show())
    # print(f'monthDF shape at 295 for {month_bucket} is: {newDF.count()}, {len(newDF.columns)}')

    newDF.createOrReplaceTempView('full_cr_data_df')
    mainDF = spark.createDataFrame(data=[], schema=ids_docs_schema)
    mainDF.persist()
    for i in range(0, uptolimit*onelakh, onelakh):
        # big_df_split = sqlContext.sql('''
        #     select * from full_cr_data_df
        #     where row_num > %d and row_num <= %d
        #     order by row_num
        #     limit %d
        # '''%(i, i+onelakh, onelakh))
        big_df_split_query = '''
            select * from full_cr_data_df 
            where row_num > %d and row_num <= %d
        '''%(i, i+onelakh)
        big_df_split = sqlContext.sql(big_df_split_query)
        print(f'query to get big_df_split =>>> {big_df_split_query}')
        print(f'big_df_split shape elasticsearch is: {big_df_split.count()}, {len(big_df_split.columns)}')
        if big_df_split.count() != 0:
            ids_docs_tuple = (big_df_split.select("Incident_Number").rdd.flatMap(lambda x: x).collect(),
                              big_df_split.select("result").rdd.flatMap(lambda x: x).collect())

            ids_docs_cols = [f'all_ids', f'all_docs']
            df_to_append = spark.createDataFrame([ids_docs_tuple]).toDF(*ids_docs_cols)

            mainDF = mainDF.unionByName(df_to_append)
    mainDF = mainDF.withColumn('preprocess_docs', udf_for_docpreprocess(col('all_docs'), col('all_ids')))

    if mainDF.count() > 0:
        if not month_bucket['key']:
            mainDF.repartition(mainDF.count()).write.mode("overwrite").parquet(
                f"hdfs:///user/root/media/preprocessed/sr-data-split/0-0000")
        else:
            mainDF.repartition(mainDF.count()).write.mode("overwrite").parquet(
                f"hdfs:///user/root/media/preprocessed/sr-data-split/{month_bucket['key']}")
    mainDF.unpersist()
    monthDF.unpersist()

