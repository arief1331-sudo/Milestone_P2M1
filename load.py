'''
=================================================
Milestone 3 
Name  : Arief Bagus Nugraha
Batch : RMT-013

=================================================
'''

import pymongo
import pandas as pd
from pyspark.sql import SparkSession

'''
    Fungsi ini menyimpan Spark DataFrame ke dalam MongoDB Atlas.
    
    Alur: Spark DF -> Pandas DF -> Dictionary -> MongoDB Insert.
    
    Parameters:
    df (DataFrame): Dataframe bersih dari proses Transform.
    connection_string (str): URL koneksi ke MongoDB Atlas.
    db_name (str): Nama database tujuan.
    collection_name (str): Nama collection (tabel) tujuan.
    '''

def load_to_mongo(df, connection_string, db_name, collection_name):
    try:
        # Convert Spark ke Pandas untuk MongoDB
        pandas_df = df.toPandas()
        data_records = pandas_df.to_dict("records")
        
        client = pymongo.MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]
        collection.insert_many(data_records)
        print("SUKSES ke MongoDB!")
        client.close()
    except Exception as e:
        print(f"Gagal: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Load").getOrCreate()
   
    df_clean = spark.read.option("header", "true") \
                         .option("inferSchema", "true") \
                         .csv("/home/jovyan/work/temp_transform")
    
    URL = "mongodb+srv://ariefbn13:Legion123@coda013.ccgltsz.mongodb.net/"
    
    load_to_mongo(df_clean, URL, "db_m3", "table_m3")
    
    spark.stop()
