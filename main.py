'''
=================================================
Milestone 3 
Name  : Arief Bagus Nugraha
Batch : RMT-013

=================================================
'''

from pyspark.sql import SparkSession
from extract import load_data
from transform import transform_data
from load import load_to_mongo

if __name__ == "__main__":
    # --- KONFIGURASI ---
    FILE_PATH = "P2M3_arief_bagus_nugraha_data_raw.csv" 
    
    MONGO_URI = "mongodb+srv://ariefbn13:Legion123@coda013.ccgltsz.mongodb.net/" 
    DB_NAME = "db_m3"
    COLL_NAME = "table_m3"
    
    # Memulai Spark Session
    spark = SparkSession.builder \
        .appName("P2M3_Pipeline_Arief") \
        .getOrCreate()
    
    # Menjalankan EXTRACT
    df_raw = load_data(spark, FILE_PATH)
    
    if df_raw is not None:
        # Menjalankan TRANSFORM
        df_clean = transform_data(df_raw)
        
        # Menjalankan LOAD
        load_to_mongo(df_clean, MONGO_URI, DB_NAME, COLL_NAME)
    
    # Mematikan Spark
    spark.stop()