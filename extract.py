'''
=================================================
Milestone 3 
Name  : Arief Bagus Nugraha
Batch : RMT-013

=================================================
'''

from pyspark.sql import SparkSession
import os

'''
    Fungsi ini bertugas untuk membaca data mentah dari file CSV menjadi Spark DataFrame.
    
    Parameters:
    spark (SparkSession): Object session dari PySpark untuk memproses data.
    file_path (str): Lokasi path file CSV yang ingin dibaca.
    
    Return:
    df (DataFrame): Dataframe PySpark yang berisi data mentah.
    '''

def load_data(spark, file_path):
    print(f"Memulai fungsi load_data untuk: {file_path} ")
    if not os.path.exists(file_path):
        print(f"ERROR: File {file_path} tidak ditemukan di sistem")
        return None
    try:
        df = spark.read.format("csv") \
             .option("header", "true") \
             .option("inferSchema", "true") \
             .option("multiLine", "true") \
             .option("quote", "\"") \
             .option("escape", "\"") \
             .load(file_path)
        print(f"--- Load Berhasil. Jumlah Baris: {df.count()} ---")
        return df
    except Exception as e:
        print(f"!!! Gagal baca CSV: {e} !!!")
        return None

if __name__ == "__main__":
    print("Memulai Script Extract")
    spark = SparkSession.builder.appName("Extract").getOrCreate()
    
    csv_path = "/home/jovyan/work/P2M3_arief_bagus_nugraha_data_raw.csv"
    output_path = "/home/jovyan/work/temp_extract"
    
    df = load_data(spark, csv_path)
    
    if df is not None:
        print(f"Menulis ke {output_path} dalam format CSV")
        
        
        df.write.mode("overwrite").option("header", "true").csv(output_path)
        
        print("Folder temp_extract (CSV) berhasil dibuat")
    else:
        print("DF Kosong, tidak ada yang ditulis")
        
    spark.stop()   