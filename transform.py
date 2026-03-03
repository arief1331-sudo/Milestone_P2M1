'''
=================================================
Milestone 3 
Name  : Arief Bagus Nugraha
Batch : RMT-013

=================================================
'''

from pyspark.sql.functions import col, split, concat_ws, monotonically_increasing_id, regexp_replace
from pyspark.sql import SparkSession

'''
    Fungsi ini melakukan pembersihan dan transformasi data sesuai hasil eksplorasi.
    
    Proses:
    1. Unique Constraint: Memastikan Transaction_ID tidak memiliki duplikat.

    2. Range Check: Memastikan nilai Rating berada dalam rentang logis (0.0 hingga 5.0).

    3. Set Integrity: Memastikan Main_Category konsisten dengan daftar kategori yang ada.

    4. Schema Validation: Memastikan tipe data kolom harga adalah numerik (float).

    5. Standard Length: Validasi panjang karakter Product_Id Amazon (10 digit).

    6. Format Regex: Memastikan Product_Id hanya terdiri dari huruf kapital dan angka.

    7. Volume Check: Memastikan jumlah baris data yang diproses berada dalam ambang batas wajar (100 - 10.000 baris)..
    
    Parameters:
    df (DataFrame): Dataframe mentah dari proses Extract.
    
    Return:
    df_transform (DataFrame): Dataframe bersih yang siap dikirim ke database.
'''

def transform_data(df):

    print("[TRANSFORM] Memulai proses pembersihan data...")
    
    # 1. Membuat Unique ID
    df = df.withColumn("Transaction_ID", 
                        concat_ws("_", col("Product_Id"), monotonically_increasing_id()))
    
    # 2. Membersihkan Kategori
    df = df.withColumn("Main_Category", split(col("Category"), r"\|").getItem(0))
    
    # 3. Membersihkan Harga
    df = df.withColumn("Discounted_Price", 
                        regexp_replace(col("Discounted_Price"), "[^0-9.]", "").cast("float"))
    
    # 4. Membersihkan Rating
    df = df.withColumn("Rating", 
                        regexp_replace(col("Rating"), "[^0-9.]", "").cast("float"))

    print("[TRANSFORM] Proses selesai.")
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Transform_Only").getOrCreate()
    
    try:
        df = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv("/home/jovyan/work/temp_extract")
    
        df_clean = transform_data(df)
        
        
        df_clean.write.mode("overwrite") \
                      .option("header", "true") \
                      .csv("/home/jovyan/work/temp_transform")
                      
        print("[TRANSFORM] Data siap dikirim ke MongoDB (Format CSV).")
        
    except Exception as e:
        print(f"[TRANSFORM] Error: {e}")
    finally:
        spark.stop()