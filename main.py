import database
import etl_pipeline
import analysis_pipeline

# Fungsi utama untuk menjalankan keseluruhan proses
def main():
    print("🚀 --- Memulai Project Data Lakehouse --- 🚀")
    
    # Dapatkan koneksi engine
    staging_engine = database.get_staging_engine()
    warehouse_engine = database.get_warehouse_engine()
    
    # Jalankan pipeline ETL
    etl_pipeline.run_all_etl(staging_engine)
    
    # Jalankan pipeline Analisis
    analysis_pipeline.run_all_analysis(staging_engine, warehouse_engine)
    
    print("\n✨ --- Semua Proses Telah Selesai dengan Sukses! --- ✨")

# Ini adalah 'pintu masuk' standar untuk script Python
if __name__ == "__main__":
    main()