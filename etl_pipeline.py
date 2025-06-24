import pandas as pd
from sqlalchemy import text
import os
import shutil
from datetime import date, datetime
import PyPDF2
from textblob import TextBlob
import random
import config

# Sortir file mentah dari landing zone ke folder terorganisir
# Berdasarkan tipe file (CSV, PDF, TXT)
def sortir_file_mentah():
    """Menyalin file dari landing zone ke folder terorganisir berdasarkan tipe."""
    print("\n--- Memulai Proses Sortir File Mentah ---")
    
    # Menggunakan variabel dari config.py, bukan hardcode
    source_path = config.RAW_DATA_PATH 
    organized_base = config.ORGANIZED_PATH
    
    paths = {
        '.csv': os.path.join(organized_base, 'csv'),
        '.pdf': os.path.join(organized_base, 'pdf'),
        '.txt': os.path.join(organized_base, 'txt')
    }

    # Pastikan semua folder tujuan ada
    for path in paths.values():
        os.makedirs(path, exist_ok=True)
        
    # Pindahkan file
    files_to_move = os.listdir(source_path)
    if not files_to_move:
        print(f"-> Folder '{source_path}' kosong, tidak ada file yang disortir.")
        return
        
    for filename in files_to_move:
        file_path = os.path.join(source_path, filename)
        _, ext = os.path.splitext(filename)
        ext = ext.lower()

        if ext in paths:
            shutil.move(file_path, os.path.join(paths[ext], filename))
            print(f"  -> File '{filename}' dipindahkan ke '{paths[ext]}'")
        else:
            print(f"  -> File '{filename}' tidak dikenali tipenya, dilewati.")
            
    print("✅ Proses Sortir File Selesai.")

# Reset semua tabel DWH & Staging sebelum diisi ulang
def reset_all_tables(engine_st):
    """Mengosongkan semua tabel DWH & Staging sebelum diisi ulang."""
    print("\n--- MERESET SEMUA TABEL DI adventureworks_st ---")
    with engine_st.connect() as connection:
        try:
            # Menggunakan CASCADE untuk menangani dependensi foreign key
            connection.execute(text('TRUNCATE TABLE "Fakta_Penjualan", "Dim_Waktu", "Dim_Lokasi", "Dim_Produk", "Dim_Pelanggan", stg_dokumen_pdf, stg_analisis_sentimen RESTART IDENTITY CASCADE;'))
            connection.commit()
            print("✅ Semua tabel di adventureworks_st berhasil dikosongkan.")
        except Exception as e:
            print(f"⚠️ Gagal mereset tabel: {e}")

def etl_dim_produk(engine_st):
    print("\n--- Memulai ETL Dim_Produk ---")
    path_sumber = os.path.join(config.ORGANIZED_PATH, 'csv', 'raw_produk.csv')
    df = pd.read_csv(path_sumber, sep=',', header=None, skiprows=1, names=config.PRODUK_COLS)
    df['warna'].fillna('Tidak Berwarna', inplace=True)
    df.to_sql('Dim_Produk', engine_st, if_exists='append', index=False, chunksize=1000)
    print(f"✅ SUKSES! {len(df)} baris dimuat ke Dim_Produk.")

def etl_dim_waktu(engine_st):
    print("\n--- Memulai ETL Dim_Waktu ---")
    # E: Extract (Ingest data mentah penjualan dari CSV untuk mendapatkan rentang tanggal)
    df = pd.read_csv(os.path.join(config.ORGANIZED_PATH, 'csv', 'raw_penjualan.csv'), sep=',', header=None, skiprows=1, names=config.PENJUALAN_COLS)
    df['tanggal_order'] = pd.to_datetime(df['tanggal_order'])
    
    # T: Transform (Membuat rentang tanggal unik dan atribut waktu)
    date_range = pd.date_range(df['tanggal_order'].min(), df['tanggal_order'].max(), freq='D')
    df_final = pd.DataFrame(date_range, columns=['tanggal'])
    
    df_final['id_waktu'] = df_final['tanggal'].dt.strftime('%Y%m%d').astype(int)
    df_final['tahun'] = df_final['tanggal'].dt.year
    df_final['kuartal'] = df_final['tanggal'].dt.quarter
    df_final['bulan'] = df_final['tanggal'].dt.month
    df_final['nama_bulan'] = df_final['tanggal'].dt.strftime('%B')
    df_final['hari'] = df_final['tanggal'].dt.day
    df_final['nama_hari'] = df_final['tanggal'].dt.strftime('%A')
    
    # L: Load (Muat data ke Dim_Waktu di adventureworks_st)
    df_final.to_sql('Dim_Waktu', engine_st, if_exists='append', index=False, chunksize=1000)
    print(f"✅ SUKSES! {len(df_final)} baris dimuat ke Dim_Waktu.")
    
def etl_dim_pelanggan_scd2(engine_st):
    print("\n--- Memulai ETL SCD Tipe 2 untuk Dim_Pelanggan ---")
    path_sumber = os.path.join(config.ORGANIZED_PATH, 'csv', 'raw_pelanggan.csv')
    source_df = pd.read_csv(path_sumber, sep=',', header=None, skiprows=1, names=config.PELANGGAN_COLS)
    source_df['nama_lengkap'] = source_df['nama_depan'] + ' ' + source_df['nama_belakang']
    try:
        dwh_df = pd.read_sql_query('SELECT * FROM "Dim_Pelanggan" WHERE status_sekarang = TRUE', engine_st)
    except:
        dwh_df = pd.DataFrame(columns=['pelanggan_key', 'id_pelanggan', 'nama_lengkap', 'email', 'kota_asal', 'tanggal_mulai', 'tanggal_akhir', 'status_sekarang'])
        
    merged_df = pd.merge(source_df, dwh_df, on='id_pelanggan', how='left', suffixes=('_new', '_old'), indicator=True)
    new_records = merged_df[merged_df['_merge'] == 'left_only'].copy(); new_to_insert = new_records[['id_pelanggan', 'nama_lengkap_new', 'email_new', 'kota_asal_new']].copy(); new_to_insert.rename(columns={'nama_lengkap_new': 'nama_lengkap', 'email_new': 'email', 'kota_asal_new': 'kota_asal'}, inplace=True)
    new_to_insert['tanggal_mulai'] = '1900-01-01'; new_to_insert['tanggal_akhir'] = '9999-12-31'; new_to_insert['status_sekarang'] = True
    changed_df = merged_df[merged_df['_merge'] == 'both'].copy(); changed_df = changed_df[changed_df['kota_asal_new'] != changed_df['kota_asal_old']]
    keys_to_expire = changed_df['pelanggan_key'].astype(int).tolist() if not changed_df.empty else []
    updates_as_new = changed_df[['id_pelanggan', 'nama_lengkap_new', 'email_new', 'kota_asal_new']].copy(); updates_as_new.rename(columns={'nama_lengkap_new': 'nama_lengkap', 'email_new': 'email', 'kota_asal_new': 'kota_asal'}, inplace=True)
    updates_as_new['tanggal_mulai'] = date.today(); updates_as_new['tanggal_akhir'] = '9999-12-31'; updates_as_new['status_sekarang'] = True
    records_to_insert = pd.concat([new_to_insert, updates_as_new], ignore_index=True)

    with engine_st.connect() as connection:
        if keys_to_expire:
            connection.execute(text(f"""UPDATE "Dim_Pelanggan" SET status_sekarang = FALSE, tanggal_akhir = '{date.today()}' WHERE pelanggan_key IN ({','.join(map(str, keys_to_expire))})""")); connection.commit()
    if not records_to_insert.empty:
        records_to_insert.to_sql('Dim_Pelanggan', engine_st, if_exists='append', index=False, chunksize=1000)
    print(f"✅ SUKSES! Proses SCD Tipe 2 Dim_Pelanggan selesai.")

def etl_dim_lokasi(engine_st):
    print("\n--- Memulai ETL Dim_Lokasi ---")
    # E: Extract (Ingest data mentah pelanggan dari CSV untuk mendapatkan kota_asal)
    df = pd.read_csv(os.path.join(config.ORGANIZED_PATH, 'csv', 'raw_pelanggan.csv'), sep=',', header=None, skiprows=1, names=config.PELANGGAN_COLS)
    
    # T: Transform (Mengambil nilai unik dari kolom 'kota_asal' dan buang nilai NaN)
    df_final = pd.DataFrame(df['kota_asal'].unique(), columns=['nama_kota']).dropna()
    
    # L: Load (Muat data ke Dim_Lokasi di adventureworks_st)
    df_final.to_sql('Dim_Lokasi', engine_st, if_exists='append', index=False, chunksize=1000)
    print(f"✅ SUKSES! {len(df_final)} baris dimuat ke Dim_Lokasi.")

def etl_pdf_to_staging(engine_st):
    print("\n--- Memulai ETL untuk file PDF ---")
    
    # Tentukan path folder PDF
    pdf_folder = os.path.join(config.ORGANIZED_PATH, 'pdf')
    
    # List kosong untuk menampung hasil ekstraksi
    hasil_ekstraksi = []
    
    try:
        # Loop melalui semua file di dalam folder pdf
        for filename in os.listdir(pdf_folder):
            if filename.lower().endswith('.pdf'):
                print(f"  -> Memproses file: {filename}...")
                file_path = os.path.join(pdf_folder, filename)
                
                text = ""
                # Buka dan baca file PDF
                with open(file_path, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    for page in reader.pages:
                        text += page.extract_text() + "\n" # Tambah baris baru antar halaman
                
                # Tambahkan hasil ke list
                hasil_ekstraksi.append({
                    'nama_file': filename,
                    'isi_teks': text
                })
        
        if not hasil_ekstraksi:
            print("⚠️ Tidak ada file PDF yang ditemukan untuk diproses.")
            return

        # Transformasi: Ubah list menjadi DataFrame dan tambahkan timestamp
        df_pdf = pd.DataFrame(hasil_ekstraksi)
        df_pdf['tgl_ekstrak'] = datetime.now()
        
        # Load: Masukkan data ke tabel stg_dokumen_pdf
        target_table_name = 'stg_dokumen_pdf'
        print(f"   -> Memuat {len(df_pdf)} dokumen ke tabel '{target_table_name}'...")
        
        # Kita pakai 'replace' untuk staging, agar setiap run hasilnya fresh
        df_pdf.to_sql(target_table_name, engine_st, if_exists='replace', index=False)

        print(f"✅ SUKSES! ETL untuk PDF selesai.")

    except Exception as e:
        print(f"❌ GAGAL! Terjadi error saat proses ETL PDF: {e}")

def etl_txt_to_staging(engine_st):
    print("\n--- Memulai ETL TXT dengan ID Pelanggan ---")
    
    txt_folder = os.path.join(config.ORGANIZED_PATH, 'txt')
    hasil_analisis = []
    
    try:
        # Langkah Baru 1: Ambil daftar ID pelanggan asli dari DWH
        print("  -> Mengambil daftar ID pelanggan dari Dim_Pelanggan...")
        list_id_pelanggan = pd.read_sql_query('SELECT id_pelanggan FROM "Dim_Pelanggan"', engine_st)['id_pelanggan'].tolist()
        
        if not list_id_pelanggan:
            print("⚠️ GAGAL! Tidak ada data pelanggan di Dim_Pelanggan untuk dijadikan referensi.")
            return

        print(f"  -> Ditemukan {len(list_id_pelanggan)} ID pelanggan.")

        # Loop melalui file TXT
        for filename in os.listdir(txt_folder):
            if filename.lower().endswith('.txt'):
                print(f"  -> Menganalisis file: {filename}...")
                file_path = os.path.join(txt_folder, filename)
                
                with open(file_path, 'r', encoding='utf-8') as file:
                    text = file.read()
                
                blob = TextBlob(text)
                
                # Langkah Baru 2: Pilih ID pelanggan secara acak dan tempelkan
                random_customer_id = random.choice(list_id_pelanggan)
                
                hasil_analisis.append({
                    'id_pelanggan': random_customer_id, # <-- KOLOM BARU
                    'nama_file': filename,
                    'isi_tweet': text,
                    'polarity': blob.sentiment.polarity,
                    'subjectivity': blob.sentiment.subjectivity
                })
        
        # ... (Sisa prosesnya sama)
        df_txt = pd.DataFrame(hasil_analisis)
        df_txt['tgl_ekstrak'] = datetime.now()
        
        target_table_name = 'stg_analisis_sentimen'
        
        # Kita perlu pastikan tabelnya bisa menampung kolom baru
        # Cara paling aman adalah dengan 'replace' karena ini tabel staging
        print(f"   -> Memuat {len(df_txt)} hasil analisis ke tabel '{target_table_name}'...")
        df_txt.to_sql(target_table_name, engine_st, if_exists='replace', index=False)

        print(f"✅ SUKSES! ETL untuk TXT dengan ID pelanggan selesai.")

    except Exception as e:
        print(f"❌ GAGAL! Terjadi error saat proses ETL TXT: {e}")
        
def etl_fakta_penjualan(engine_st):
    print("\n--- Memulai ETL Fakta_Penjualan (Versi Final & Robust) ---")
    
    try:
        # --- EXTRACT ---
        print("   -> [E] Mengekstrak data penjualan mentah...")
        df_penjualan = pd.read_csv(os.path.join(config.ORGANIZED_PATH, 'csv', 'raw_penjualan.csv'), sep=',', header=None, skiprows=1, names=config.PENJUALAN_COLS)
        
        print("   -> [E] Mengekstrak dimensi AKTIF dari DWH...")
        # KUNCI UTAMA: Kita hanya ambil data pelanggan yang statusnya saat ini aktif
        df_dim_pelanggan_aktif = pd.read_sql_query('SELECT pelanggan_key, id_pelanggan, kota_asal FROM "Dim_Pelanggan" WHERE status_sekarang = TRUE', engine_st)
        df_dim_lokasi = pd.read_sql_table('Dim_Lokasi', engine_st)
        
        # --- TRANSFORM ---
        print("   -> [T] Melakukan transformasi dan penggabungan data...")
        
        # 1. Buat id_waktu dari tanggal_order
        df_penjualan['tanggal_order'] = pd.to_datetime(df_penjualan['tanggal_order'])
        df_penjualan['id_waktu'] = df_penjualan['tanggal_order'].dt.strftime('%Y%m%d').astype(int)

        # 2. Gabungkan penjualan dengan pelanggan AKTIF untuk mendapatkan pelanggan_key dan kota_asal
        # Kita pakai INNER JOIN untuk memastikan hanya penjualan dari pelanggan yang terdaftar di DWH yang masuk
        df_merged = pd.merge(df_penjualan, df_dim_pelanggan_aktif, on='id_pelanggan', how='inner')
        
        # 3. Gabungkan dengan Dim_Lokasi untuk mendapatkan id_lokasi
        df_final = pd.merge(df_merged, df_dim_lokasi, left_on='kota_asal', right_on='nama_kota', how='left')

        # 4. Pilih kolom final sesuai struktur tabel Fakta_Penjualan yang baru
        kolom_final = [
            'id_order_detail', 
            'id_order', 
            'pelanggan_key', # <-- Ini kunci surrogate kita
            'id_produk', 
            'id_waktu', 
            'id_lokasi', 
            'jumlah_barang', 
            'harga_satuan', 
            'total_harga'
        ]
        df_final = df_final[kolom_final].copy()
        
        # 5. Jaring pengaman duplikat terakhir
        df_final.drop_duplicates(subset=['id_order_detail'], keep='first', inplace=True)

        # --- LOAD ---
        print(f"   -> [L] Memuat {len(df_final)} baris data ke Fakta_Penjualan...")
        if len(df_final) > 0:
            df_final.to_sql('Fakta_Penjualan', engine_st, if_exists='append', index=False, chunksize=1000)
            print("\n✅ Proses ETL untuk Fakta_Penjualan selesai!")
        else:
            print("\n⚠️ Tidak ada data yang valid untuk dimuat ke Fakta_Penjualan.")
            
    except Exception as e:
        print(f"❌ GAGAL! Terjadi error saat ETL Fakta Penjualan: {e}")

# Kita juga bisa buat satu fungsi utama di sini untuk menjalankan semuanya
def run_all_etl(engine_st):
    """Fungsi untuk menjalankan semua tugas ETL secara berurutan."""
    print("\n\n===== MEMULAI PIPELINE ETL LENGKAP =====")
    sortir_file_mentah() # Sesuaikan fungsi ini agar menggunakan path dari config
    reset_all_tables(engine_st)
    etl_dim_produk(engine_st)
    etl_dim_waktu(engine_st)
    etl_dim_pelanggan_scd2(engine_st) 
    etl_dim_lokasi(engine_st)
    etl_pdf_to_staging(engine_st)
    etl_txt_to_staging(engine_st)
    etl_fakta_penjualan(engine_st)
    print("\n\n===== PIPELINE ETL SELESAI DENGAN SUKSES! =====")