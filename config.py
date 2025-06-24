# --- Konfigurasi Koneksi Database ---
DB_USER = 'postgres'
DB_PASSWORD = 'ardiwinata230803'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME_STAGING = 'adventureworks_st'
DB_NAME_WAREHOUSE = 'adventureworks_wh'

# --- Konfigurasi Path Folder ---
ORGANIZED_PATH = 'data_organized'
RAW_DATA_PATH = 'data_raw'
OUTPUT_PATH = 'output'

# --- Konfigurasi Nama Kolom ---
PELANGGAN_COLS = ['id_pelanggan', 'nama_depan', 'nama_belakang', 'email', 'kota_asal']
PRODUK_COLS = ['id_produk', 'nama_produk', 'subkategori', 'kategori', 'harga_standar', 'warna', 'lini_produk']
PENJUALAN_COLS = ['id_order', 'id_order_detail', 'tanggal_order', 'id_pelanggan', 'id_produk', 'jumlah_barang', 'harga_satuan', 'total_harga']