import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import re
from wordcloud import WordCloud
import config

def generate_report_penjualan_per_kategori(p_source_engine, p_target_engine, show_plot=True):
    """
    Membuat laporan penjualan per kategori, menyimpan hasil ke Data Mart,
    dan membuat visualisasi bar chart.
    """
    print("\n--- 📞 Laporan 1: Penjualan per Kategori ---")
    try:
        query = """
            SELECT dp.kategori, SUM(fp.total_harga) AS total_penjualan
            FROM "Fakta_Penjualan" fp JOIN "Dim_Produk" dp ON fp.id_produk = dp.id_produk
            GROUP BY dp.kategori ORDER BY total_penjualan DESC;
        """
        df_report = pd.read_sql_query(query, p_source_engine)
        
        df_report['tgl_laporan'] = datetime.now()
        df_report.to_sql('rpt_penjualan_per_kategori', p_target_engine, if_exists='append', index=False)
        print("   -> ✅ Hasil analisis berhasil disimpan ke Data Mart.")
        
        plt.figure(figsize=(12, 7))
        barplot = sns.barplot(x='total_penjualan', y='kategori', data=df_report, palette='magma')
        plt.title('Total Penjualan Berdasarkan Kategori Produk', fontsize=16, pad=20)
        plt.xlabel('Total Penjualan (dalam Juta)'); plt.ylabel('Kategori Produk')
        barplot.xaxis.set_major_formatter(lambda x, pos: f'{x/1e6:.0f} Jt')
        plt.tight_layout()
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_baru = f"{timestamp_str}_penjualan_per_kategori.png"
        image_path = os.path.join(config.OUTPUT_PATH, nama_file_baru)
        plt.savefig(image_path, dpi=300, bbox_inches='tight')
        print(f"   -> ✅ Grafik disimpan di: {image_path}")
        if show_plot: plt.show()
        
    except Exception as e:
        print(f"   -> ❌ GAGAL! Error: {e}")

def generate_report_tren_bulanan(p_source_engine, p_target_engine, show_plot=True):
    """
    Membuat laporan tren penjualan bulanan, menyimpan hasil ke Data Mart,
    dan membuat visualisasi line chart.
    """
    print("\n--- 📞 Laporan 2: Tren Penjualan Bulanan ---")
    try:
        query = """
            SELECT TO_DATE(w.tahun || '-' || w.bulan, 'YYYY-MM') AS periode, SUM(fp.total_harga) AS total_penjualan
            FROM "Fakta_Penjualan" fp JOIN "Dim_Waktu" w ON fp.id_waktu = w.id_waktu
            GROUP BY 1 ORDER BY 1;
        """
        df_report = pd.read_sql_query(query, p_source_engine)
        
        df_report['tgl_laporan'] = datetime.now()
        df_report.to_sql('rpt_tren_penjualan_bulanan', p_target_engine, if_exists='append', index=False)
        print("   -> ✅ Hasil analisis berhasil disimpan ke Data Mart.")

        plt.figure(figsize=(15, 7))
        lineplot = sns.lineplot(x='periode', y='total_penjualan', data=df_report, marker='o', color='royalblue')
        plt.title('Tren Total Penjualan per Bulan', fontsize=18, pad=20)
        plt.xlabel('Periode (Bulan)'); plt.ylabel('Total Penjualan (dalam Juta)')
        plt.xticks(rotation=45); lineplot.yaxis.set_major_formatter(lambda x, pos: f'{x/1e6:.1f} Jt')
        plt.tight_layout()
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_baru = f"{timestamp_str}_report_tren_bulanan.png"
        image_path = os.path.join(config.OUTPUT_PATH, nama_file_baru)
        plt.savefig(image_path, dpi=300, bbox_inches='tight')
        print(f"   -> ✅ Grafik disimpan di: {image_path}")
        if show_plot: plt.show()
        
    except Exception as e:
        print(f"   -> ❌ GAGAL! Error: {e}")

def generate_report_top_10_pelanggan(p_source_engine, p_target_engine, show_plot=True):
    """
    Membuat laporan top 10 pelanggan, menyimpan hasil ke Data Mart,
    dan membuat visualisasi horizontal bar chart.
    """
    print("\n--- 📞 Laporan 3: Top 10 Pelanggan ---")
    try:
        query = """
            SELECT p.nama_lengkap, SUM(fp.total_harga) AS total_belanja
            FROM "Fakta_Penjualan" fp
            JOIN "Dim_Pelanggan" p ON fp.pelanggan_key = p.pelanggan_key
            WHERE p.status_sekarang = TRUE
            GROUP BY p.nama_lengkap
            ORDER BY total_belanja DESC LIMIT 10;
        """
        df_report = pd.read_sql_query(query, p_source_engine)
        
        df_report['tgl_laporan'] = datetime.now()
        df_report.to_sql('rpt_top_10_pelanggan', p_target_engine, if_exists='append', index=False)
        print("   -> ✅ Hasil analisis berhasil disimpan ke Data Mart.")

        plt.figure(figsize=(12, 8))
        barplot = sns.barplot(x='total_belanja', y='nama_lengkap', data=df_report, palette='viridis')
        plt.title('Top 10 Pelanggan dengan Total Belanja Terbanyak', fontsize=18, pad=20)
        plt.xlabel('Total Belanja (dalam Ribu)'); plt.ylabel('Nama Pelanggan')
        barplot.xaxis.set_major_formatter(lambda x, pos: f'{(x/1e3):.0f} Rb')
        plt.tight_layout()
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_baru = f"{timestamp_str}_report_top_10_pelanggan.png"
        image_path = os.path.join(config.OUTPUT_PATH, nama_file_baru)
        plt.savefig(image_path, dpi=300, bbox_inches='tight')
        print(f"   -> ✅ Grafik disimpan di: {image_path}")
        if show_plot: plt.show()

    except Exception as e:
        print(f"   -> ❌ GAGAL! Error: {e}")

def generate_report_sentimen_vs_penjualan(p_source_engine, p_target_engine, show_plot=True):
    """
    Membuat laporan gabungan sentimen vs total belanja, menyimpan hasil ke Data Mart,
    dan membuat visualisasi scatter plot.
    """
    print("\n--- 📞 Laporan 4: Korelasi Sentimen vs Penjualan ---")
    try:
        query = """
            WITH
                BelanjaPelanggan AS (
                    SELECT p.id_pelanggan, p.nama_lengkap, SUM(fp.total_harga) AS total_belanja
                    FROM "Fakta_Penjualan" fp
                    JOIN "Dim_Pelanggan" p ON fp.pelanggan_key = p.pelanggan_key
                    WHERE p.status_sekarang = TRUE
                    GROUP BY p.id_pelanggan, p.nama_lengkap
                ),
                SentimenPelanggan AS (
                    SELECT id_pelanggan, polarity, isi_tweet
                    FROM stg_analisis_sentimen
                )
            SELECT bp.nama_lengkap, sp.polarity, bp.total_belanja, sp.isi_tweet
            FROM SentimenPelanggan sp
            JOIN BelanjaPelanggan bp ON sp.id_pelanggan = bp.id_pelanggan;
        """
        df_report = pd.read_sql_query(query, p_source_engine)
        
        df_report['tgl_laporan'] = datetime.now()
        df_report.to_sql('rpt_sentimen_vs_penjualan', p_target_engine, if_exists='append', index=False)
        print("   -> ✅ Hasil analisis gabungan berhasil disimpan ke Data Mart.")

        plt.figure(figsize=(12, 8))
        sns.scatterplot(
            data=df_report,
            x='polarity',
            y='total_belanja',
            size='total_belanja',
            sizes=(50, 1000),
            alpha=0.7,
            hue='polarity',
            palette='coolwarm_r'
        )
        plt.title('Korelasi Sentimen Pelanggan vs Total Belanja', fontsize=18, pad=20)
        plt.xlabel('Polarity Sentimen (Negatif < 0 < Positif)', fontsize=12)
        plt.ylabel('Total Belanja', fontsize=12)
        plt.axvline(0, color='grey', linestyle='--') # Tambah garis netral
        plt.tight_layout()
        
        image_path = os.path.join(config.OUTPUT_PATH, 'sentimen_vs_penjualan.png')
        plt.savefig(image_path, dpi=300, bbox_inches='tight')
        print(f"   -> ✅ Grafik disimpan di: {image_path}")
        if show_plot: plt.show()
        
        return df_report, image_path
    except Exception as e:
        print(f"   -> ❌ GAGAL! Error: {e}")
        return None, None

def generate_report_from_pdf(p_source_engine, p_target_engine):
    """
    Membaca teks mentah dari staging (_st), melakukan ekstraksi informasi
    dengan Regex yang lebih robust, dan menyimpan hasilnya ke Data Mart (_wh).
    """
    print("\n--- 📞 Laporan dari Dokumen PDF (Versi Upgrade) ---")
    try:
        # E: Baca teks mentah dari staging di _st
        print("   -> [E] Membaca teks laporan dari stg_dokumen_pdf...")
        df_staged_pdf = pd.read_sql_table('stg_dokumen_pdf', p_source_engine)
        
        hasil_laporan = []
        # T: Lakukan ekstraksi informasi dengan Regex untuk setiap dokumen
        print("   -> [T] Melakukan ekstraksi informasi dengan Regex yang lebih pintar...")
        for index, row in df_staged_pdf.iterrows():
            text = row['isi_teks']
            filename = row['nama_file']
            
            # --- POLA REGEX YANG DI-UPGRADE ---
            # Pola Periode: \s* mentolerir spasi/baris baru yang banyak
            periode = re.search(r"Kuartal\s*(\d)\s*(\d{4})", text)
            periode_str = f"Q{periode.group(1)} {periode.group(2)}" if periode else None

            # Pola Pendapatan: sama, sudah cukup robust
            pendapatan = re.search(r"\$(\d+\.?\d*)\s*juta", text)
            pendapatan_float = float(pendapatan.group(1)) if pendapatan else None

            # Pola Pertumbuhan: cari saja angka yang diikuti tanda %
            pertumbuhan = re.search(r"(\d+)\s*%", text)
            pertumbuhan_int = int(pertumbuhan.group(1)) if pertumbuhan else None

            # Kumpulkan hasil
            hasil_laporan.append({
                'nama_file': filename,
                'periode_laporan': periode_str,
                'total_pendapatan_juta': pendapatan_float,
                'persentase_pertumbuhan': pertumbuhan_int,
            })

        df_report = pd.DataFrame(hasil_laporan)
        df_report['tgl_laporan'] = datetime.now()

        # L: Load hasil terstruktur ke Data Mart di _wh
        target_table_name = 'rpt_kinerja_kuartalan'
        print(f"   -> [L] Menyimpan hasil ekstraksi ke {target_table_name}...")
        
        # Kita pakai 'replace' agar setiap run hasilnya adalah yang terbaru dari script ini
        df_report.to_sql(target_table_name, p_target_engine, if_exists='append', index=False)
        print(f"   -> ✅ {len(df_report)} laporan kuartalan berhasil disimpan ke Data Mart.")
        
        # Tampilkan hasilnya di notebook untuk verifikasi
        return df_report

    except Exception as e:
        print(f"   -> ❌ GAGAL! Error: {e}")
        return None

def generate_report_word_cloud(p_source_engine, show_plot=True):
    """
    Membuat laporan Word Cloud dari semua teks tweet yang ada di staging,
    dan menyimpan hasilnya sebagai gambar.
    """
    print("\n--- 📞 Laporan 5: Word Cloud dari Teks Tweet ---")
    try:
        # E: Baca semua isi tweet dari tabel staging di _st
        print("   -> [E] Membaca semua teks tweet dari stg_analisis_sentimen...")
        query = 'SELECT isi_tweet FROM stg_analisis_sentimen'
        df_tweets = pd.read_sql_query(query, p_source_engine)

        if df_tweets.empty:
            print("   -> ⚠️ Tidak ada data tweet untuk dibuatkan Word Cloud.")
            return None

        # T: Gabungkan semua teks menjadi satu paragraf raksasa
        print("   -> [T] Menggabungkan dan membersihkan teks...")
        all_text = ' '.join(df_tweets['isi_tweet'])

        # Definisikan 'stop words' (kata-kata umum yang mau kita abaikan)
        # Kita bisa tambahkan kata lain di sini jika perlu
        stopwords = set([
            "di", "dan", "yang", "ini", "itu", "ke", "dari", "buat", "aja", "gak", "ga", "nggak", 
            "nya", "sih", "deh", "kok", "mah", "banget", "udah", "ada", "bisa", "jadi", "sama", 
            "tapi", "juga", "gue", "loh", "satu", "soal", "buat", "yg", "aja", "adventureworks"
        ])

        # Buat objek WordCloud
        wordcloud = WordCloud(
            width=1200, 
            height=600, 
            background_color='white',
            stopwords=stopwords,
            colormap='cividis', # Palet warna lain biar beda
            min_font_size=10,
            collocations=False # Menghindari kata ganda seperti "sepeda gunung" dihitung aneh
        ).generate(all_text)
        print("   -> ✅ Word Cloud berhasil dibuat.")

        # L: Simpan gambar
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_baru = f"{timestamp_str}_word_cloud_tweets.png"
        image_path = os.path.join(config.OUTPUT_PATH, nama_file_baru)
        
        wordcloud.to_file(image_path)
        print(f"   -> ✅ Grafik disimpan di: {image_path}")

        # Visualisasi
        if show_plot:
            plt.figure(figsize=(15, 8))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis("off")
            plt.tight_layout(pad=0)
            plt.show()
        
        return image_path

    except Exception as e:
        print(f"   -> ❌ GAGAL! Error: {e}")
        return None

# --- FUNGSI UTAMA UNTUK MENJALANKAN SEMUA ANALISIS ---
def run_all_analysis(p_source_engine, p_target_engine):
    """Fungsi untuk menjalankan semua analisis dan membuat laporan."""
    print("\n\n===== MEMULAI PIPELINE ANALISIS & REPORTING =====")
    
    generate_report_penjualan_per_kategori(p_source_engine, p_target_engine, show_plot=False)
    generate_report_tren_bulanan(p_source_engine, p_target_engine, show_plot=False)
    generate_report_top_10_pelanggan(p_source_engine, p_target_engine, show_plot=False)
    generate_report_sentimen_vs_penjualan(p_source_engine, p_target_engine, show_plot=False)
    generate_report_from_pdf(p_source_engine, p_target_engine)
    generate_report_word_cloud(p_source_engine, show_plot=False)
    
    print("\n\n===== PIPELINE ANALISIS & REPORTING SELESAI! =====")