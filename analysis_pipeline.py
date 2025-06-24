import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
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

# --- FUNGSI UTAMA UNTUK MENJALANKAN SEMUA ANALISIS ---
def run_all_analysis(p_source_engine, p_target_engine):
    """Fungsi untuk menjalankan semua analisis dan membuat laporan."""
    print("\n\n===== MEMULAI PIPELINE ANALISIS & REPORTING =====")
    
    generate_report_penjualan_per_kategori(p_source_engine, p_target_engine, show_plot=False)
    generate_report_tren_bulanan(p_source_engine, p_target_engine, show_plot=False)
    generate_report_top_10_pelanggan(p_source_engine, p_target_engine, show_plot=False)
    
    print("\n\n===== PIPELINE ANALISIS & REPORTING SELESAI! =====")