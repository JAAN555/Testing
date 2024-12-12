import os
import shutil

def filter_files(base_path):
    # Defineerime kaustade asukoha
    stocks_path = os.path.join(base_path, 'stocks')
    etfs_path = os.path.join(base_path, 'etfs')

    # File list, mida soovime säilitada
    file_list = [
        "A", "AA", "AACG", "AAL", "AAN", "AAOI", "AAON", "AAP", "AAU", "B", "DIS",
        "BWA", "SNE", "PGRE", "UVV", "LGF.B", "NFLX", "REG", "SPOT", "ROKU", "AMZN",
        "TME", "IQ", "BILI", "ZNGA", "ATVI", "EA", "NTES", "TTWO", "MAT", "HAS", "FNKO",
        "CZR", "SIX", "ORCL", "HPQ", "DELL", "AAPL", "MSFT", "TSLA", "NVDA", "AMD",
        "LPL", "BAX", "JNJ", "PFE", "NVS", "AZN", "MRK", "MDT", "BSX", "NKE", "PBYI",
        "UAA", "PG", "PLNT", "PTON", "LULU", "FSLR", "WMT", "COST", "HD", "UNH", "CVS",
        "GOOG", "GOOGL", "BAC", "C", "EAD", "GBIL", "CVX", "MPC", "PSX", "PSXP", "CCZ",
        "VZ", "CHTR", "DIS", "ALL", "AIG", "MCD", "SBUX", "DPZ", "F", "GM"
    ]

    # Kustutame 'etfs' kausta
    if os.path.exists(etfs_path):
        shutil.rmtree(etfs_path)

    # Läbime 'stocks' kaustas olevad failid
    for filename in os.listdir(stocks_path):
        # Täielik failitee
        file_path = os.path.join(stocks_path, filename)
        
        # Kui fail ei ole .csv vormingus või ei ole file_list-is, siis kustutame
        if not filename.endswith('.csv') or filename[:-4] not in file_list:
            os.remove(file_path)
    
    # Hoidke "symbols_valid_meta.csv" fail alles, kui see on olemas
    symbols_valid_meta_path = os.path.join(base_path, 'symbols_valid_meta.csv')
    if not os.path.exists(symbols_valid_meta_path):
        print("symbols_valid_meta.csv not found, but it should be kept.")
    else:
        print("Keeping symbols_valid_meta.csv file.")