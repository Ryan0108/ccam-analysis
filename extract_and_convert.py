#!/usr/bin/env python3
"""
Script d'extraction et de conversion des fichiers CCAM
Extrait les fichiers ZIP et convertit les DBF en DataFrames pandas
"""

import zipfile
from pathlib import Path
from dbfread import DBF
import pandas as pd
from charset_normalizer import from_bytes


def detect_encoding(dbf_path: Path) -> str:
    """Détecte l'encodage d'un fichier DBF"""
    try:
        with open(dbf_path, 'rb') as f:
            raw_data = f.read(10000)  # Lit les 10 premiers ko
            result = from_bytes(raw_data)
            best_guess = result.best()

            if best_guess:
                encoding = best_guess.encoding
                confidence = round(best_guess.encoding_aliases.get('confidence', 1.0) * 100) if hasattr(best_guess, 'encoding_aliases') else 100
                print(f"🗂  {dbf_path.name} ➜ encodage probable : {encoding} (confiance : {confidence}%)")
                return encoding
            else:
                print(f"🗂  {dbf_path.name} ➜ encodage non détecté, utilisation de cp850 par défaut")
                return 'cp850'
    except Exception as e:
        print(f"❌ Erreur détection encodage pour {dbf_path.name}: {e}")
        return 'cp850'


def convert_dbf_to_df(dbf_path: Path) -> pd.DataFrame:
    """Convertit un fichier DBF en DataFrame pandas"""
    # Détecte l'encodage
    encoding = detect_encoding(dbf_path)

    # Lit le fichier DBF
    dbf = DBF(str(dbf_path), encoding=encoding)

    # Récupère les noms de colonnes
    column_names = [field.name.lower() for field in dbf.fields]

    # Convertit en DataFrame
    df = pd.DataFrame(iter(dbf))

    # Renomme les colonnes en minuscules
    if not df.empty:
        df.columns = map(str.lower, df.columns)
    else:
        df = pd.DataFrame(columns=column_names)

    return df


def extract_zip_files(data_path: Path):
    """Extrait tous les fichiers ZIP du dossier"""
    print("\n📦 Extraction des fichiers ZIP...")
    zip_files = list(data_path.glob("*.zip"))

    if not zip_files:
        print("   Aucun fichier ZIP à extraire")
        return

    for zip_file in zip_files:
        print(f"   Extraction de {zip_file.name}...")
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(data_path)
            print(f"   ✓ {zip_file.name} extrait avec succès")
        except Exception as e:
            print(f"   ❌ Erreur lors de l'extraction de {zip_file.name}: {e}")


def list_dbf_files(data_path: Path):
    """Liste tous les fichiers DBF disponibles"""
    dbf_files = sorted(data_path.glob("*.dbf"))
    print(f"\n📋 {len(dbf_files)} fichiers DBF trouvés:")

    for dbf_file in dbf_files:
        size_mb = dbf_file.stat().st_size / (1024 * 1024)
        print(f"   - {dbf_file.name:<25} ({size_mb:.2f} MB)")

    return dbf_files


def main():
    """Fonction principale"""
    print("=" * 80)
    print("EXTRACTION ET CONVERSION DES FICHIERS CCAM v79.10")
    print("=" * 80)

    # Chemin vers les données
    project_root = Path(__file__).parent
    data_path = project_root / "data" / "ccam_v79.10"

    if not data_path.exists():
        print(f"❌ Erreur: Le dossier {data_path} n'existe pas")
        return

    print(f"\n📁 Dossier de travail: {data_path}")

    # Extraction des ZIP
    extract_zip_files(data_path)

    # Liste des fichiers DBF
    dbf_files = list_dbf_files(data_path)

    print("\n" + "=" * 80)
    print("✓ Extraction et analyse terminées")
    print("=" * 80)

    # Retourne les fichiers DBF pour utilisation ultérieure
    return dbf_files


if __name__ == "__main__":
    dbf_files = main()
