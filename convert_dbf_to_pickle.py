#!/usr/bin/env python3
"""
Script de pré-conversion des fichiers DBF en pickle
pour accélérer les chargements futurs
"""

from pathlib import Path
import pickle
from extract_and_convert import convert_dbf_to_df


def convert_and_save(dbf_path: Path, output_dir: Path):
    """Convertit un DBF en DataFrame et le sauvegarde en pickle"""
    table_name = dbf_path.stem
    output_file = output_dir / f"{table_name}.pkl"

    if output_file.exists():
        print(f"   ⏭  {table_name} déjà converti")
        return

    print(f"   📊 Conversion de {table_name}...", end="", flush=True)

    try:
        df = convert_dbf_to_df(dbf_path)

        # Sauvegarder en pickle
        with open(output_file, 'wb') as f:
            pickle.dump(df, f)

        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f" ✓ ({len(df):,} lignes, {size_mb:.1f} MB)")

    except Exception as e:
        print(f" ❌ Erreur: {e}")


def main():
    """Fonction principale"""
    print("=" * 80)
    print("CONVERSION DES FICHIERS DBF EN PICKLE")
    print("=" * 80)

    # Chemins
    data_path = Path(__file__).parent / "data" / "ccam_v79.10"
    output_dir = Path(__file__).parent / "data" / "pickle"
    output_dir.mkdir(exist_ok=True)

    # Liste des tables nécessaires
    tables_needed = [
        "R_ACTE",
        "R_ACTE_IVITE",
        "R_ACTE_IVITE_PHASE",
        "R_PU_BASE",
        "R_MENU",
        "R_ACTIVITE",
        "R_TB23"
    ]

    print(f"\n📁 Dossier source: {data_path}")
    print(f"📁 Dossier destination: {output_dir}")
    print(f"\n🔄 Tables à convertir: {len(tables_needed)}\n")

    for i, table_name in enumerate(tables_needed, 1):
        dbf_file = data_path / f"{table_name}.dbf"

        if not dbf_file.exists():
            print(f"{i}. ❌ {table_name}.dbf introuvable")
            continue

        print(f"{i}. {table_name}")
        convert_and_save(dbf_file, output_dir)

    print("\n" + "=" * 80)
    print("✅ CONVERSION TERMINÉE")
    print("=" * 80)

    # Afficher la taille totale
    total_size = sum(f.stat().st_size for f in output_dir.glob("*.pkl"))
    print(f"\nTaille totale des fichiers pickle: {total_size / (1024 * 1024):.1f} MB")


if __name__ == "__main__":
    main()
