#!/usr/bin/env python3
"""
Analyse d'évolution des prix CCAM par menu

Ce script :
1. Charge et joint les tables CCAM (R_ACTE, R_ACTE_IVITE, R_ACTE_IVITE_PHASE, R_PU_BASE, R_MENU)
2. Filtre selon les critères : grilles 3,4,5,7,17,18 / activités 1-4 / phase 0
3. Génère un fichier Excel par menu avec l'évolution des prix de 2005 à 2025
"""

from pathlib import Path
import pandas as pd
from extract_and_convert import convert_dbf_to_df
from datetime import datetime


class CCAMPriceEvolutionAnalyzer:
    """Analyseur d'évolution des prix CCAM"""

    def __init__(self, data_path: Path):
        """
        Initialise l'analyseur

        Args:
            data_path: Chemin vers le dossier contenant les fichiers DBF
        """
        self.data_path = data_path
        self.output_dir = Path(__file__).parent / "output_by_menu"
        self.output_dir.mkdir(exist_ok=True)

        # Critères de filtrage
        self.grilles_filter = [3, 4, 5, 7, 17, 18]
        self.activites_filter = [1, 2, 3, 4]
        self.phase_filter = 0

        # DataFrames
        self.df_acte = None
        self.df_acte_ivite = None
        self.df_acte_ivite_phase = None
        self.df_pu_base = None
        self.df_menu = None
        self.df_activite = None
        self.df_grille = None
        self.df_merged = None

    def load_all_tables(self):
        """Charge toutes les tables nécessaires"""
        print("\n" + "=" * 80)
        print("CHARGEMENT DES TABLES")
        print("=" * 80)

        print("\n📊 Chargement de R_ACTE...")
        self.df_acte = convert_dbf_to_df(self.data_path / "R_ACTE.dbf")
        print(f"   ✓ {len(self.df_acte):,} actes chargés")

        print("\n📊 Chargement de R_ACTE_IVITE...")
        self.df_acte_ivite = convert_dbf_to_df(self.data_path / "R_ACTE_IVITE.dbf")
        print(f"   ✓ {len(self.df_acte_ivite):,} actes-activités chargés")

        print("\n📊 Chargement de R_ACTE_IVITE_PHASE...")
        self.df_acte_ivite_phase = convert_dbf_to_df(self.data_path / "R_ACTE_IVITE_PHASE.dbf")
        print(f"   ✓ {len(self.df_acte_ivite_phase):,} actes-activités-phases chargés")

        print("\n📊 Chargement de R_PU_BASE...")
        self.df_pu_base = convert_dbf_to_df(self.data_path / "R_PU_BASE.dbf")
        print(f"   ✓ {len(self.df_pu_base):,} prix de base chargés")

        print("\n📊 Chargement de R_MENU...")
        self.df_menu = convert_dbf_to_df(self.data_path / "R_MENU.dbf")
        print(f"   ✓ {len(self.df_menu):,} menus chargés")

        print("\n📊 Chargement de R_ACTIVITE...")
        self.df_activite = convert_dbf_to_df(self.data_path / "R_ACTIVITE.dbf")
        print(f"   ✓ {len(self.df_activite):,} activités chargées")

        print("\n📊 Chargement de R_TB23 (grilles)...")
        self.df_grille = convert_dbf_to_df(self.data_path / "R_TB23.dbf")
        print(f"   ✓ {len(self.df_grille):,} grilles chargées")

    def merge_tables(self):
        """Joint toutes les tables"""
        print("\n" + "=" * 80)
        print("JOINTURE DES TABLES")
        print("=" * 80)

        # 1. R_ACTE + R_ACTE_IVITE (acte_cod -> cod_acte)
        print("\n1️⃣  Jointure R_ACTE + R_ACTE_IVITE...")
        df_temp = pd.merge(
            self.df_acte_ivite,
            self.df_acte,
            left_on='acte_cod',
            right_on='cod_acte',
            how='inner'
        )
        print(f"   ✓ {len(df_temp):,} lignes après jointure")

        # 2. + R_ACTE_IVITE_PHASE (cod_aa -> aa_cod)
        print("\n2️⃣  Jointure + R_ACTE_IVITE_PHASE...")
        df_temp = pd.merge(
            df_temp,
            self.df_acte_ivite_phase,
            left_on='cod_aa',
            right_on='aa_cod',
            how='inner'
        )
        print(f"   ✓ {len(df_temp):,} lignes après jointure")

        # 3. + R_PU_BASE (cod_aap -> aap_cod + grille_cod)
        print("\n3️⃣  Jointure + R_PU_BASE...")
        df_temp = pd.merge(
            df_temp,
            self.df_pu_base,
            left_on='cod_aap',
            right_on='aap_cod',
            how='inner'
        )
        print(f"   ✓ {len(df_temp):,} lignes après jointure")

        # 4. + R_MENU (menu_cod -> cod_menu)
        print("\n4️⃣  Jointure + R_MENU...")
        df_temp = pd.merge(
            df_temp,
            self.df_menu[['cod_menu', 'libelle', 'cod_pere']],
            left_on='menu_cod',
            right_on='cod_menu',
            how='left'
        )
        df_temp = df_temp.rename(columns={'libelle': 'menu_libelle'})
        print(f"   ✓ {len(df_temp):,} lignes après jointure")

        # 5. + R_ACTIVITE (activ_cod -> cod_activ)
        print("\n5️⃣  Jointure + R_ACTIVITE...")
        df_temp = pd.merge(
            df_temp,
            self.df_activite,
            left_on='activ_cod',
            right_on='cod_activ',
            how='left'
        )
        df_temp = df_temp.rename(columns={'libelle': 'activite_libelle'})
        print(f"   ✓ {len(df_temp):,} lignes après jointure")

        # 6. + R_TB23 (grille_cod -> cod_grille)
        print("\n6️⃣  Jointure + R_TB23 (grilles)...")
        df_temp = pd.merge(
            df_temp,
            self.df_grille[['cod_grille', 'libelle']],
            left_on='grille_cod',
            right_on='cod_grille',
            how='left'
        )
        df_temp = df_temp.rename(columns={'libelle': 'grille_libelle'})
        print(f"   ✓ {len(df_temp):,} lignes après jointure")

        self.df_merged = df_temp
        print(f"\n✅ Jointure complète : {len(self.df_merged):,} lignes")

    def apply_filters(self):
        """Applique les filtres selon les critères"""
        print("\n" + "=" * 80)
        print("APPLICATION DES FILTRES")
        print("=" * 80)

        print(f"\nAvant filtrage : {len(self.df_merged):,} lignes")

        # Filtre sur les grilles
        print(f"\n🔍 Filtre grilles : {self.grilles_filter}")
        self.df_merged = self.df_merged[self.df_merged['grille_cod'].isin(self.grilles_filter)]
        print(f"   ✓ {len(self.df_merged):,} lignes restantes")

        # Filtre sur les activités
        print(f"\n🔍 Filtre activités : {self.activites_filter}")
        self.df_merged = self.df_merged[self.df_merged['activ_cod'].isin(self.activites_filter)]
        print(f"   ✓ {len(self.df_merged):,} lignes restantes")

        # Filtre sur la phase
        print(f"\n🔍 Filtre phase : {self.phase_filter}")
        self.df_merged = self.df_merged[self.df_merged['phase_cod'] == self.phase_filter]
        print(f"   ✓ {len(self.df_merged):,} lignes restantes")

        print(f"\n✅ Après filtrage : {len(self.df_merged):,} lignes")

    def get_menu_hierarchy_path(self, menu_cod):
        """Récupère le chemin hiérarchique d'un menu"""
        path = []
        current = menu_cod

        while current != 0 and current is not None:
            menu_row = self.df_menu[self.df_menu['cod_menu'] == current]
            if not menu_row.empty:
                libelle = menu_row.iloc[0]['libelle']
                path.insert(0, f"{current}_{libelle}")
                current = menu_row.iloc[0]['cod_pere']
            else:
                break

        return " > ".join(path) if path else "SANS_MENU"

    def group_by_menu(self):
        """Groupe les actes par menu"""
        print("\n" + "=" * 80)
        print("REGROUPEMENT PAR MENU")
        print("=" * 80)

        menus = self.df_merged['menu_cod'].unique()
        print(f"\n📋 {len(menus)} menus uniques trouvés")

        return menus

    def analyze_price_evolution_for_menu(self, menu_cod):
        """Analyse l'évolution des prix pour un menu donné"""
        # Filtrer par menu
        df_menu = self.df_merged[self.df_merged['menu_cod'] == menu_cod].copy()

        if df_menu.empty:
            return None

        # Convertir les dates
        if not pd.api.types.is_datetime64_any_dtype(df_menu['apdt_modif']):
            df_menu['apdt_modif'] = pd.to_datetime(df_menu['apdt_modif'], errors='coerce')

        # Pour chaque acte, on veut l'évolution du prix dans le temps
        results = []

        actes = df_menu['cod_acte'].unique()
        for acte in actes:
            df_acte = df_menu[df_menu['cod_acte'] == acte]

            # Informations de l'acte
            acte_info = df_acte.iloc[0]

            # Pour chaque grille
            for grille in self.grilles_filter:
                df_acte_grille = df_acte[df_acte['grille_cod'] == grille]

                if not df_acte_grille.empty:
                    # Trier par date
                    df_acte_grille = df_acte_grille.sort_values('apdt_modif')

                    # Première et dernière modification
                    first = df_acte_grille.iloc[0]
                    last = df_acte_grille.iloc[-1]

                    result = {
                        'cod_acte': acte,
                        'nom_court': acte_info['nom_court'],
                        'nom_long': acte_info['nom_long'],
                        'activite': acte_info['activ_cod'],
                        'activite_libelle': acte_info['activite_libelle'],
                        'grille_cod': grille,
                        'grille_libelle': acte_info['grille_libelle'] if grille == acte_info['grille_cod'] else self.df_grille[self.df_grille['cod_grille'] == grille].iloc[0]['libelle'],
                        'date_premiere_modif': first['apdt_modif'],
                        'prix_initial': first['pu_base'],
                        'date_derniere_modif': last['apdt_modif'],
                        'prix_actuel': last['pu_base'],
                        'evolution_euros': last['pu_base'] - first['pu_base'],
                        'evolution_pct': ((last['pu_base'] - first['pu_base']) / first['pu_base'] * 100) if first['pu_base'] != 0 else 0,
                        'nb_modifications': len(df_acte_grille)
                    }
                    results.append(result)

        return pd.DataFrame(results)

    def export_menu_to_excel(self, menu_cod, df_analysis):
        """Exporte l'analyse d'un menu en Excel"""
        if df_analysis is None or df_analysis.empty:
            return

        # Récupérer le nom du menu
        menu_info = self.df_menu[self.df_menu['cod_menu'] == menu_cod]
        if menu_info.empty:
            menu_name = f"Menu_{menu_cod}"
        else:
            menu_name = menu_info.iloc[0]['libelle']
            # Nettoyer le nom pour le système de fichiers
            menu_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in menu_name)
            menu_name = menu_name[:100]  # Limiter la longueur

        filename = self.output_dir / f"{menu_cod}_{menu_name}.xlsx"

        # Créer le fichier Excel avec plusieurs feuilles
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Feuille principale : évolution des prix
            df_analysis.to_excel(writer, sheet_name='Evolution_prix', index=False)

            # Feuille 2 : Statistiques par grille
            stats_by_grille = df_analysis.groupby(['grille_cod', 'grille_libelle']).agg({
                'cod_acte': 'count',
                'prix_initial': 'mean',
                'prix_actuel': 'mean',
                'evolution_euros': 'mean',
                'evolution_pct': 'mean'
            }).round(2)
            stats_by_grille.columns = ['Nb_actes', 'Prix_initial_moyen', 'Prix_actuel_moyen', 'Evolution_€_moyenne', 'Evolution_%_moyenne']
            stats_by_grille.to_excel(writer, sheet_name='Stats_par_grille')

            # Feuille 3 : Top évolutions
            top_evolutions = df_analysis.nlargest(20, 'evolution_euros')[['cod_acte', 'nom_court', 'grille_cod', 'prix_initial', 'prix_actuel', 'evolution_euros', 'evolution_pct']]
            top_evolutions.to_excel(writer, sheet_name='Top_evolutions', index=False)

        return filename

    def run_full_analysis(self):
        """Exécute l'analyse complète"""
        print("=" * 80)
        print("ANALYSE D'ÉVOLUTION DES PRIX CCAM PAR MENU")
        print("=" * 80)

        # 1. Chargement
        self.load_all_tables()

        # 2. Jointures
        self.merge_tables()

        # 3. Filtrage
        self.apply_filters()

        # 4. Groupement par menu
        menus = self.group_by_menu()

        # 5. Analyse et export pour chaque menu
        print("\n" + "=" * 80)
        print("GÉNÉRATION DES FICHIERS EXCEL PAR MENU")
        print("=" * 80)

        total_menus = len(menus)
        for i, menu_cod in enumerate(menus, 1):
            print(f"\n[{i}/{total_menus}] Menu {menu_cod}...")

            # Analyse
            df_analysis = self.analyze_price_evolution_for_menu(menu_cod)

            if df_analysis is not None and not df_analysis.empty:
                # Export
                filename = self.export_menu_to_excel(menu_cod, df_analysis)
                print(f"   ✓ {len(df_analysis)} actes analysés → {filename.name}")
            else:
                print(f"   ⚠ Aucune donnée pour ce menu")

        print("\n" + "=" * 80)
        print(f"✅ ANALYSE TERMINÉE - {total_menus} fichiers Excel générés dans : {self.output_dir}")
        print("=" * 80)


def main():
    """Fonction principale"""
    project_root = Path(__file__).parent
    data_path = project_root / "data" / "ccam_v79.10"

    analyzer = CCAMPriceEvolutionAnalyzer(data_path)
    analyzer.run_full_analysis()


if __name__ == "__main__":
    main()
