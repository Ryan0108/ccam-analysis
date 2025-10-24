#!/usr/bin/env python3
"""
Script d'analyse des prix de la table PU_BASE de la CCAM v79.10

Analyse les tarifs de base des actes CCAM selon différentes grilles tarifaires.
"""

from pathlib import Path
import pandas as pd
from extract_and_convert import convert_dbf_to_df


class PUBaseAnalyzer:
    """Analyseur de la table PU_BASE"""

    def __init__(self, data_path: Path):
        """
        Initialise l'analyseur

        Args:
            data_path: Chemin vers le dossier contenant R_PU_BASE.dbf
        """
        self.data_path = data_path
        self.pu_base_path = data_path / "R_PU_BASE.dbf"
        self.df = None
        self.output_dir = Path(__file__).parent / "output"
        self.output_dir.mkdir(exist_ok=True)

    def load_data(self):
        """Charge les données de PU_BASE"""
        print("\n📊 Chargement de la table PU_BASE...")
        if not self.pu_base_path.exists():
            raise FileNotFoundError(f"Fichier {self.pu_base_path} introuvable")

        self.df = convert_dbf_to_df(self.pu_base_path)
        print(f"   ✓ {len(self.df):,} enregistrements chargés")
        print(f"   Colonnes: {', '.join(self.df.columns.tolist())}")

    def print_statistics(self):
        """Affiche les statistiques globales"""
        print("\n" + "=" * 80)
        print("STATISTIQUES GLOBALES")
        print("=" * 80)

        print(f"\nNombre total d'enregistrements: {len(self.df):,}")
        print(f"Nombre d'actes uniques: {self.df['aap_cod'].nunique():,}")
        print(f"Nombre de grilles tarifaires: {self.df['grille_cod'].nunique()}")

        print("\n--- Prix (PU_BASE) ---")
        print(f"Prix moyen: {self.df['pu_base'].mean():.2f} €")
        print(f"Prix médian: {self.df['pu_base'].median():.2f} €")
        print(f"Prix min: {self.df['pu_base'].min():.2f} €")
        print(f"Prix max: {self.df['pu_base'].max():.2f} €")
        print(f"Écart-type: {self.df['pu_base'].std():.2f} €")

        # Distribution des prix
        print("\n--- Distribution des prix ---")
        percentiles = [10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            value = self.df['pu_base'].quantile(p / 100)
            print(f"Percentile {p:2d}%: {value:8.2f} €")

    def analyze_by_grille(self):
        """Analyse les prix par grille tarifaire"""
        print("\n" + "=" * 80)
        print("ANALYSE PAR GRILLE TARIFAIRE")
        print("=" * 80)

        grille_stats = self.df.groupby('grille_cod').agg({
            'pu_base': ['count', 'mean', 'median', 'min', 'max', 'std'],
            'aap_cod': 'nunique'
        }).round(2)

        grille_stats.columns = ['Nb_entrées', 'Prix_moyen', 'Prix_médian',
                                'Prix_min', 'Prix_max', 'Écart_type', 'Nb_actes_uniques']

        print("\n" + grille_stats.to_string())

        # Export en CSV
        output_file = self.output_dir / "analyse_par_grille.csv"
        grille_stats.to_csv(output_file)
        print(f"\n✓ Analyse par grille exportée: {output_file}")

        return grille_stats

    def top_actes_by_price(self, n=20):
        """Identifie les actes les plus chers et les moins chers"""
        print("\n" + "=" * 80)
        print(f"TOP {n} ACTES PAR PRIX")
        print("=" * 80)

        # Top actes les plus chers (pour chaque grille)
        print(f"\n--- {n} Actes les PLUS CHERS (tous grilles confondues) ---")
        top_expensive = self.df.nlargest(n, 'pu_base')[['aap_cod', 'grille_cod', 'pu_base', 'apdt_modif']]
        print(top_expensive.to_string(index=False))

        # Export
        output_file = self.output_dir / f"top_{n}_actes_plus_chers.csv"
        top_expensive.to_csv(output_file, index=False)
        print(f"\n✓ Exporté: {output_file}")

        # Actes avec prix à 0
        print(f"\n--- Actes avec prix à 0€ ---")
        zero_price = self.df[self.df['pu_base'] == 0]
        print(f"Nombre d'enregistrements avec prix = 0€: {len(zero_price):,}")
        print(f"Nombre d'actes uniques avec prix = 0€: {zero_price['aap_cod'].nunique():,}")

        if len(zero_price) > 0:
            print("\nExemples d'actes à 0€:")
            print(zero_price[['aap_cod', 'grille_cod', 'apdt_modif']].head(10).to_string(index=False))

        return top_expensive

    def analyze_by_acte(self):
        """Analyse les prix pour chaque acte à travers toutes les grilles"""
        print("\n" + "=" * 80)
        print("ANALYSE PAR ACTE (variation entre grilles)")
        print("=" * 80)

        # Pour chaque acte, calculer la variation de prix entre grilles
        acte_stats = self.df.groupby('aap_cod').agg({
            'pu_base': ['count', 'mean', 'min', 'max', 'std'],
            'grille_cod': 'nunique'
        }).round(2)

        acte_stats.columns = ['Nb_grilles_présent', 'Prix_moyen', 'Prix_min',
                              'Prix_max', 'Écart_type', 'Nb_grilles_uniques']

        # Calcul de la variation
        acte_stats['Variation_prix'] = acte_stats['Prix_max'] - acte_stats['Prix_min']
        acte_stats['Variation_%'] = ((acte_stats['Prix_max'] - acte_stats['Prix_min']) /
                                      acte_stats['Prix_min'] * 100).replace([float('inf'), -float('inf')], 0)

        print(f"\nNombre total d'actes: {len(acte_stats):,}")

        # Actes avec plus grande variation de prix
        print("\n--- Top 20 actes avec la PLUS GRANDE VARIATION de prix entre grilles ---")
        top_variation = acte_stats.nlargest(20, 'Variation_prix')
        print(top_variation[['Prix_min', 'Prix_max', 'Variation_prix', 'Nb_grilles_uniques']].to_string())

        # Export
        output_file = self.output_dir / "analyse_par_acte.csv"
        acte_stats.to_csv(output_file)
        print(f"\n✓ Analyse par acte exportée: {output_file}")

        output_file_variation = self.output_dir / "top_variations_prix.csv"
        top_variation.to_csv(output_file_variation)
        print(f"✓ Top variations exporté: {output_file_variation}")

        return acte_stats

    def analyze_temporal(self):
        """Analyse l'évolution temporelle des modifications"""
        print("\n" + "=" * 80)
        print("ANALYSE TEMPORELLE")
        print("=" * 80)

        # Convertir en datetime si ce n'est pas déjà fait
        if not pd.api.types.is_datetime64_any_dtype(self.df['apdt_modif']):
            self.df['apdt_modif'] = pd.to_datetime(self.df['apdt_modif'])

        print(f"\nDate de modification la plus ancienne: {self.df['apdt_modif'].min()}")
        print(f"Date de modification la plus récente: {self.df['apdt_modif'].max()}")

        # Modifications par année
        self.df['annee_modif'] = self.df['apdt_modif'].dt.year
        modifs_par_an = self.df.groupby('annee_modif').agg({
            'aap_cod': 'count',
            'pu_base': ['mean', 'median']
        }).round(2)
        modifs_par_an.columns = ['Nb_modifications', 'Prix_moyen', 'Prix_médian']

        print("\n--- Modifications par année ---")
        print(modifs_par_an.to_string())

        # Export
        output_file = self.output_dir / "analyse_temporelle.csv"
        modifs_par_an.to_csv(output_file)
        print(f"\n✓ Analyse temporelle exportée: {output_file}")

        return modifs_par_an

    def export_full_data(self):
        """Exporte toutes les données en CSV"""
        output_file = self.output_dir / "pu_base_complete.csv"
        self.df.to_csv(output_file, index=False)
        print(f"\n✓ Données complètes exportées: {output_file}")

    def run_full_analysis(self):
        """Exécute l'analyse complète"""
        print("=" * 80)
        print("ANALYSE DES PRIX CCAM - TABLE PU_BASE")
        print("=" * 80)

        # Chargement
        self.load_data()

        # Analyses
        self.print_statistics()
        self.analyze_by_grille()
        self.top_actes_by_price(n=20)
        self.analyze_by_acte()
        self.analyze_temporal()
        self.export_full_data()

        print("\n" + "=" * 80)
        print(f"✓ ANALYSE TERMINÉE - Résultats exportés dans: {self.output_dir}")
        print("=" * 80)


def main():
    """Fonction principale"""
    project_root = Path(__file__).parent
    data_path = project_root / "data" / "ccam_v79.10"

    analyzer = PUBaseAnalyzer(data_path)
    analyzer.run_full_analysis()


if __name__ == "__main__":
    main()
