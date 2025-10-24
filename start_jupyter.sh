#!/bin/bash
# Script pour lancer Jupyter Lab avec le bon environnement Python

cd "$(dirname "$0")"

echo "🚀 Lancement de Jupyter Lab..."
echo "📁 Dossier : $(pwd)"
echo ""

# Utiliser le Python de l'environnement virtuel Poetry
/Users/mrouer/Library/Caches/pypoetry/virtualenvs/ccam-analysis-hAISVUzg-py3.12/bin/jupyter lab

# Alternative : utiliser poetry run
# poetry run jupyter lab
