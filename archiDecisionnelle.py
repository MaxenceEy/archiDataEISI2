import requests
from datetime import datetime, timedelta
from tqdm import tqdm

# Modèle du lien avec des espaces réservés pour l'année et le mois
modele_lien = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{0}-{1:02d}.parquet"

# Dates de début et de fin
date_debut = datetime(2018, 1, 1)
date_fin = datetime(2023, 6, 30)

# Dossier où les fichiers seront enregistrés
dossier_destination = "parquet_files/"

# Assurez-vous que le dossier de destination existe
import os
os.makedirs(dossier_destination, exist_ok=True)

# Calculer le nombre total de mois entre la date de début et la date de fin
nombre_total_de_mois = (date_fin.year - date_debut.year) * 12 + date_fin.month - date_debut.month + 1
print(nombre_total_de_mois)

# Initialiser la barre de progression avec un style ascii et le nombre total de mois comme étapes
barre_progression = tqdm(total=nombre_total_de_mois, desc="Downloading", bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{rate_fmt}] {remaining}', mininterval=0.1)

# Boucle pour itérer sur les années et les mois
date_actuelle = date_debut
while date_actuelle <= date_fin:
    annee = date_actuelle.year
    mois = date_actuelle.month
    lien = modele_lien.format(annee, mois)

    # Télécharger le fichier Parquet depuis le lien
    response = requests.get(lien)
    if response.status_code == 200:
        # Enregistrer le fichier Parquet dans le dossier de destination
        fichier_destination = f"{dossier_destination}yellow_tripdata_{annee}-{mois:02d}.parquet"
        with open(fichier_destination, 'wb') as fichier:
            fichier.write(response.content)
        # Mettre à jour la barre de progression
        barre_progression.update(1)
    else:
        print(f"Échec du téléchargement du fichier Parquet depuis {lien}. Code d'état HTTP : {response.status_code}")

    # Incrémentation de la date d'un mois
    date_actuelle += timedelta(days=31)

# Fermer la barre de progression
barre_progression.close()