# script_combine.py

import os
import requests
from datetime import datetime, timedelta
from tqdm import tqdm

# Définir les constantes pour la date de début et de fin comme variables globales
DATE_DEBUT = datetime(2018, 1, 1)
DATE_FIN = datetime(2023, 6, 30)

def telecharger_et_installer_dernier_mois(date_fin):
    # Lien pour le dernier mois
    dernier_mois = date_fin.replace(day=1) - timedelta(days=1)
    lien_dernier_mois = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dernier_mois.year}-{dernier_mois.month:02d}.parquet"

    # Dossier où le fichier sera enregistré
    dossier_destination_dernier_mois = "dernier_mois/"
    os.makedirs(dossier_destination_dernier_mois, exist_ok=True)

    # Vérifier si le fichier existe avant de télécharger
    fichier_destination = f"{dossier_destination_dernier_mois}yellow_tripdata_{dernier_mois.year}-{dernier_mois.month:02d}.parquet"
    if not os.path.exists(fichier_destination):
        # Télécharger le fichier Parquet depuis le lien
        response = requests.get(lien_dernier_mois)
        if response.status_code == 200:
            # Enregistrer le fichier Parquet dans le dossier de destination
            with open(fichier_destination, 'wb') as fichier:
                fichier.write(response.content)
            print(f"Téléchargement et installation du dernier mois réussis : {fichier_destination}")
        else:
            print(f"Échec du téléchargement du fichier Parquet depuis {lien_dernier_mois}. Code d'état HTTP : {response.status_code}")
    else:
        print(f"Le fichier {fichier_destination} existe déjà. Pas besoin de le télécharger.")

def telecharger_et_installer_tous_les_mois():
    # Modèle du lien avec des espaces réservés pour l'année et le mois
    modele_lien = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{0}-{1:02d}.parquet"

    # Dossier où les fichiers seront enregistrés
    dossier_destination_tous_les_mois = "parquet_files/"
    os.makedirs(dossier_destination_tous_les_mois, exist_ok=True)

    # Calculer le nombre total de mois entre la date de début et la date de fin
    nombre_total_de_mois = (DATE_FIN.year - DATE_DEBUT.year) * 12 + DATE_FIN.month - DATE_DEBUT.month + 1

    # Initialiser la barre de progression avec un style ascii et le nombre total de mois comme étapes
    barre_progression = tqdm(total=nombre_total_de_mois, desc="Downloading", bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{rate_fmt}] {remaining}', mininterval=0.1)

    # Boucle pour itérer sur les années et les mois
    date_actuelle = DATE_DEBUT
    while date_actuelle <= DATE_FIN:
        annee = date_actuelle.year
        mois = date_actuelle.month
        lien = modele_lien.format(annee, mois)

        # Vérifier si le fichier existe avant de télécharger
        fichier_destination = f"{dossier_destination_tous_les_mois}yellow_tripdata_{annee}-{mois:02d}.parquet"
        if not os.path.exists(fichier_destination):
            # Télécharger le fichier Parquet depuis le lien
            response = requests.get(lien)
            if response.status_code == 200:
                # Enregistrer le fichier Parquet dans le dossier de destination
                with open(fichier_destination, 'wb') as fichier:
                    fichier.write(response.content)
                # Mettre à jour la barre de progression
                barre_progression.update(1)
            else:
                print(f"Échec du téléchargement du fichier Parquet depuis {lien}. Code d'état HTTP : {response.status_code}")
        else:
            print(f"Le fichier {fichier_destination} existe déjà. Pas besoin de le télécharger.")

        # Incrémentation de la date d'un mois
        date_actuelle += timedelta(days=31)

    # Fermer la barre de progression
    barre_progression.close()

def main():
    # Appeler la fonction pour télécharger et installer le dernier mois
    telecharger_et_installer_dernier_mois(DATE_FIN)

    # Appeler la fonction pour télécharger et installer tous les mois
    telecharger_et_installer_tous_les_mois()

if __name__ == "__main__":
    main()
