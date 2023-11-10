import urllib.request

def download_data(url, destination):
    """Télécharge un fichier à partir de l'URL spécifiée et le sauvegarde localement.

    Args:
        url (str): L'URL du fichier à télécharger.
        destination (str): Le chemin local où sauvegarder le fichier.
    """
    try:
        urllib.request.urlretrieve(url, destination)
        print(f"Le fichier a été téléchargé avec succès à {destination}")
    except Exception as e:
        print(f"Une erreur s'est produite lors du téléchargement : {e}")

if __name__ == "__main__":
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-08.parquet"
    destination = "./data/data"

    download_data(url, destination)
