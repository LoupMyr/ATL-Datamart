from minio import Minio
import urllib.request
import sys
import os

def main():
    grab_data()
    write_data_minio()
    return 0

def grab_data() -> None:
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    destination_folder = os.path.abspath("data/raw")

    os.makedirs(destination_folder, exist_ok=True)

    file_name = "yellow_taxis-january_2024"
    file_path = os.path.join(destination_folder, f"{file_name}.parquet")

    try:
        # Vérifier si le fichier existe sur le serveur avant de télécharger
        response = urllib.request.urlopen(url)
        if response.status == 200:
            print(f"Téléchargement de {url}")
            urllib.request.urlretrieve(url, file_path)
            print(f"Fichier téléchargé")
    except Exception as e:
        print(f"Erreur lors du téléchargement de {url} : {e}")


def write_data_minio():
    """
    Cette méthode transfère tous les fichiers Parquet de 'BIGDATA/data/raw' vers Minio.
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    
    # Utilisez un nom de bucket valide
    bucket = "tp1"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket {bucket} créé.")
    else:
        print(f"Bucket {bucket} existe déjà.")

    directory_path = "data/raw"
    for filename in os.listdir(directory_path):
        if filename.endswith(".parquet"):
            file_path = os.path.join(directory_path, filename)
            try:
                client.fput_object(bucket, filename, file_path)
                print(f"{filename} téléchargé dans {bucket}.")
            except Exception as e:
                print(f"Erreur lors du téléchargement de {filename} : {e}")

if __name__ == '__main__':
    sys.exit(main())
