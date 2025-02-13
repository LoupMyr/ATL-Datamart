from minio import Minio
import urllib.request
import sys
import os

def main():
    grab_data()
    

def grab_data() -> None:
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    destination_folder = os.path.abspath("data/raw")

    os.makedirs(destination_folder, exist_ok=True)

    file_type = "yellow_tripdata_2024-"
    
    for month in range(1, 9):
        month_str = f"{month:02d}"  
        
        url = f"{base_url}{file_type}{month_str}.parquet"
        file_path = os.path.join(destination_folder, f"{file_type}{month_str}.parquet")
        
        try:
            # Vérifier si le fichier existe sur le serveur avant de télécharger
            response = urllib.request.urlopen(url)
            if response.status == 200:
                print(f"Téléchargement de {url}")
                urllib.request.urlretrieve(url, file_path)
                print(f"Fichier téléchargé")
        except Exception as e:
            print(f"Erreur : {e}")


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
