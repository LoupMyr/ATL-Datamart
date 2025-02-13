import gc
import os
import sys
import io
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.begin() as connection:  # Assure une transaction propre
            dataframe.to_sql(
                db_config["dbms_table"],
                con=connection,
                index=False,
                if_exists="append",
                method="multi"  # Optimise les requêtes pour PostgreSQL
            )
            print("conn")
            success: bool = True

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def get_minio_client():
    """
    Crée et retourne un client Minio
    """
    return Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )


def read_parquet_from_minio(client, bucket_name, object_name):
    """
    Lit un fichier parquet depuis Minio et le retourne comme DataFrame
    
    Parameters:
        - client (Minio): Client Minio
        - bucket_name (str): Nom du bucket
        - object_name (str): Nom de l'objet dans le bucket
        
    Returns:
        - pd.DataFrame: DataFrame contenant les données du fichier parquet
    """
    try:
        # Récupère l'objet de Minio
        data = client.get_object(bucket_name, object_name)
        # Lit le fichier parquet directement depuis le flux de données
        df = pd.read_parquet(io.BytesIO(data.read()))
        return df
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier {object_name}: {e}")
        return None


def main() -> None:
    # Initialise le client Minio
    minio_client = get_minio_client()
    bucket_name = "tp1"  # Le bucket utilisé dans le TP1
    
    try:
        # Liste tous les objets dans le bucket
        objects = minio_client.list_objects(bucket_name)
        
        for obj in objects:
            if obj.object_name.lower().endswith('.parquet'):
                print(f"Traitement du fichier: {obj.object_name}")
                
                # Lit le fichier parquet depuis Minio
                parquet_df = read_parquet_from_minio(minio_client, bucket_name, obj.object_name)
                
                if parquet_df is not None:
                    # Nettoie les noms de colonnes
                    clean_column_name(parquet_df)
                    
                    # Écrit dans PostgreSQL
                    if not write_data_postgres(parquet_df):
                        del parquet_df
                        gc.collect()
                        return
                    
                    del parquet_df
                    gc.collect()
                    
    except Exception as e:
        print(f"Erreur lors de l'accès à Minio: {e}")
        return


if __name__ == '__main__':
    sys.exit(main())
