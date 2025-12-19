from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import pandas as pd
from datetime import datetime
from functools import wraps
import uuid
import time
import os

# --- Fonction utilitaire pour l'écriture Delta ---

def append_status_to_delta_rust(table_path: str, job_metadata: dict | pd.DataFrame, schema_dtype: dict, mode: str = 'append'):
    """
    Ajoute les métadonnées du job à la table Delta spécifiée par table_path.
    """
    df_new_row = pd.DataFrame([job_metadata])

    # Application du schéma et nettoyage
    try:
        df_new_row = df_new_row.astype(schema_dtype)
    except Exception as e:
        # Ceci est très important: si une clé manque, le DataFrame sera créé avec le mauvais type.
        print(f"Erreur de conversion de type. Vérifiez que toutes les clés du schéma sont dans le dictionnaire de métadonnées: {e}")
        raise 

    df_new_row['error_message'] = df_new_row['error_message'].fillna('')

    # Écriture transactionnelle en mode 'append'
    write_deltalake(
        table_or_uri=table_path, 
        data=df_new_row, 
        mode=mode
    )
    
    print(f"Statut du job '{job_metadata.get('job_name')}' ajouté à la table Delta à {table_path}")

# ----------------------------------------------------------------------
# --- Usine de Décorateurs (Décorateur avec Paramètres) ---
# ----------------------------------------------------------------------

def log_etl_status_factory(delta_path: str, schema_dtype = None, job_id = uuid.uuid4().int % (10**18), job_name = "", engine="pyspark"):
    """
    Ceci est l'usine qui prend le chemin (path) en argument et retourne le décorateur.
    """
    def log_etl_status_decorator(func):
        """
        Ceci est le décorateur qui prend la fonction en argument.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            
            start_time = datetime.now()
            status = 'FAILURE'
            result = None
            error_message = None
            
            # Initialiser les métriques
            metrics = {
                'records_processed': 0, 
                'records_created': 0, 
                'records_updated': 0,
                'records_kept': 0, 
                'records_skipped': 0,
            }
            
            try:
                print(f"Job '{job_name}' (ID: {job_id}) démarré...")
                result = func(*args, **kwargs)
                
                if isinstance(result, dict):
                    metrics.update(result)
                    status = 'SUCCESS'
                else:
                    status = 'PARTIAL'
                    error_message = f"Fonction '{job_name}' a réussi, mais n'a pas retourné de dictionnaire de métriques."

            except Exception as e:
                status = 'FAILURE'
                error_message = str(e)
                print(f"ERREUR lors de l'exécution de '{job_name}': {e}")
                raise 

            finally:
                end_time = datetime.now()
                # Créer le dictionnaire de métadonnées complet, en utilisant les valeurs par défaut si besoin
                job_metadata = {
                    'job_id': job_id, 
                    'job_name': job_name, 
                    'start_time': start_time,
                    'end_time': end_time, 
                    'job_date': start_time.date().strftime("%Y-%m-%d"),
                    'status': status, 
                    'records_processed': metrics.get('records_processed', 0),
                    'records_created': metrics.get('records_created', 0),
                    'records_updated': metrics.get('records_updated', 0),
                    'records_kept': metrics.get('records_kept', 0),
                    'records_skipped': metrics.get('records_skipped', 0),
                    'error_message': error_message,
                }
                if engine == "pyspark":
                    
                    # Utiliser le chemin passé à l'usine de décorateurs (delta_path)
                    from pyspark.sql import SparkSession
                    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

                    # Define the explicit schema for your log table
                    from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

                    schema_dtype = StructType([
                        StructField("job_id", StringType(), True),
                        StructField("job_name", StringType(), True),
                        StructField("start_time", TimestampType(), True),
                        StructField("end_time", TimestampType(), True),
                        StructField("job_date", StringType(), True),
                        StructField("status", StringType(), True),
                        StructField("records_processed", LongType(), True),
                        StructField("records_created", LongType(), True),
                        StructField("records_updated", LongType(), True),
                        StructField("records_kept", LongType(), True),
                        StructField("records_skipped", LongType(), True),
                        StructField("error_message", StringType(), True)
                    ])
                    spark_session = SparkSession.builder.getOrCreate()
                    df_new_row_pyspark = spark_session.createDataFrame(pd.DataFrame([job_metadata]), schema=schema_dtype)
                    df_new_row_pyspark.write.format("delta").mode("append").saveAsTable(delta_path)
                else:
                    if schema_dtype is None:
                        schema_dtype = {
                            'job_id': 'str',
                            'job_name': 'str',
                            'start_time': 'datetime64[ns]',
                            'end_time': 'datetime64[ns]',
                            'job_date': 'str',
                            'status': 'str',
                            'records_processed': 'Int64',
                            'records_created': 'Int64',
                            'records_updated': 'Int64',
                            'records_kept': 'Int64',
                            'records_skipped': 'Int64',
                            'error_message': 'str',
                        }
                    # Utiliser le chemin passé à l'usine de décorateurs (delta_path)
                    append_status_to_delta_rust(delta_path, job_metadata, schema_dtype)
            
            return result

        return wrapper

    return log_etl_status_decorator