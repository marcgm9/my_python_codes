from google.cloud import storage, bigquery
import os
import gzip
import time
from io import BytesIO

credentials_path="local_path_to_credentials"
entorno = os.environ.get('OS', 'CLOUD')
if entorno == 'Windows_NT':
    #cliente a utilizar de storage y bigquery (cuando se lanza desde linea de comandos en un PC)
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bq_client = bigquery.Client.from_service_account_json(credentials_path)
else:
    #cliente a utilizar de storage y bigquery (cuando se lanza desde google cloud functions)
    storage_client = storage.Client()
    bq_client = bigquery.Client()

project = 'your_project'
dataset_id = 'dataset_id'

dataset=bq_client.get_dataset(dataset_id)
full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
friendly_name = dataset.friendly_name
print(
    "Got dataset '{}' with friendly_name '{}'.".format(
        full_dataset_id, friendly_name
    )
)

# # View dataset properties.
print("Description: {}".format(dataset.description))
print("Labels:")
labels = dataset.labels
if labels:
    for label, value in labels.items():
        print("\t{}: {}".format(label, value))
else:
    print("\tDataset has no labels defined.")

print("Tables:")
tables = list(bq_client.list_tables(dataset))  # Make an API request(s).
if tables:
    for table_name in tables:
        mida=table_name.size
        print("\t{}".format(table_name.table_id))

        source_bucket=storage_client.get_bucket("test_bucket")
        temp_bucket=storage_client.get_bucket("temp_bucket")
        
        carpeta_origen="original_folder/"

        #%% copiar taula bq a storage (ho divideix en molts fitxers)
        start_time=time.time()
        print("Processarem la taula: {}.{}".format(dataset_id, table_name.table_id))

        destination_uri = "gcs_path_work_bucket/folder/"+table_name.table_id+"_*.csv.gz"
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_name.table_id)

        job_config = bigquery.job.ExtractJobConfig(print_header=False)
        job_config.compression = bigquery.Compression.GZIP

        extract_job = bq_client.extract_table(
            table_ref,
            destination_uri,
            location="EU",
            job_config=job_config,
        )  
        extract_job.result()  

        #document amb la capçalera de la taula (noms columnes) que unirem a la resta d'exportacions per a que no es repeteixin les capçaleres
        headers = """
        select column_name
        from `{}`.{}.INFORMATION_SCHEMA.COLUMNS 
        where table_name='{}'
        """.format(project,dataset_id,table_name.table_id)

        zz = bq_client.query(headers)
        cols = ''
        for result in zz:
            cols = cols + str(result[0]) + ','
        cols = cols.rstrip(',')
        cols = cols + '\n'

        blob = source_bucket.blob(carpeta_origen+'/'+table_name.table_id+'_0.csv.gz')
        blob.upload_from_string(cols)

        contenido_gz = BytesIO()
        with gzip.GzipFile(fileobj=contenido_gz, mode='wb') as f:
            f.write(cols.encode('utf-8'))
        contenido_gz.seek(0)
        blob.upload_from_file(contenido_gz, content_type='application/octet-stream')

        print('exportar taula: ' + str(time.time() - start_time))

        llista_blobs=list()
        mides=list()
        mida=0
        part=0
        for blob in source_bucket.list_blobs(prefix=carpeta_origen):
            
            llista_blobs.append(blob)
            if len(llista_blobs)==32:

                part+=1
                print("He fet {} parts = ".format(part), part*32)
                temp_blob=source_bucket.blob(carpeta_origen+"parts/"+table_name.table_id+"_{}.csv.gz".format(part))
                temp_blob.compose(llista_blobs)
                llista_blobs.clear()

        print(len(llista_blobs))
        part+=1
        temp_blob=source_bucket.blob(carpeta_origen+"parts/"+table_name.table_id+"_{}.csv.gz".format(part))
        temp_blob.compose(llista_blobs)
        llista_blobs.clear()

        llista_parts=list()
        refer=0
        if part>32:
            for blob in source_bucket.list_blobs(prefix=carpeta_origen+"parts/"):
                llista_parts.append(blob)
                if len(llista_parts)==32:
                    refer+=1
                    print("He agrupat en: {} fitxers".format(refer))
                    temp_blob=source_bucket.blob(carpeta_origen+"bons/"+table_name.table_id+"_{}.csv.gz".format(refer))
                    temp_blob.compose(llista_parts)
                    llista_parts.clear() 

        refer+=1
        temp_blob=source_bucket.blob(carpeta_origen+"bons/"+table_name.table_id+"_{}.csv.gz".format(refer))
        temp_blob.compose(llista_parts)
        llista_parts.clear()

        llista_final=list()
        for blob in source_bucket.list_blobs(prefix=carpeta_origen+"bons/"):
            llista_final.append(blob)

        source_blob=source_bucket.blob(table_name.table_id+".csv.gz")
        source_blob.compose(llista_final)


        for blob in source_bucket.list_blobs(prefix=carpeta_origen):
            b=blob.size
            mida=mida+blob.size

        print("Són {} Gb".format(mida//1024**3))
else:
    print("\tThis dataset does not contain any tables.")