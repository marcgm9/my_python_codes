import xml.etree.cElementTree as ET
from google.cloud import bigquery, storage
import os
import gzip
import io
import time
import pandas as pd
import numpy as np
import json
from pandasql import sqldf
from datetime import datetime

credentials_path=""
bucket_name=""
entorno = os.environ.get('OS', 'CLOUD')
if entorno == 'Windows_NT':
    #cliente a utilizar de storage y bigquery (cuando se lanza desde linea de comandos en un PC)
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bq_client = bigquery.Client.from_service_account_json(credentials_path)
else:
    #cliente a utilizar de storage y bigquery (cuando se lanza desde google cloud functions)
    storage_client = storage.Client()
    bq_client = bigquery.Client()

destination_bucket=storage_client.get_bucket(bucket_name)

table_id="project.dataset.table"
start_time=time.time()
table_check=bq_client.get_table(table_id)
print(table_check.modified.day)
print(datetime.now().day)
# if table_check.modified.day == datetime.now().day:


table = bigquery.TableReference.from_string(
    table_id
)

rows = bq_client.list_rows(
    table,
)

zz = rows.to_dataframe()
llista_bq=zz["person_id"].to_list()
print("He agafat el màster del PER")
dades="local_path_to_file"
file_name="file_name"
data_xml="day_to_read"
folder_prefix="folder"
llista=list()
for blob in destination_bucket.list_blobs(prefix=folder_prefix+data_xml):
    llista.append(blob.name)
print(llista)

### these lines are to work from cloud:
# per_blob=destination_bucket.blob(per_blob)
# nom=per_blob.name
# data_xml=nom.split("_")[3][0:8]
# dades=io.BytesIO(per_blob.download_as_string())

### these are to take the file from local:
with gzip.open(dades,"rb") as file:
    xml=file.read().decode("utf-8")

tree=ET.fromstring(xml)
person_id_llista=list()
id_tag=""
for child in tree.iter(id_tag):
    person_id_llista.append(child.text)

print("Tenim la llista dels person_id del ",file_name)
dif=list(set(person_id_llista).difference(set(llista_bq)))

tags=list() ## tags we want to take from xml
llista_jsons=list() ## all jsons we'll craft from xml
dict_xml=dict() ## every key will be an ID, and it'll be linked to a dict with the attribs
for event in tree.iter("tag_node"):
    bandera=event.find(id_tag)
    json_test="{"
    if bandera.text in dif:
        dict_xml[bandera.text]=dict()
        for child in tags:
            atribut=event.find(child)
            
            if atribut.text:
                dict_xml[bandera.text][atribut.tag]=atribut.text.replace('"'," ").replace("\t","").replace("\\","")
                json_test=json_test+'"'+child+'":"'+atribut.text.replace('"'," ").replace("\t","").replace("\\","")+'",'
            else:
                json_test=json_test+'"'+child+'":" ",'
        json_test=(json_test+"}\n").replace(",}","}")
        llista_jsons.append(json.loads(json_test))
    
print("PER destriat")

df_insertar=pd.DataFrame(llista_jsons)
df_insertar=df_insertar.replace(" ",np.nan)
df_insertar=df_insertar.dropna(subset=["column_1"])
print(df_insertar.head(5))
job_config=bigquery.LoadJobConfig(
schema=[
    bigquery.SchemaField("", "STRING"),

],
create_disposition = "CREATE_IF_NEEDED",
)
if (df_insertar.shape[0])!=0:
    job = bq_client.load_table_from_dataframe(
    df_insertar, table_id, job_config=job_config
    )
    job.result() 
now=datetime.now()
dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
new_file_path=""
with open(new_file_path,"w",encoding="utf-8") as xml_file:
    xml_file.write('<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<person_file creationDate=\"{}\">\n'.format(dt_string))
    for key in (dict_xml.keys() and df_insertar["person_id"]):
        xml_file.write("\t<person>\n")
        for tag, value in dict_xml[key].items():
            xml_file.write("\t\t<{}>{}</{}>\n".format(tag,value,tag))
        xml_file.write("\t</person>\n")
    xml_file.write("</person_file>")
print("Ja hem insertat els nous ")
## carreguem tot el PER a taula

################################################ fins aquí el que posarem a la cloud function
df=pd.DataFrame(llista_jsons)
#%%
## df és el dataframe del PER i zz és el màster
query="""
select old
from df as d
inner join zz as z
on d.column_1 = z.column_1
where d.column_2 <> z.column_2 or d.column_3 <> z.column_3
"""
df_query=sqldf(query)

print(df_query.head(10))
# df_query=df_query.replace("None","")

# df_query=df_query[~df_query['column_2'].str.contains('None')]


#%%
## Fem update dels person_id que vinguins amb un nom diferent del que tinguèssim al màster
for index, row in df_query.iterrows():
    query_update="""update `{}`
set first_name='{}', last_name='{}'
where person_id='{}' """.format(table_id,row['column_2'], row['column_3'], row['column_4'])
    bq_client.query(query_update)
print("Master PER actualitzat")
## Escrivim l'xml que rebrà JMM
dif=list(set(person_id_llista).difference(set(llista_bq)))
dif=list(set(dif).union(set(df_query["column_1"].to_list())))
tags=["column_1","column_2","column_3","column_4"]

arrel=ET.Element("person_file")
node_file=ET.SubElement(arrel,"file")
ET.SubElement(node_file,"format_version").text="1.0"
ET.SubElement(node_file,"file_type").text="05"
ET.SubElement(node_file,"file_subtype").text="00"
ET.SubElement(arrel,"info_provider").text="ONO"

for event in tree.iter("person"):
    bandera=event.find("person_id")
    json_test="{"
    if bandera.text in dif:
        node=ET.SubElement(arrel,"person")
        for child in tags:
            atribut=event.find(child)
            ET.SubElement(node,child).text=atribut.text 
nou_arbre=ET.ElementTree(arrel)
ET.indent(nou_arbre," ")
nou_arbre.write(new_file_path+"{}_2.0.xml".format(data_xml),encoding="utf-8",xml_declaration=True)
