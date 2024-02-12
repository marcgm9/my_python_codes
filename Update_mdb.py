## Per a automatitzar el canvi de cadenes, cal actualitzar la mdb
## L'idea és establir connexió amb la base de dades i anar fent les querys per a 
## actualitzar-ne el contingut.
## També fem un backup de la mdb abans de fer el canvi.

import pyodbc
import sys
from datetime import datetime
import shutil

## Agafem els paràmetres 
#ajustem els noms per a fer-ho més fàcil de manipular
bandera=int(sys.argv[1]) ## la bandera és per a saber si afegim una cadena o n'esborrarem una
codi=int(sys.argv[2])
nom=sys.argv[3]
day=sys.argv[4]
#canviem la data del canvi al format correcte
datetimeobject=datetime.strptime(sys.argv[4],'%Y%m%d')
dataformat=datetimeobject.strftime('%d/%m/%Y')

org="path_to_original_mdb" ## Agafem la mdb
copia="path_to_backup_%s" % day ## Fem el backup
shutil.copy2(org,copia)

## Omplim les llistes amb els valors que afegirem, una per a cada taula
Ch=[codi,nom,nom,0,0,dataformat,dataformat]
BC=[codi,nom,nom,0]

NC=[codi]
BCBS=[codi]
LC=[codi,codi]

#La variable que farem servir per a veure si és una app o no, retorna -1 si no
#hi troba app, si troba que es una app, retorna quan comença la cadena "app"
#per tant, retorna un valor positiu

app=nom.find("App")

#set up some constants
mdb = "path_to_mdb" ## Direcció del fitxer
drv = "mdb_driver" ##El driver
pwd = "mdb_password" ## La clau de pas

# # connect to db
con = pyodbc.connect('DRIVER;DBQ;PWD') ##Establim l'objecte de connexió
cur = con.cursor() ## Definim el cursor

#### Si aquest tros dóna problemes, es pot esborrar
## Comprovem si agafem un codi que ja estigui ocupat
check='Select CodChannel from table_1'
codis=cur.execute(check).fetchall() ## Executem la query i guardem el resultat

llistacodis=[t[0] for t in codis] ## Agafem tots els codchannel
tic=codi in llistacodis

if bandera==1 and tic==True:
    print("Intentes afegir una cadena amb un codi que ja existeix")
    quit()

if bandera==0 and tic==False:
    print("Intentes actualitzar una cadena que no existeix, l'has d'insertar abans")
    quit()
########


#agafem la dateCreated i la posicio del Channels abans de res
if app ==-1:

    if bandera==0:

        test="Select * from table_2 where CodChannelChild=?"
        nom_org=cur.execute(test,codi).fetchall()
        toc=len(nom_org)

        if toc==1:

            SQLTTV="Select MAX(Pos) from table_2 where CodChannel=10020"
            fila=cur.execute(SQLTTV).fetchall()
            d=[l[0] for l in fila]
            posApp=d[0]+1

            fila_nova=("Insert into table_2 Values ()")
            cur.execute(fila_nova,codi)


#la bandera indica si modifiquem o afegim una cadena
#queries per a modificar cadena
if bandera==0:
    ######### Agafem les dades que necessitem abans d'esborrar
    ## Primer la Channels
    SQLdata="Select DateCreated from table_1 where CodChannel=?" 
    fila=cur.execute(SQLdata,codi).fetchall() 
    a=[j[0] for j in fila]
    data=a[0]

    SQLposCh="Select Pos from table_1 where CodChannel=?"
    fila=cur.execute(SQLposCh, codi).fetchall()
    b=[k[0] for k in fila]
    posCh=b[0]


    #esborrem les linies anteriors
    BCesb= "Delete * from table_1 where column_1=?" 
    cur.execute(BCesb,codi)
    
    #hi introduim les noves
    Ch=[codi,nom,nom,posCh,posBC+2000,data,dataformat]

    SQL = "Insert into table_1 Values ()"
    cur.execute(SQL, Ch)

    BC=[codi,nom,nom,posBC]

    SQL1= "Insert into table_2 Values ()"
    cur.execute(SQL1,BC)

    for i in range (1,19):
        SQL2 = ("Insert into table_5 Values ()")
        cur.execute(SQL2,BCBS)

    SQL3 = "Insert into table_4 Values ()" 
    cur.execute(SQL3,LC)

    if app != -1:
        SQL4 = ("Insert into table_3 Values ()")
        cur.execute(SQL4,NC)
    elif app == -1:
         SQL4 = ("Insert into table_3 Values ()")
         cur.execute(SQL4,NC)
         SQL5= ("Insert into table_3 Values ()")
         cur.execute(SQL5,NC)

# #queries per a afegir cadena

    
#insereix una cadena nova
elif bandera==1:

#### Agafem les dades de posició
##Channels
    SQLposMCh="Select MAX(Pos) from table_1 where Pos not like '999%'"
    fila=cur.execute(SQLposMCh).fetchall()
    Mb=[k[0] for k in fila]
    posMCh=Mb[0]+1
##Base Channels
    SQLposMBC="Select MAX(Pos) from table_2"
    fila=cur.execute(SQLposMBC).fetchall()
    Me=[k[0] for k in fila]
    posMBC=Me[0]+1

##L'estimada table_3
    SQLnoApp="Select MAX(Pos) from table_3 where CodChannel=10020"
    fila=cur.execute(SQLnoApp).fetchall()
    a=[j[0] for j in fila]
    posi=a[0]+1

    SQLTTV="Select MAX(Pos) from table_3 where CodChannel=10000"
    fila=cur.execute(SQLTTV).fetchall()
    d=[l[0] for l in fila]
    posnoApp=d[0]+1

    Ch=[codi,nom,nom,posMCh,posMBC+2000,dataformat, dataformat]

    SQL = "Insert into table_1 Values ()" # your query goes here 
    cur.execute(SQL,Ch)

    BC=[codi,nom,nom,posMBC]
    SQL1= "Insert into table_2 Values ()" ##Escrivim la query
    cur.execute(SQL1,BC) ##L'executem

    for i in range (1,19):
        SQL2 = ("Insert into table_5 Values ()")
        cur.execute(SQL2,BCBS)

    SQL3 = "Insert into table_4 Values ()"  
    cur.execute(SQL3,LC)

    if app != -1:
        SQL4 = ("Insert into table_3 Values ()")
        cur.execute(SQL4,NC)
    elif app == -1:
        NC=[codi,posnoApp]
        SQL4 = "Insert into table_3 Values ()" 
        cur.execute(SQL4,NC)
        NC=[codi,posi]
        SQL5= "Insert into table_3 Values ()" 
        cur.execute(SQL5,NC)

#tanquem recursos
cur.close()
con.commit()
con.close()
