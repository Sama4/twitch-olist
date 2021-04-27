import os
import pandas as pd 
import sqlalchemy



str_connection = 'sqlite:///{path}'

# enderecos e sub pastas do projeto
BASE_DIR = os.path.dirname( os.path.dirname( os.path.dirname( os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# list comprehension
files_names = [i for i in os.listdir( DATA_DIR) if i.endswith('.csv')]


# abrindo conexao com banco
str_connection = str_connection.format( path = os.path.join(DATA_DIR, 'olist.db')) 
connection = sqlalchemy.create_engine( str_connection )

# Para cada arquivo e realizado uma insercao no banco
for i in files_names:
    df_tmp = pd.read_csv( os.path.join( DATA_DIR, i ) )
    table_name ="tb_" + i.strip(".csv").replace("olist_", "").replace("_dataset", "")
    df_tmp.to_sql( table_name, connection, if_exists='replace', index=False ) 


