import os
from dotenv import load_dotenv # pip install python-dotenv
import pyodbc # pip install pyodbc
import pandas as pd # pip install pandas
import sqlalchemy # pip install sqlalchemy
import urllib

load_dotenv()

SERVER = os.getenv('SERVER') #'<server address>,<port>'
DATABASE = os.getenv('DATABASE') # '<db name>'
USERNAME = os.getenv('DB_USERNAME') 
PASSWORD = os.getenv('DB_PASSWORD')

connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=YES'
connection_url = sqlalchemy.URL.create("mssql+pyodbc", query={"odbc_connect": connectionString})

engine = sqlalchemy.create_engine(connection_url)

COLUNA_COMPARACAO = 1
TABELA_ORIGEM = "Cliente"
TABELA_DESTINO = "Cliente2"

SQL_QUERY = f"""
    declare @sql varchar(max)
    declare @coluna_posicao int = {COLUNA_COMPARACAO}
    
    set @sql = (select col.nome + '' 
        FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY ordinal_position) as Row, column_name as nome
            FROM information_schema.columns
            where table_name = '{TABELA_ORIGEM}'
        ) col WHERE Row = @coluna_posicao for xml path(''))

    exec('SELECT ' + @sql + ' FROM {TABELA_ORIGEM}')
"""

SQL_QUERY2 = f"""
    declare @sql varchar(max)
    declare @coluna_posicao int = {COLUNA_COMPARACAO}
    
    set @sql = (select col.nome + '' 
        FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY ordinal_position) as Row, column_name as nome
            FROM information_schema.columns
            where table_name = '{TABELA_ORIGEM}'
        ) col WHERE Row = @coluna_posicao for xml path(''))

    exec('SELECT ' + @sql + ' FROM {TABELA_DESTINO}')
"""

df1 = pd.read_sql(SQL_QUERY, con=engine)
df2 = pd.read_sql(SQL_QUERY2, con=engine)

df = df1.merge(df2, how='right')

x = df.to_string(header=False,
                  index=False,
                  index_names=False).split('\n')
vals = ', '.join(ele for ele in x)

SQL_QUERY_BUSCAR_DIFERENCIAL = f"""
    declare @sql varchar(max)
    declare @coluna_posicao int = {COLUNA_COMPARACAO}
    
    set @sql = (select col.nome + '' 
        FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY ordinal_position) as Row, column_name as nome
            FROM information_schema.columns
            where table_name = '{TABELA_ORIGEM}'
        ) col WHERE Row = @coluna_posicao for xml path(''))

    exec('SELECT * FROM {TABELA_ORIGEM} WHERE ' + @sql + ' NOT IN ({vals})')
"""

df3 = pd.read_sql(SQL_QUERY_BUSCAR_DIFERENCIAL, con= engine)

with engine.connect() as con:
    con.execute(sqlalchemy.text(f"SET IDENTITY_INSERT {TABELA_ORIGEM} ON;"))

    for index, row in df3.iterrows():
        con.execute(sqlalchemy.text(f"""INSERT INTO {TABELA_DESTINO} ({','.join(row.index)}) VALUES ({','.join(str(ele) if type(ele) is int or type(ele) is float else ('NULL' if ele is None else f"'{str(ele)}'") for ele in row.values)})"""))
        con.commit()

    con.execute(sqlalchemy.text(f"SET IDENTITY_INSERT {TABELA_DESTINO} OFF;"))