
import  sqlite3
import pandas as pd
conn  =  sqlite3.connect('mydb.db')
cur= conn.cursor()


df =  pd.read_sql_query('select min as minutes , users  from  UsersPerminute' , conn)
average_users =  df['users'].mean()
max_users =  df['users'].max()
print(df)
print(f'avg Users per minute : {average_users}')
print(f'max Users per minute : {max_users}')


