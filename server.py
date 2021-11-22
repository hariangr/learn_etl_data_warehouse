from IPython.core.display import JSON
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

base_oltp = "./assets/oltp"

# Kelurahan Id ditulis dengan 9 digit seperti 6171030001, dan kadang dianggap menjadi notasi ilmiah seperti 6.171031001E9
# Ubah notasi ilmiah menjadi string of int, lalu ubah menjadi int
def scientific_notation_9_kelurahan_id_to_int(cell):
    a = cell.replace(".", "").replace("E9", "")
    return int(a)


# ### Dimensi Kategori
dim_kategori = pd.read_excel(f"{base_oltp}/master_kategori.xlsx")
dim_kategori = dim_kategori.set_index('category_id')

# Transformasi int64 0 dan 1 menjadi bool
dim_kategori['category_is_food'] = dim_kategori['category_is_food'] == 1
dim_kategori.info()
dim_kategori


# ### Dimensi Driver
dim_driver = pd.read_excel(f"{base_oltp}/master_driver.xlsx").set_index('user_id')
# dim_driver.head()

dim_driver['user_gender'] = dim_driver['user_gender'] == 'L'

dim_driver = dim_driver.rename(columns={'user_gender': 'is_male'})
dim_driver.info()
dim_driver.head()


# ### Dimensi Kelurahan
dim_kelurahan = pd.read_excel(f"{base_oltp}/master_kelurahan.xlsx").set_index('kelurahan_id')
dim_kelurahan.head()


# ### Dimensi Merchant
def remove_after_dot(s):
    return s.split('.')[0]

dim_merchant = pd.read_excel(f"{base_oltp}/master_merchant.xlsx").astype({'kelurahan_id': 'str'}).set_index('merchant_id')
dim_merchant['kelurahan_id'] = dim_merchant['kelurahan_id'].apply(remove_after_dot)
dim_merchant.info()
dim_merchant.head()
# TODO: Hapus nan


# ### Dimensi User
dim_user = pd.read_excel(f"{base_oltp}/master_user.xlsx").set_index('user_id')
dim_user['user_gender'] = dim_user['user_gender'] == 'L'
dim_user = dim_user.rename(columns={'user_gender': 'is_male'})
dim_user.head()


# ### Fakta Transaksi
import sqlite3

con = sqlite3.connect(f"{base_oltp}/dummy_ojol_transactions_raw_only.sqlite")
fact_transaction = pd.read_sql_query(f"SELECT * FROM dummy_ojol_transactions_raw_only_query_get_transaction_list_koto", con)
fact_transaction = fact_transaction.set_index('id')

# Transform date_process menjadi date_start dan date_end dari str
def split_date_process_start(row):
    s = row['date_process']
    splitted = s.split(" s/d ")
    return splitted[0]
def split_date_process_end(row):
    s = row['date_process']
    splitted = s.split(" s/d ")
    return splitted[1]
date_start = fact_transaction.apply(lambda row: split_date_process_start(row), axis=1)
fact_transaction['date_start'] = pd.to_datetime(date_start)
date_end = fact_transaction.apply(lambda row: split_date_process_end(row), axis=1)
fact_transaction['date_end'] =  pd.to_datetime(date_end)

# Ubah notasi science di kolom kelurahanid menjadi int64
fact_transaction['from_kelurahanid'] = fact_transaction['from_kelurahanid'].apply(scientific_notation_9_kelurahan_id_to_int)
fact_transaction['to_kelurahanid'] = fact_transaction['to_kelurahanid'].apply(scientific_notation_9_kelurahan_id_to_int)

# Split latlng from dan to
def get_lat(cell):
    clean = cell.replace("\t","").replace(" ","") # Atasi "-0.03844709999999999,109.3272303 \t\t\t\t\t\t\..."
    split_by_comma = clean.split(",")
    if len(split_by_comma) == 2:
        return float(split_by_comma[0].strip())
    elif len(split_by_comma) == 1:
        return float(clean[ : clean.find('-', 1)])
    else:
        raise Exception("Format error again")
    
def get_lng(cell):
    # Complicated karena transaction_from_latlng ada data yang rusak
    clean = cell.replace(r"\t","").replace(" ","") # Atasi "-0.03844709999999999,109.3272303 \t\t\t\t\t\t\..."
    clean = cell.split(r" \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t")[0] # Atasi >>-0.03844709999999999,109.3272303 																															-0.03844709999999999<<
    clean = clean.split(' ')[0]
    split_by_comma = clean.split(",")
    return float(split_by_comma[1].strip())
    # try:
    #     if len(split_by_comma) == 2:
    #         return float(split_by_comma[1].strip())
    #     elif len(split_by_comma) == 1:
    #         return float(clean[clean.find('-', 1) : ])
    #     else:
    #         raise Exception("Format error again")
    # except:
    #     print(f">>{repr(cell)}<<")
    

fact_transaction['transaction_from_lat'] = fact_transaction['transaction_from_latlng'].apply(get_lat).astype(float)
fact_transaction['transaction_from_lng'] = fact_transaction['transaction_from_latlng'].apply(get_lng).astype(float)
fact_transaction['transaction_to_lat'] = fact_transaction['transaction_to_latlng'].apply(get_lat).astype(float)
fact_transaction['transaction_to_lng'] = fact_transaction['transaction_to_latlng'].apply(get_lng).astype(float)

# Banyak ada yang formatnya tidak bener misal transaction_from_latlng "-0.03844709999999999,109.3272303 \t\t\t\t\t\t\..."
fact_transaction.loc[fact_transaction['transaction_from_latlng'].str.contains("\t")]

fact_transaction = fact_transaction.drop('date_process', axis=1)
fact_transaction = fact_transaction.drop('transaction_from_latlng', axis=1)
fact_transaction = fact_transaction.drop('transaction_to_latlng', axis=1)
fact_transaction.info()
fact_transaction.head()

sort_quarterly = fact_transaction.groupby(fact_transaction['date_start'].dt.to_period('Q'))
simplify = {}
for key, it in sort_quarterly:
    simplify[str(key)] = it

import os
import pandas as pd 
import numpy as np 
import flask
import pickle
from flask import Flask, render_template, request
from flask import Response
import json

template_dir = os.path.abspath(os.path.dirname(__file__))
template_dir = os.path.join(template_dir, 'frontend')
print(template_dir)
app=Flask(__name__, template_folder=template_dir)

@app.route('/')
def index():
    return flask.render_template('index.html')

@app.route('/keys')
def keys():
    keys = sort_quarterly.keys
    return Response(json.dumps(list(simplify.keys())))

@app.route('/byquarter/<q>')
def byquarter(q):
    from IPython.display import display, HTML
    return Response(simplify[q].to_html())

app.run(debug=True, port=3333)