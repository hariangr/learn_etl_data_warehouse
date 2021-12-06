import json
from flask import Response
from flask import Flask, render_template, request
import pickle
import flask
import os
import sqlite3
from IPython.core.display import JSON
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from transformer import get_lat, get_lng, remove_after_dot, scientific_notation_9_kelurahan_id_to_int, split_date_process_end, split_date_process_start

base_oltp = "./assets/oltp"


# ### Dimensi Kategori
dim_kategori = pd.read_excel(
    f"{base_oltp}/master_kategori.xlsx").set_index('category_id')
dim_kategori['category_is_food'] = dim_kategori['category_is_food'] == 1

# ### Dimensi Driver
dim_driver = pd.read_excel(
    f"{base_oltp}/master_driver.xlsx").set_index('user_id')
dim_driver['user_gender'] = dim_driver['user_gender'] == 'L'
dim_driver = dim_driver.rename(columns={'user_gender': 'is_male'})


# ### Dimensi Kelurahan
dim_kelurahan = pd.read_excel(
    f"{base_oltp}/master_kelurahan.xlsx").set_index('kelurahan_id')

dim_merchant = pd.read_excel(f"{base_oltp}/master_merchant.xlsx").astype(
    {'kelurahan_id': 'str'}).set_index('merchant_id')
dim_merchant['kelurahan_id'] = dim_merchant['kelurahan_id'].apply(
    remove_after_dot)
# TODO: Hapus nan

# ### Dimensi User
dim_user = pd.read_excel(f"{base_oltp}/master_user.xlsx").set_index('user_id')
dim_user['user_gender'] = dim_user['user_gender'] == 'L'
dim_user = dim_user.rename(columns={'user_gender': 'is_male'})


con = sqlite3.connect(f"{base_oltp}/dummy_ojol_transactions_raw_only.sqlite")
fact_transaction = pd.read_sql_query(
    f"SELECT * FROM dummy_ojol_transactions_raw_only_query_get_transaction_list_koto", con)
fact_transaction = fact_transaction.set_index('id')


date_start = fact_transaction.apply(
    lambda row: split_date_process_start(row), axis=1)
fact_transaction['date_start'] = pd.to_datetime(date_start)
date_end = fact_transaction.apply(
    lambda row: split_date_process_end(row), axis=1)
fact_transaction['date_end'] = pd.to_datetime(date_end)

# Ubah notasi science di kolom kelurahanid menjadi int64
fact_transaction['from_kelurahanid'] = fact_transaction['from_kelurahanid'].apply(
    scientific_notation_9_kelurahan_id_to_int)
fact_transaction['to_kelurahanid'] = fact_transaction['to_kelurahanid'].apply(
    scientific_notation_9_kelurahan_id_to_int)

# Split latlng from dan to


fact_transaction['transaction_from_lat'] = fact_transaction['transaction_from_latlng'].apply(
    get_lat).astype(float)
fact_transaction['transaction_from_lng'] = fact_transaction['transaction_from_latlng'].apply(
    get_lng).astype(float)
fact_transaction['transaction_to_lat'] = fact_transaction['transaction_to_latlng'].apply(
    get_lat).astype(float)
fact_transaction['transaction_to_lng'] = fact_transaction['transaction_to_latlng'].apply(
    get_lng).astype(float)

# Banyak ada yang formatnya tidak bener misal transaction_from_latlng "-0.03844709999999999,109.3272303 \t\t\t\t\t\t\..."
fact_transaction.loc[fact_transaction['transaction_from_latlng'].str.contains(
    "\t")]

fact_transaction = fact_transaction.drop('date_process', axis=1)
fact_transaction = fact_transaction.drop('transaction_from_latlng', axis=1)
fact_transaction = fact_transaction.drop('transaction_to_latlng', axis=1)

sort_quarterly = fact_transaction.groupby(
    fact_transaction['date_start'].dt.to_period('Q'))
simplify = {}
for key, it in sort_quarterly:
    simplify[str(key)] = it


template_dir = os.path.abspath(os.path.dirname(__file__))
template_dir = os.path.join(template_dir, 'frontend')
app = Flask(__name__, template_folder=template_dir)


@app.route('/')
def index():
    keys = list(simplify.keys())
    return flask.render_template('index.html', keys=keys)


@app.route('/quarterly/<q>')
def byquarter(q):
    import base64
    import matplotlib
    import plotly.express as px
    from io import BytesIO
    from IPython.display import display, HTML
    matplotlib.use('Agg')

    # Amount Transaction Delivery
    plt.figure(figsize=(10, 8))
    plt.title("Amount Transaction Delivery IDR")
    plt.xticks(rotation=90)
    plt.hist(simplify[q]['amount_delivery'].to_list())
    tmpfile = BytesIO()
    plt.savefig(tmpfile, format='png')
    graph_amount_trans = base64.b64encode(tmpfile.getvalue()).decode('utf-8')

    # Amount per mode graph
    plt.figure(figsize=(10, 8))
    plt.title("Amount Per Mode")
    plt.xticks(rotation=90)
    plt.hist(simplify[q]['mode'].to_list())
    tmpfile = BytesIO()
    plt.savefig(tmpfile, format='png')
    graph_per_mode = base64.b64encode(tmpfile.getvalue()).decode('utf-8')

    # From Geo
    fig = px.scatter_geo(simplify[q], lat='transaction_from_lat', lon='transaction_from_lng',
                         hover_name="from_alamat")
    fig.update_layout(title='Sebaran Lokasi Start', title_x=0.5, geo=dict(
        projection_scale=7.5,  # this is kind of like zoom
        # this will center on the point
        center=dict(lat=-2.462587, lon=117.492602),
    ))
    tmpfile = BytesIO()
    fig.write_image(tmpfile, format='png')
    from_geo = base64.b64encode(tmpfile.getvalue()).decode('utf-8')

    # GOAL Geo
    fig = px.scatter_geo(simplify[q], lat='transaction_to_lat', lon='transaction_to_lng',
                         hover_name="from_alamat")
    fig.update_layout(title='Sebaran Lokasi Finish', title_x=0.5, geo=dict(
        projection_scale=7.5,  # this is kind of like zoom
        # this will center on the point
        center=dict(lat=-2.462587, lon=117.492602),
    ))
    tmpfile = BytesIO()
    fig.write_image(tmpfile, format='png')
    to_geo = base64.b64encode(tmpfile.getvalue()).decode('utf-8')

    # Table transaksi
    table_trans = simplify[q].to_html()

    # # Quarter Select
    keys = list(simplify.keys())

    return flask.render_template('quarterly.html', keys=keys, to_geo=to_geo, table_trans=table_trans, from_geo=from_geo, graph_amount_trans=graph_amount_trans, quarter=q, graph_per_mode=graph_per_mode)


app.run(debug=True, port=3333)
