import time

import streamlit as st
import pandas as pd

from ws_msg import LongWS

st.title('DMS SQL Console')

global db_id, cookie, sql


def parse_data(ws_data):
    columns = ws_data['resultSet']['columns']
    columns_map = dict(map(lambda it: (it['field'], it['realName']), columns))
    count = ws_data['resultSet']['count']

    result = pd.DataFrame(ws_data['resultSet']['result'])
    result.rename(columns_map, axis='columns', inplace=True)
    # data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
    return result


@st.cache_data
def new_ws_data():
    if db_id is None or db_id == '':
        return
    if cookie is None or cookie == '':
        return
    if sql is None or sql == '':
        return
    lws = LongWS(cookie)
    lws.connect()
    return parse_data(lws.sql_query(sql, db_id)['data'])


with st.sidebar:
    global db_id, cookie
    db_id = st.text_input("Db:")
    cookie = st.text_input("Cookie:")

sql = st.text_input("Console:")
if st.button("Query"):
    if db_id is None or db_id == "":
        st.warning("Please input Db id!")
    if cookie is None or cookie == "":
        st.warning("Please input cookie!")
    if sql is None or sql == "":
        st.warning("Please input sql!")
    new_ws_data.clear()

data = new_ws_data()
if data is not None:
    st.subheader('Raw data')
    st.write(data)
