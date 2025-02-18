import json
import time
import pandas as pd
import streamlit as st

from ws_msg import LongWS

# 全局变量声明
db_id = None
cookie = None
sql = None
limit_num = None

st.title('DMS SQL Console')


def parse_items(ws_data):
    columns = ws_data['resultSet']['columns']
    columns_map = dict(map(lambda it: (it['field'], it['realName']), columns))
    count = ws_data['resultSet']['count']

    result = pd.DataFrame(ws_data['resultSet']['result'])
    result.rename(columns_map, axis='columns', inplace=True)
    return result


@st.cache_data
def ws_client(c):
    return LongWS(c)


@st.cache_data
def new_ws_data():
    if db_id is None or db_id == '':
        return
    if cookie is None or cookie == '':
        return
    if sql is None or sql == '':
        return
    lws = ws_client(cookie)
    lws.connect()
    start = time.time()
    resp_data = lws.sql_query(sql, db_id)['data']
    end = time.time()
    print(f"query cost: {(end - start) * 1000} ms")

    print(json.dumps(resp_data, ensure_ascii=False))
    exec_sql = str(resp_data["resultSet"]["orderLink"]["orderLinkData"]["exeSQL"])
    cnt = int(resp_data["resultSet"]["count"])

    if exec_sql.startswith("SELECT") and limit_num <= cnt and False: # 未通过，每日查询次数可能就100次
        max_row = int(resp_data["resultSet"]["maxRow"])
        page = 2
        others = list()
        while limit_num <= cnt:
            start = time.time()
            other_resp_data = lws.sql_query(sql + f" limit {cnt}, {cnt + max_row}", db_id)['data']
            end = time.time()
            print(f"query cost: {(end - start) * 1000} ms, page {page}")
            cnt = cnt + int(resp_data["resultSet"]["count"])
            page = page + 1
            others.append(other_resp_data)
    else:
        others = list()
    return resp_data, others


with st.sidebar:
    db_id = st.text_input("Db:")
    cookie = st.text_input("Cookie:")
    limit_num = st.text_input("Limit:")
    try:
        limit_num = int(limit_num)
    except:
        limit_num = 200
    st.text("希望返回的最大条数")

sql = st.text_input("Console:")
st.markdown("> 输入多条SQL，也只能执行第一条指令")
if st.button("Query"):
    if db_id is None or db_id == "":
        st.warning("Please input Db id!")
    if cookie is None or cookie == "":
        st.warning("Please input cookie!")
    if sql is None or sql == "":
        st.warning("Please input sql!")
    new_ws_data.clear()


def render_data(r_data):
    if len(r_data) < 1:
        return
    if len(r_data) == 1:
        r_data = r_data[0]
        print(json.dumps(r_data, ensure_ascii=False))
        count = r_data["resultSet"]["count"]
        execute_time = r_data["resultSet"]["executeTime"]
        schema_display_name = r_data["resultSet"]["orderLink"]["orderLinkData"]["schemaDisplayName"]
        info = f"一共{count}条数据，执行耗时{execute_time}ms. \n{schema_display_name}"
        st.text(info)

        columns = r_data['resultSet']['columns']
        columns_map = dict(map(lambda it: (it['field'], it['realName']), columns))

        result = pd.DataFrame(r_data['resultSet']['result'])
        result.rename(columns_map, axis='columns', inplace=True)
        st.write(result)
    else:
        schema_display_name = r_data[0]["resultSet"]["orderLink"]["orderLinkData"]["schemaDisplayName"]

        count = sum(list(map(lambda x: x["resultSet"]["count"], r_data)))
        execute_time = sum(list(map(lambda x: x["resultSet"]["executeTime"], r_data)))
        info = f"一共{count}条数据，执行耗时{execute_time}ms. \n{schema_display_name}"

        result = pd.DataFrame([j for i in list(map(lambda x: x['resultSet']['result'], r_data)) for j in i])
        columns = r_data[0]['resultSet']['columns']
        columns_map = dict(map(lambda it: (it['field'], it['realName']), columns))
        result.rename(columns_map, axis='columns', inplace=True)

        st.text(info)
        st.write(result)


data = new_ws_data()
if data is not None and len(data) >= 1:
    st.subheader('Raw data')
    render_data(data)
