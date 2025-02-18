import re
import time
import pandas as pd
import streamlit as st

from ws_msg import LongWS

# 全局变量声明
db_id = None
cookie = None
sql = None
limit_num = None

if 'history_query' not in st.session_state:
    st.session_state.history_query = []
history_query = st.session_state.history_query

st.title('DMS SQL Console')


def parse_items(ws_data):
    columns = ws_data['resultSet']['columns']
    columns_map = dict(map(lambda it: (it['field'], it['realName']), columns))
    count = ws_data['resultSet']['count']

    result = pd.DataFrame(ws_data['resultSet']['result'])
    result.rename(columns_map, axis='columns', inplace=True)
    return result

class SQL_WARP():
    def __init__(self, sql, limit_num):
        self.sql = sql
        self.page = 1
        self.limit_num = limit_num
        self.pageable = sql.lower().startswith("select") and (limit_num > 0 or sql.lower().find("limit") != -1)
        if self.sql.lower().find("limit") != -1:
            # parse sql by regex
            limit_info = re.search(r"limit\s+(\d+),\s+(\d+)", self.sql)
            limit_info_2 = re.search(r"limit\s+(\d+)", self.sql)
            if limit_info:
                self.offset = int(limit_info.group(1))
                self.limit_num = int(limit_info.group(2))
                self.sql = self.sql.replace(limit_info.group(0), "")
            elif limit_info_2:
                self.offset = 0
                self.limit_num = int(limit_info_2.group(1))
                self.sql = self.sql.replace(limit_info_2.group(0), "")
            else:
                print(f"parse sql limit error, {self.sql}")
                self.offset = 0
        else:
            self.offset = 0
        self.cnt = 0
    
    def offset_inc(self, inc):
        self.offset += inc
        self.cnt += inc
        self.page += 1
    
    def has_more(self):
        if self.pageable:
            if self.page > 10 and self.cnt < self.limit_num:
                print(f"fuse 10次查询, {self.sql}")
                return False
            return self.cnt < self.limit_num
        else:
            return False
    
    def pageable_sql(self, page_size=200):
        if self.pageable:
            page_size = min(page_size, self.limit_num - self.cnt)
            return self.sql + f" limit {self.offset}, {page_size}"
        else:
            return self.sql

class StopWatch():
    def __init__(self):
        self.ts = None
        self.all_cost = 0.0
        self.last_cost = 0.0
    
    def start(self):
        self.ts = time.time()
    
    def stop(self):
        self.last_cost = (time.time() - self.ts) * 1000
        self.all_cost += self.last_cost
        self.ts = None
    
    def reset(self):
        self.all_cost = 0.0
        self.last_cost = 0.0

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
    sql_warp = SQL_WARP(sql, limit_num)
    stop_watch = StopWatch()

    stop_watch.start()
    resp_data = lws.sql_query(sql_warp.pageable_sql(), db_id)['data']
    stop_watch.stop()
    print(f"query cost: {stop_watch.last_cost} ms, {sql_warp.pageable_sql()}")

    all_data = list()
    all_data.append(resp_data)

    cnt = int(resp_data["resultSet"]["count"])
    sql_warp.offset_inc(cnt)

    # 每日查询次数又上限，可能就100次
    if sql_warp.has_more(): 
        max_row = int(resp_data["resultSet"]["maxRow"])
        while sql_warp.has_more():
            stop_watch.start()
            other_resp_data = lws.sql_query(sql_warp.pageable_sql(max_row), db_id)['data']
            stop_watch.stop()
            print(f"query cost: {stop_watch.last_cost} ms, {sql_warp.pageable_sql(max_row)}")

            cnt = int(resp_data["resultSet"]["count"])
            sql_warp.offset_inc(cnt)
            all_data.append(other_resp_data)
    history_query.append({
        "sql": sql,
        "limit_num": sql_warp.limit_num,
        "cost": stop_watch.all_cost
    })
    return all_data


with st.sidebar:
    db_id = st.text_input("Db:")
    cookie = st.text_input("Cookie:")
    limit_num = st.text_input("Limit:")
    try:
        limit_num = int(limit_num)
    except:
        limit_num = 0
    st.text("希望返回的最大条数(SQL中包含limit则不生效)")

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

st.subheader('History Query')
if len(history_query) > 0:
    st.table(history_query)
else:
    st.write('No history query')