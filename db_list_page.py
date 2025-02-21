import streamlit as st
import pandas as pd
from dms_api import DMS_API

def prev_page():
    st.session_state.db_list_page -= 1
    st.session_state.db_list_data = None

def next_page():
    st.session_state.db_list_page += 1
    st.session_state.db_list_data = None

def show():
    st.title('DMS Database List')

    # 侧边栏配置
    with st.sidebar:
        cookie = st.text_input("Cookie:", key="db_list_cookie")
        db_id = st.text_input("Db:", key="db_list_db_id")
        page = st.number_input("Page:", key="db_list_page", min_value=1, value=1, step=1, format="%d")
        rows = st.number_input("Rows per page:", key="db_list_rows", min_value=10, max_value=1000, value=100, step=10, format="%d")

    # 主界面
    if not cookie:
        st.warning("请输入Cookie!")
        return

    try:
        dms = DMS_API(cookie)
        
        # 添加刷新按钮
        if st.button("刷新数据"):
            st.session_state.db_list_data = None
            st.rerun()

        if 'db_list_data' not in st.session_state or st.session_state.db_list_data is None:
            # 使用session_state缓存数据
            with st.spinner('加载数据中...'):
                data = dms.db_list(page=page, rows=rows, db_id=db_id)
                st.session_state.db_list_data = data
        
        data = st.session_state.db_list_data
        
        if not data:
            st.info("没有找到数据库")
            return
            

        column_config = {
                "dbId": "数据库ID",
                "dbType": "数据库类型",
                "envType": "环境类型",
                "tableId": "表ID",
                "tableSchemaName": "数据库名",
                "tableName": "表名",
                "description": "描述",
                "encoding": "编码",
                "engine": "引擎",
                "numRows": "行数",
                "ownerNames": "负责人"
        }
        # 转换为DataFrame以更好地展示
        df = pd.DataFrame(data['root'], columns=column_config.keys())

        # 显示总记录数
        st.write(f"总计: {data['totalCount']} 个数据库")
        
        # 显示数据表格
        st.dataframe(
            df,
            column_config=column_config,
            hide_index=True
        )
        
        # 显示分页信息
        st.write(f"当前页: {page}, 每页显示: {rows}")
        
        # 添加上一页/下一页按钮
        col1, col2 = st.columns(2)
        with col1:
            if page > 1 and st.button("上一页", on_click=prev_page):
                pass
                
        with col2:
            if data['totalCount'] > page * rows and st.button("下一页", on_click=next_page):
                pass

    except Exception as e:
        st.error(f"错误: {str(e)}")

if __name__ == "__main__":
    show() 