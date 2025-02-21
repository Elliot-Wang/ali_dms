import requests

class DMS_API:
    def __init__(self, cookie):
        self.cookie = cookie
        self.base_url = "https://dms.aliyun.com"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'bx-v': '2.5.28', 
            'content-type': 'application/x-www-form-urlencoded; charset=utf-8',
            'dnt': '1',
            'origin': 'https://dms.aliyun.com',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Chromium";v="133", "Not(A:Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors', 
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'x-requested-with': 'FETCH'
        }

    def db_list(self, db_id, searchKey='', page=1, rows=100, logic=False, instance_id=None, db_type='mysql'):
        """
        获取数据库列表
        db_id: 数据库ID
        searchKey: 搜索关键字
        page: 页码
        rows: 每页条数
        logic: 是否逻辑库
        instance_id: 实例ID 可为None
        db_type: 数据库类型 可为None
        """
        url = f"{self.base_url}/meta/table/db/list"
        
        data = {
            'page': page,
            'sort': 'tableName',
            'rows': rows,
            'searchKey': searchKey,
            'logic': logic
        }
        
        if db_id:
            data['dbId'] = db_id
        if instance_id:
            data['instanceId'] = instance_id 
        if db_type:
            data['dbType'] = db_type

        response = requests.post(
            url,
            headers=self.headers,
            cookies={'cookie': self.cookie},
            data=data
        )
        
        return response.json()

    def table_columns(self, db_id, table_id=None, table_name=None, is_logic=False):
        """
        获取表字段信息
        db_id: 数据库ID
        table_id: 表ID
        table_name: 表名称
        表id和表名称二选一
        """
        url = f"{self.base_url}/meta/table/columns"

        if not table_id and not table_name:
            raise ValueError("table_id和table_name至少有一个")
        
        data = {
            'dbId': db_id,
            'tableId': table_id,
            'tableSchemaName': '', # 可为空
            'tableName': table_name,
            'isLogic': is_logic
        }

        response = requests.post(
            url,
            headers=self.headers, 
            cookies={'cookie': self.cookie},
            data=data
        )

        return response.json()
    
if __name__ == "__main__":
    cookie = ''
    dms_api = DMS_API(cookie)
    db_id = 123123
    db_list = dms_api.db_list(db_id)
    
    db_info = db_list['root'][0]
    print(db_list['root'][0])
    print(dms_api.table_columns(db_id, table_name='advance_deposit_rule'))

