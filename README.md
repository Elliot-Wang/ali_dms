# ali_dms

This project provides a DMS SQL console using Streamlit and websockets to query data from a database. The user can input a database ID, cookie, and SQL query in the Streamlit UI, and the application will execute the query and display the results in a Pandas DataFrame.

## Usage
- Visit https://alidms-console.streamlit.app/
- Enter the database ID, cookie, and SQL query in the Streamlit UI
- Click the "Execute" button to execute the query
- The results will be displayed in a Pandas DataFrame

### How to get the database ID and cookie
- Go to the Ali DMS database console page
- Open the developer tools (F12 or right-click -> Inspect)
- Execute any query, and in the "Network" tab, find the "ws" request
- In the "Headers" section, find the "Cookie" entry, and copy the value 
- In the "Message" section, find the "dbId" key of the first message, and copy the value
- Paste them in the Streamlit UI

## TODO
- [x] Catch errors in the websocket and display them in the UI