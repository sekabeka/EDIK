import gspread

def set_table(keys, result, filename='credentials.json'):
    gc = gspread.service_account(filename)
    sh = gc.open('inSales')
    ws = sh.worksheet('result')
    ws.clear()
    ws.append_row(keys)
    ws.append_rows(
        [list(i.values()) for i in result]
    )




