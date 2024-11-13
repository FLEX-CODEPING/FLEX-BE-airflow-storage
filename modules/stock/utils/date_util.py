from datetime import datetime
def get_dated_filename(base_name):
    today = datetime.now().strftime('%Y%m%d')
    return f"{base_name}_{today}.csv"