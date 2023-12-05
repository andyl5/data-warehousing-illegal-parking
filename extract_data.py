import pandas as pd
from sodapy import Socrata

data_url = 'data.cityofnewyork.us'
app_token = 'P6aMTgXXbSq4qlMfdjZTfZcdq'
client = Socrata(data_url, app_token)
client.timeout = 600

# # 311 dataset
for year in range(2021, 2024):
    start = 0
    chunk_size = 2000
    results = []
    where_clause = f"complaint_type = 'Illegal Parking' AND date_extract_y(created_date)={year}"
    data_set = 'erm2-nwe9'
    record_count = client.get(data_set, where=where_clause, select='COUNT(*)')
    print(f'Fetching Illegal Parking complaints data...')
    while True:
        results.extend(client.get(data_set, where=where_clause, offset=start, limit=chunk_size))
        start += chunk_size
        if (start > int(record_count[0]['COUNT'])):
            break
    df = pd.DataFrame.from_records(results)
    df.to_csv(f'data/311_illegal_parking_complaints_{year}.csv', index=False)


# Open Parking dataset
for year in range(2021, 2024):
    results = []
    where_clause = f"violation LIKE '%PARKING%' AND (issue_date LIKE '%{year}')"
    data_set = 'nc67-uf89'
    print(f'Fetching Open Parking violations data...')
    results.extend(client.get(data_set, where=where_clause, offset=start, limit=1000000))
    df = pd.DataFrame.from_records(results)
    # Remove interest_amount since it is creating a bug when importing to BigQuery
    df = df.drop(['interest_amount'], axis=1)
    df.to_csv(f'data/open_parking_violations_{year}.csv', index=False)