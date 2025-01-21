import pandas as pd
import re
import numpy as np

def create_cleaned_table(data, crop_type, year_col):
    df = data['table'].copy(deep=True)

    # Fix column names
    df.iloc[0] = df.iloc[0].ffill(axis=0)
    if df.iloc[1, 0] is None:
        try:
            df.at[0, 0], df.at[1, 0] = df.iloc[0, 0].split('/\n')
        except Exception:
            df.at[0, 0], df.at[1, 0] = df.iloc[0, 0].split('/')
    if data['unit'] == '':
        df.at[0, 0], df.at[1, 0] = df.iloc[1, 0], df.iloc[0, 0]
    df.at[0, 0] = df.iloc[0, 0].upper().strip()
    df.at[1, 0] = df.iloc[1, 0].upper().strip()
    df.columns = pd.MultiIndex.from_arrays([df.iloc[0], df.iloc[1]])

    def fetch_drop_index(iter_df):
        ix = 0
        for row in iter_df[('DIVISIONS', 'DISTRICTS')]:
            ix += 1
            if 'PUNJAB' in row:
                return ix
        return -1

    # Fix column levels
    if data['unit'] == '':
        df.columns = df.columns.swaplevel(0, 1)
        df = df[['DIVISIONS', year_col]]
        drop_index = fetch_drop_index(df)
        df = df.iloc[drop_index+1:]
        df.columns = df.columns.droplevel(1)
    else:
        df = df[['DIVISIONS', year_col]]
        drop_index = fetch_drop_index(df)
        df = df.iloc[drop_index+1:]
        df.columns = df.columns.droplevel(0)

    # Add divisions
    df['DIVISIONS'] = ''
    last_div = ''
    for ix, row in df.iterrows():
        district_val = row['DISTRICTS']
        extract = re.search(r'(.*) D.*V.*:', district_val, flags = re.IGNORECASE)
        # Drop row if row district has punjab.
        extract_drop = re.search(r'.*PUNJAB.*', district_val, flags = re.IGNORECASE)
        if extract_drop:
            df.drop(index=ix, axis=0, inplace=True)
            continue
        if extract:
            last_div = extract.group(1)
            df.loc[ix, 'DIVISIONS'] = extract.group(1)
            df.drop(index=ix, axis=0, inplace=True)
            continue
        df.loc[ix, 'DIVISIONS'] = last_div
    df['DIVISIONS'] = df['DIVISIONS'].map(lambda x: x.title())

    # Clean Table for storage
    col_list = df.columns.tolist()
    df = pd.melt(df, id_vars=[col_list[0], col_list[-1]], var_name='MEASURE_TYPE', value_name='MEASURE_VALUE')
    df['TITLE'] = data['heading']
    df['YEAR'] = year_col

    # Add unit value
    if data['unit'] == '':
        df['UNIT'] = ''
        for ix, row in df.iterrows():
            row_val = row['MEASURE_TYPE']
            try:
                df.loc[ix, 'MEASURE_TYPE'], df.loc[ix, 'UNIT'] = re.search(r'(.*)\((.*)\)', row_val).groups()
            except Exception:
                temp = df.loc[ix, 'UNIT'].split(' ')
                df.loc[ix, 'MEASURE_TYPE'], df.loc[ix, 'UNIT'] = temp[0], temp[-1]

    else:
        df['UNIT'] = data['unit']
    def convert_to_numeric(val):
        try:
            return str(float(val))
        except ValueError:
            return 'NOVAL'
    df['MEASURE_VALUE'] = df['MEASURE_VALUE'].apply(convert_to_numeric)
    df = df[df['MEASURE_VALUE'] != 'NOVAL']
    df.loc[:, 'MEASURE_VALUE'] = df['MEASURE_VALUE'].astype(np.float32)
    df['CROP_TYPE'] = crop_type.title()
    return df[['CROP_TYPE', 'YEAR', 'TITLE', 'DIVISIONS', 'DISTRICTS', 'MEASURE_TYPE', 'UNIT', 'MEASURE_VALUE']].reset_index(drop=True)