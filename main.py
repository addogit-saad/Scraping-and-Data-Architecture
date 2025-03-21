# from scrapper import GetData
from parser import PDFParser
import os
import argparse
from cleaner import create_cleaned_table
import pandas as pd

def clean_all_files(parsed_arr, crop_type, year_col):
    data_arr = []
    for data in parsed_arr:
        data_arr.append(create_cleaned_table(data, crop_type, year_col))
    return pd.concat(data_arr, axis=0)

def parse(base_dir, prev_year_incl):
    if os.path.isfile(base_dir):
        base_dir, file_paths = base_dir.split('/')
        file_paths = [file_paths]
    else:
        file_paths = os.listdir(base_dir)
    final_df = []
    for file_path in file_paths:
        if file_path == 'backup':
            continue
        crop_type, _, year_col = file_path.split('_')
        year_col = year_col.split('.')[0]
        parser = PDFParser(os.path.join(base_dir, file_path), year_col, crop_type)
        parsed_data = parser.parse_pdf()
        combined_df  = clean_all_files(parsed_data, crop_type, year_col)
        if prev_year_incl and year_col != '2020-21':
            year_1, year_2 = list(map(lambda x: int(x), year_col.split('-')))
            year_col_2 = f'{year_1-1}-{year_2-1}'
            combined_df_2 = clean_all_files(parsed_data, crop_type, year_col_2)
            combined_df = pd.concat([combined_df, combined_df_2], axis=0)
        combined_df.to_csv(f'{crop_type}_{year_col}.csv', index=False)
        final_df.append(combined_df)
    if len(final_df) > 1:
        pd.concat(final_df, axis=0).to_parquet('DATA.parquet')

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--cmd', type=str, help="""
                        THIS COMMAND IS DEPRECATED AND WILL NOT BE MAINTAINED
                        default case is force and download
                        [force/download/parse.]
                            force: Force download the web data.
                            download: Download the pdf files.
                            parse: Parse the pdf files.
                        """)
    parser.add_argument('--prev', type=int, help="""
                        default case is don't include.
                        specifying an int (0=False, 1=True) value of True means each current file would 
                        include previous years data.
    """)
    parser.add_argument('--base_dir', type=str, help="""
                        Specify file OR File directory.
                        """)
    args = parser.parse_args()

    match args.cmd:
        case 'force':
            raise DeprecationWarning('The following implementation is deprecated.')
            base_link = 'https://crs.agripunjab.gov.pk/reports'
            scraper = GetData(base_link, force=True)
        case 'download':
            raise DeprecationWarning('The following implementation is deprecated.')
            base_link = 'https://crs.agripunjab.gov.pk/reports'
            scraper = GetData(base_link) #, force=True)
            scraper.download()
        case 'parse':
            pass
        case _:
            pass
    # Parse data here
    prev_year_incl = bool(args.prev) if args.prev is not None else False
    base_dir = args.base_dir if args.base_dir else 'pdf_files'
    parse(base_dir, prev_year_incl=prev_year_incl)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('An error occured in the execution of the program.')
        print(e)