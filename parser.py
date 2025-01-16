import pdfplumber
import pandas as pd
import re
from tqdm import tqdm


class DataStore:
        def __init__(self, data=[]):
            self.parsed_data = data

        def __len__(self):
            return len(self.parsed_data)
        
        def __getitem__(self, ix):
            return self.parsed_data[ix]
        
        def append(self, data):
            data = {
                'heading': data[0][0],
                'unit': data[0][1],
                'table': data[1]
            }
            self.parsed_data.append(data)

class PDFParser:        
    def __init__(self, file_path):
        self.file_path = file_path

    def get_text(self, page):
        page_text = page.extract_text()
        try:
            heading = re.search(r'FINAL ESTIMATE OF(.*?)\n', page_text, re.DOTALL).group(1).strip() if page_text else ''
            unit_search = re.search(r'\((Avg\..*|Production.*|Area.*)\)', page_text, re.DOTALL) if page_text else ''
            unit = unit_search.group(1).strip() if unit_search else ''
        except AttributeError as e:
            return None
        return None if heading == '' else (heading, unit)

    def get_tables(self, page):
        settings = {
            "vertical_strategy": "text",
            "horizontal_strategy": 'text'
        }   
        table = page.extract_table(table_settings=settings)
        table_df = pd.DataFrame(table)
        header = page.extract_table()
        header_df = pd.DataFrame(header).iloc[:2]
        table_df.at[0] = header_df.iloc[0]
        table_df.at[1] = header_df.iloc[1]
        return table_df.iloc[:-1]

    def parse_pdf(self):
        data = DataStore()
        print('Reading Pages: ')
        with pdfplumber.open(self.file_path) as pdf:
            for page in tqdm(pdf.pages):
                type_metric_col = self.get_text(page)
                if type_metric_col is None:
                    continue
                table = self.get_tables(page)
                data.append((type_metric_col, table))
            print('Pages Read Successfully!')
        return data

# Usage
if __name__ == '__main__':
    file_path = 'pdf_files/kharif_links_2023-24.pdf'
    parser = PDFParser(file_path)
    parsed_data = parser.parse_pdf()

    print(parsed_data[0]['table'])
