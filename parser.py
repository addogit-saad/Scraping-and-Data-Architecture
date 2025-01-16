import pdfplumber
import pandas as pd
import re
from tqdm import tqdm

class PDFParser:
    def __init__(self, file_path):
        self.file_path = file_path

    def get_text(self, page):
        page_text = page.extract_text()
        try:
            heading = re.search(r'FINAL ESTIMATE OF(.*?)\n', page_text, re.DOTALL).group(1).strip() if page_text else ''
            unit = re.search(r'Crop Reporting Service, Punjab(.*?)\n', page_text, re.DOTALL).group(1).strip() if page_text else ''
        except AttributeError as e:
            return None
        return {'heading': heading, 'unit': unit}

    def get_tables(self, page):
        tables = page.extract_tables()
        return pd.DataFrame(tables[0])

    def parse_pdf(self):
        data = []
        print('Reading Pages: ')
        with pdfplumber.open(self.file_path) as pdf:
            for page in tqdm(pdf.pages):
                type_metric_col = self.get_text(page)
                if type_metric_col is None:
                    continue
                tables = self.get_tables(page)
                data.append({'text': type_metric_col, 'tables': tables})
            print('Pages Read Successfully!')
        return data

# Usage
if __name__ == '__main__':
    file_path = 'pdf_files/kharif_links_2020-21.pdf'
    parser = PDFParser(file_path)
    parsed_data = parser.parse_pdf()
    print(parsed_data[0])

