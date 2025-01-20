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
    def __init__(self, file_path, year_col):
        self.file_path = file_path
        match year_col:
            case '2023-24':
                def text_extractor(page):
                    page_text = page.extract_text()
                    try:
                        heading = re.search(r'.*FINAL ESTIMATE OF(.*?)\n', page_text, re.DOTALL).group(1).strip() if page_text else ''
                        unit_search = re.search(r'\((Avg\..*|Production.*|Area.*)\)', page_text, re.DOTALL) if page_text else ''
                        unit = unit_search.group(1).strip() if unit_search else ''
                    except AttributeError as e:
                        return None
                    return None if heading == '' else (heading, unit)
                def tabulator(page):
                    settings = {
                        "vertical_strategy": "text",
                        "horizontal_strategy": "text"
                    }
                    table = page.extract_table(table_settings=settings)
                    table_df = pd.DataFrame(table)
                    header = page.extract_table()
                    header_df = pd.DataFrame(header).iloc[:2]
                    table_df.at[0] = header_df.iloc[0]
                    table_df.at[1] = header_df.iloc[1]
                    return table_df.iloc[:-1]
                self.__get_text__ = text_extractor
                self.__get_tabulator__ = tabulator
            case '2022-23':
                raise NotImplementedError
                def text_extractor(page):
                    page_text = page.extract_text()
                    try:
                        heading = re.search(r'.*FINAL ESTIMATE OF(.*?)\n', page_text, re.DOTALL).group(1).strip() if page_text else ''
                        unit_search = re.search(r'(AREA .*|AV\. .*|AVG\. .*|PROD: .*|PRODUCTION .*|AV: |YIELD )\n', page_text)
                        unit = unit_search.group(1).strip().replace(')', '') if unit_search else ''
                    except AttributeError as e:
                        return None
                    return None if heading == '' else (heading, unit)
                def tabulator(page):
                    settings = {
                        "vertical_strategy": "text",
                        "horizontal_strategy": "text"
                    }
                    table = page.extract_table(table_settings=settings)
                    table_df = pd.DataFrame(table)
                    header = page.extract_table()
                    header_df = pd.DataFrame(header).iloc[:2]
                    table_df.at[0] = header_df.iloc[0]
                    table_df.at[1] = header_df.iloc[1]
                    return table_df.iloc[:-1]
                self.__get_text__ = text_extractor
                self.__get_tabulator__ = tabulator
            case '2021-22':
                def text_extractor(page):
                    page_text = page.extract_text()
                    try:
                        t1 = re.search(
                            r'.*FINAL ESTIMATE OF(.*?)\n', 
                            page_text, 
                            re.DOTALL
                        )
                        t2 = re.search(
                            r'\n(.*).\(FINAL .*2021-22\)',
                            page_text,
                            re.DOTALL
                        )
                        if t1:
                            heading = t1.group(1).strip()
                        elif t2:
                            heading = t2.group(1).strip() + ' CROP'
                        else:
                            raise AttributeError
                        if heading == 'SASAMUM CROP':
                            unit = ''
                        else:
                            unit_search = re.search(r'(AREA .*|AV\. .*|AVG\. .*|PROD: .*|PRODUCTION .*|AV: .*|YIELD .*)\n', page_text) if page_text else ''
                        unit = unit_search.group(1).strip() if unit_search else ''
                        unit = unit.strip().replace(')', '')
                    except AttributeError as e:
                        return None
                    return None if heading == '' else (heading, unit)
                def tabulator(page):
                    # Default case
                    settings = {
                        "vertical_strategy": "text",
                        "horizontal_strategy": "text",
                    }
                    table = page.extract_table(table_settings=settings)
                    table = [row for row in table if any(val != '' for val in row)]
                    table = table[:-1] if any('source: ' not in val for val in table[-1]) else table
                    if page.page_number >= 8 and page.page_number <= 12:
                        settings = {
                            "vertical_strategy": "text",
                            "horizontal_strategy": "text",
                        }

                    else:
                        header = page.extract_table()
                        i = 0
                        for i, row in enumerate(header):
                            if row[0].startswith('DIVISIONS'):
                                break
                        header = header[i:]
                        table_df = pd.DataFrame(table)
                        header_df = pd.DataFrame(header)
                        table_df.at[0] = header_df.iloc[0]
                        table_df.at[1] = header_df.iloc[1]
                    return table_df
                self.__get_text__ = text_extractor
                self.__get_tabulator__ = tabulator
            case '2020-21':
                def text_extractor(page):
                    page_text = page.extract_text()
                    if page.page_number <= 2: 
                        return None
                    try:
                        heading = re.search(r'.*FINAL ESTIMATE OF(.*?)\n', page_text, re.IGNORECASE).group(1).strip() if page_text else ''
                        if (page.page_number >=64 and page.page_number <= 69) or (page.page_number >= 73):
                            unit = ''
                        else:
                            unit_search = re.search(r'(AVG\..*|PRODUCTION.*|AREA.*|AVG:.*|Avg.*)\n', page_text, re.IGNORECASE) if page_text else ''
                            unit = unit_search.group(1).strip() if unit_search else ''
                    except AttributeError as e:
                        return None
                    return None if heading == '' else (heading, unit)
                def tabulator(page):
                    settings = {
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines"
                    }
                    table = page.extract_table(table_settings=settings)
                    table.insert(2, table[2])
                    if page.page_number >= 70 and page.page_number <= 72:
                        table = [row[:-2] for row in table]
                        table = [[table[0][0]] + ['2020-21' for _ in range(len(table[0])-1)]] + table
                        table[1][0] = None
                    if page.page_number == 93:
                        table[0][0] = 'DIVISIONS /\nDISTRICTS'
                        table[1][0] = None
                    table_df = pd.DataFrame(table)
                    
                    return table_df
                self.__get_text__ = text_extractor
                self.__get_tabulator__ = tabulator
                


    def get_text(self, page):
        return self.__get_text__(page=page)

    def get_tables(self, page):
        return self.__get_tabulator__(page=page)

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
    file_path = 'pdf_files/kharif_links_2020-21.pdf'
    year_col = '2020-21'
    parser = PDFParser(file_path, year_col)
    parsed_data = parser.parse_pdf()
    print(len(parsed_data))