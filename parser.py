import pdfplumber
from pdfplumber.table import TableFinder
import pandas as pd
import re
from tqdm import tqdm
import numpy as np


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
    def __init__(self, file_path, year_col, crop_type):
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
                    def get_lines(page):
                        lines = [round(line['x1']) for line in page.vertical_edges if 70 < line['top'] < 90]
                        unique_lines = set(lines)
                        filtered_lines = sorted(unique_lines)
                        filtered_lines = [line for i, line in enumerate(filtered_lines) if i == 0 or abs(line - filtered_lines[i - 1]) >= 5]
                        return filtered_lines
                    settings = {
                        "vertical_strategy": "explicit",
                        "horizontal_strategy": "text",
                        "explicit_vertical_lines": get_lines(page)
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
            case '2021-22':
                def text_extractor_r(page):
                    page_text = page.extract_text()
                    if page.page_number <= 2: 
                        return None
                    try:
                        if 3 <= page.page_number <= 7 or 13 <= page.page_number <= 48 or page.page_number >= 64 :
                            heading = re.search(r'FINAL ESTIMATE OF (.*)\n', page_text, re.IGNORECASE).group(1).strip()
                            if 13 <= page.page_number <= 14 or 20 <= page.page_number <= 23 or 29 <= page.page_number <= 30 or 36 <= page.page_number <= 43 or page.page_number >= 64:
                                unit = ''
                            else:
                                unit = re.search(r'(AREA.*|PROD:.*|AVG:.*|AV\..*|AVG\..*|PRODUCTION.*)\n', page_text, re.IGNORECASE).group(1).strip()
                        elif 8 <= page.page_number <= 12 or 49 <= page.page_number <= 63:
                            heading = re.search(r'FINAL ESTIMATE OF (.*) IN THE.*\n', page_text, re.IGNORECASE).group(1).strip()
                            unit = re.search(r'(AREA.*|PRODUCTION.*|AV:.*|YIELD.*|AVG\..*|PROD.*|AVG:.*)\n', page_text, re.IGNORECASE).group(1).strip()
                        else:
                            raise AttributeError
                    except AttributeError as e:
                        return None
                    return None if heading == '' else (heading, unit)
                def tabulator_r(page):
                    table_df = None
                    if 13 <= page.page_number <= 48 or page.page_number >= 64:
                        settings = {
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines"
                        }  
                        table = page.extract_table(table_settings=settings)
                        table_df = pd.DataFrame(table)
                    elif 3 <= page.page_number <= 12 or 49 <= page.page_number <= 63:
                        settings = {
                            "vertical_strategy": "text",
                            "horizontal_strategy": "text"
                        }
                        table = page.extract_table(table_settings=settings)
                        header = page.extract_table()
                        table = [row for row in table if any(val != '' for val in row)]
                        table = table[:-1] if any('bhawalnagar' not in val.lower().strip() for val in table[-1]) else table
                        i = 0
                        for i, row in enumerate(table):
                            if row[0].startswith('DIVISION'):
                                break
                        table = table[i:]
                        if 8 <= page.page_number <= 12 or 49 <= page.page_number <= 63:
                            # Swap row 1 <-> row 2
                            header[0], header[1] = header[1], header[0]
                            header[0][0], header[1][0] = header[1][0].split('/')
                        table_df = pd.DataFrame(table)
                        header_df = pd.DataFrame(header)
                        table_df.at[0] = header_df.iloc[0]
                        table_df.at[1] = header_df.iloc[1]
                    else:
                        raise Exception
                    return table_df
                def text_extractor_k(page):
                    page_text = page.extract_text()
                    try:
                        t1 = re.search(r'.*FINAL ESTIMATE OF(.*?)(\n| IN|[0-9])', page_text, re.IGNORECASE)
                        t2 = re.search(r'\n*(.*).\(FINAL .*2021-22\)', page_text)
                        if t1:
                            heading = t1.group(1).strip()
                        elif t2:
                            heading = t2.group(1).strip() + ' CROP'
                        else:
                            raise AttributeError
                        if page.page_number in range(43, 45) or page.page_number >= 63:
                            unit = ''
                        else:
                            unit_search = re.search(
                                r'(AREA .*|AV\..*|AV:.*|AVG\..*|AVG:.*|PROD:.*|PRODUCTION .*|YIELD .*)',
                                page_text, re.IGNORECASE
                            ) if page_text else ''
                            unit = unit_search.group(1).strip() if unit_search else ''
                            unit = unit.strip().replace(')', '')
                    except AttributeError as e:
                        return None
                    return None if heading == '' else (heading, unit)
                def tabulator_k(page):
                    def get_lines(page):
                        lines = [round(line['x1']) for line in page.vertical_edges if 0 < line['top'] < 150]
                        unique_lines = set(lines)
                        filtered_lines = sorted(unique_lines)
                        filtered_lines = [line for i, line in enumerate(filtered_lines) if i == 0 or abs(line - filtered_lines[i - 1]) >= 3]
                        return filtered_lines
                    settings = {
                        "vertical_strategy": "explicit",
                        "horizontal_strategy": "text"
                    }
                    if 8 <= page.page_number <= 12:
                        temp_lines = get_lines(page)
                        columns_1 = np.linspace(temp_lines[1], temp_lines[2], 5).tolist()
                        columns_2 = np.linspace(temp_lines[2], temp_lines[3], 5).tolist()[1:]
                        columns_3 = np.linspace(temp_lines[3], temp_lines[4], 5).tolist()[1:]
                        lines = [temp_lines[0]] + columns_1 + columns_2 + columns_3
                        settings['explicit_vertical_lines'] = lines
                    else:
                        settings["explicit_vertical_lines"] = get_lines(page)
                    table = page.extract_table(table_settings=settings)
                    table = [row for row in table if any(val != '' for val in row)]
                    # Extract header
                    i = 0
                    for i, row in enumerate(table):
                        if any('division' in val.lower() for val in row):
                            break
                    table = table[i:]
                    # Fix header
                    if page.page_number <= 42 or 53 <= page.page_number <= 57:
                        table[0] = [table[0][0]] + ['2021-22'] * 3 + ['2020-21'] * 3 + ['%Age change'] * 3
                    elif 48 <= page.page_number <= 52:
                        header = page.extract_table()
                        table[0], table[1] = [header[0][0]] + header[1][1:], [header[1][0]] + header[0][1:]
                    elif 58 <= page.page_number <= 62:
                        table[0] = [table[0][0]] + ['2021-22'] * 5 + ['2020-21'] * 5 + ['%Age change'] * 5
                    else:
                        header = page.extract_table()
                        table[0], table[1] = header[0], header[1]
                    table = table[:-1] if any('source' in val.lower() for val in table[-1]) else table
                    return pd.DataFrame(table)
                self.__get_text__ = text_extractor_k if crop_type == 'kharif' else text_extractor_r
                self.__get_tabulator__ = tabulator_k if crop_type == 'kharif' else tabulator_r
            case '2020-21':
                def text_extractor_r(page):
                    page_text = page.extract_text()
                    if page.page_number <= 3: 
                        return None
                    try:
                        if 4 <= page.page_number <= 13 or 19 <= page.page_number <= 44 or 63 <= page.page_number <= 64 or 70 <= page.page_number <= 85 or 87 <= page.page_number <= 88 or page.page_number >= 90:
                            heading = re.search(r'FINAL ESTIMATE OF (.*)\n', page_text, re.IGNORECASE).group(1).strip()
                            if 29 <= page.page_number <= 32 or 38 <= page.page_number <= 39 or 45 <= page.page_number <= 52 or 63 <= page.page_number <= 64 or page.page_number >= 70:
                                unit = ''
                            else:
                                unit = re.search(r'(AREA.*|PRODUCTION.*|AVG\..*|AVG .*|AVG:.*)\n', page_text, re.IGNORECASE).group(1).strip()
                        elif 15 <= page.page_number <= 18:
                            heading = re.search(r'(.*)\(FINAL ESTIMATE.*\)', page_text, re.IGNORECASE).group(1).strip() + ' CROP'
                            unit = re.search(r'(AREA.*|PRODUCTION.*|AV:.*|YIELD.*|AVG\..*|PROD.*)\n', page_text, re.IGNORECASE).group(1).strip()
                        elif 14 == page.page_number or 45 <= page.page_number <= 62 or 65 <= page.page_number <= 69:
                            heading = re.search(r'FINAL ESTIMATE OF (.*) IN THE.*\n', page_text, re.IGNORECASE).group(1).strip()
                            if 45 <= page.page_number <= 52:
                                unit = ''
                            else:
                                unit = re.search(r'(AREA.*|PRODUCTION.*|AV:.*|YIELD.*|AVG\..*|PROD.*)\n', page_text, re.IGNORECASE).group(1).strip()
                        else:
                            raise AttributeError
                    except AttributeError as e:
                        return None
                    return None if heading == '' else (heading, unit)
                def tabulator_r(page):
                    table_df = None
                    if 4 <= page.page_number <= 13 or 19 <= page.page_number <= 57 or 63 <= page.page_number <= 64 or 70 <= page.page_number <= 85 or 87 <= page.page_number <= 88 or page.page_number >= 90:
                        settings = {
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines"
                        }  
                        table = page.extract_table(table_settings=settings)
                        table_df = pd.DataFrame(table)
                    elif 14 <= page.page_number <= 18 or 58 <= page.page_number <= 62 or 65 <= page.page_number <= 69:
                        settings = {
                            "vertical_strategy": "text",
                            "horizontal_strategy": "text"
                        }
                        table = page.extract_table(table_settings=settings)
                        header = page.extract_table()
                        table = [row for row in table if any(val != '' for val in row)]
                        table = table[:-1] if any('bhawalnagar' not in val.lower().strip() for val in table[-1]) else table
                        i = 0
                        for i, row in enumerate(table):
                            if row[0].startswith('DIVISION'):
                                break
                        table = table[i:]
                        if 14 <= page.page_number <= 18 or 58 <= page.page_number <= 62 or 65 <= page.page_number <= 69:
                            # Swap row 1 <-> row 2
                            header[0], header[1] = header[1], header[0]
                            header[0][0], header[1][0] = header[1][0].split('/')
                        table_df = pd.DataFrame(table)
                        header_df = pd.DataFrame(header)
                        table_df.at[0] = header_df.iloc[0]
                        table_df.at[1] = header_df.iloc[1]
                    else:
                        raise Exception
                    return table_df
                def text_extractor_k(page):
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
                def tabulator_k(page):
                    settings = {
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines"
                    }
                    table = page.extract_table(table_settings=settings)
                    #table.insert(2, table[2])
                    if page.page_number >= 70 and page.page_number <= 72:
                        table = [row[:-2] for row in table]
                        table = [[table[0][0]] + ['2020-21' for _ in range(len(table[0])-1)]] + table
                        table[1][0] = None
                    if page.page_number == 93:
                        table[0][0] = 'DIVISIONS /\nDISTRICTS'
                        table[1][0] = None
                    table_df = pd.DataFrame(table)
                    
                    return table_df
                self.__get_text__ = text_extractor_k if crop_type == 'kharif' else text_extractor_r
                self.__get_tabulator__ = tabulator_k if crop_type == 'kharif' else tabulator_r
                
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
    file_path = 'pdf_files/kharif_links_2021-22.pdf'
    year_col = '2021-22'
    crop_type = 'kharif'
    parser = PDFParser(file_path, year_col, crop_type)
    parsed_data = parser.parse_pdf()
    print(len(parsed_data))