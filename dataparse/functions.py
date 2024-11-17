import glob

import pandas as pd
from bs4 import BeautifulSoup

class FinAttrClass:
    def __init__(self, data, columns):
        self.df = pd.DataFrame(data=data, columns=columns)

    def store_csv(self, store_path: str):
        """
        Given a store_path as input, use pandas to_csv() function to store the csv. The delimiter is always set as ';'
        Args:
        - store_path (str): folder path to where the csv will be stored
        Return:
        - None
        """

        self.df.to_csv(store_path, index=False, sep = ';')
    
    def count_char(self, source_column: str, target_column: str, target_char: str = 'a'):
        """
        Creates a new column by the name of the target_column that will be the result of the count of target_char in the source_column
        Args:
        - source_column (str): column that will receive the aggregation
        - target_column (str): column to store the result of the aggregation
        - target_char (str): the char that will be counted
        Return:
        - None
        """

        self.df[target_column] = self.df[source_column].apply(lambda x: str(x).count(target_char))

    def contains_char(self, source_column: str, target_column: str):
        """
        Creates a new column by the name of the target_column that will be return YES/NO wheter the source column is > 0 or no
        Args:
        - source_column (str): column that will receive the comparison
        - target_column (str): column to store the result of the aggregation
        Return:
        - None
        """

        self.df[target_column] = self.df[source_column].apply(lambda x: 'YES' if x > 0 else 'NO')

    @classmethod
    def read_csv(cls, read_path):
        """
        Given an read_path, this will read the csv in that path, infer the columns and return a class instance with that data and columns
        Args:
        - read_path (str): folder path to where the csv will be read
        Return:
        - class instance with the data and columns defined
        """
                
        df = pd.read_csv(read_path, sep=';')
        data = df.to_dict(orient='records')
        columns = df.columns.tolist()
        return cls(data = data, columns = columns)

def read_local_xml(read_path: str = 'downloads/') -> str:
    """
    Given an read_path as input, it will read all XML files in the folder and keep the first, then return the xml as a string
    Args:
    - read_path (str): folder path to where the XML is stored
    Return:
    - xml_file (str): received XML as a string
    """

    xml_files = glob.glob(f'{read_path}*.xml')

    # TODO if length of xml_files != 1 ...

    with open(xml_files[0], 'r') as f:
        xml_file = f.read(4000) # TODO remove 4000

    return xml_file

def parse_xml(xml: str, header: list) -> list:
    """
    Given an an XML file as string, it will find all the required attributes inside FinInstrm > FinInstrmGnlAttrbts > [...]
    Args:
    - xml (str): xml parsed as string
    - header (list):    this list serves the double purpose. 
                        First being the column names
                        Using split('.')[0], we can get the the search-key for the parsed xml
    Return:
    - table (list): after selecting only the desired fields from the xml, they are stored as a bi-dimensional list that can be used to create a Pandas DataFrame
    """

    table = []

    data = BeautifulSoup(xml, features='xml')

    fin_instrm = data.find_all('FinInstrm')

    for fin_instrm_item in fin_instrm:
        fin_instrm_gnl_attrbts = fin_instrm_item.find('FinInstrmGnlAttrbts')

        row = {}
        for attribute in header:
            if attribute == 'Issr':
                row[attribute] = fin_instrm_item.find('Issr').text
            else:
                row[attribute] = fin_instrm_gnl_attrbts.find(attribute.split('.'[-1])).text


        table.append(pd.Series(row))

    return table