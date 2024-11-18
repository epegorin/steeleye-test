import glob
import logging

import pandas as pd
from bs4 import BeautifulSoup

class FinAttrClass:
    def __init__(self, data, columns):
        self.df = pd.DataFrame(data=data, columns=columns)

    def store_csv(self, logger: logging.Logger, store_path: str):
        """
        Given a store_path as input, use pandas to_csv() function to store the csv. The delimiter is always set as ';'
        Args:
        - store_path (str): folder path to where the csv will be stored
        Return:
        - None
        """

        self.df.to_csv(store_path, index=False, sep = ';')
        logger.info(f'Pandas DF stored as csv at: {store_path}')
    
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
    def read_csv(cls, logger: logging.Logger, read_path):
        """
        Given an read_path, this will read the csv in that path, infer the columns and return a class instance with that data and columns
        Args:
        - read_path (str): folder path to where the csv will be read
        Return:
        - class instance with the data and columns defined
        """
        logger.info(f'Reading CSV from: {read_path} into CSV')

        df = pd.read_csv(read_path, sep=';')
        data = df.to_dict(orient='records')
        columns = df.columns.tolist()
        
        return cls(data = data, columns = columns)

def read_local_xml(logger: logging.Logger, read_path: str = 'downloads/') -> str:
    """
    Given an read_path as input, it will read all XML files in the folder and keep the first, then return the xml as a string
    Args:
    - read_path (str): folder path to where the XML is stored
    Return:
    - xml_file (str): received XML as a string
    """

    xml_files = glob.glob(f'{read_path}*.xml')

    if len(xml_files) != 1:
        raise ValueError(f'None or more than 1 XML files were found in directory: {read_path}')        

    with open(xml_files[0], 'r') as f:
        xml_file = f.read()

    logger.info(f'Local XML read from {read_path} and ready to be parsed')
    return xml_file

def parse_xml(logger: logging.Logger, xml: str, header: list) -> list:
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
    try:
        fin_instrm = data.find_all('FinInstrm')    
        if len(fin_instrm) <= 0:
            raise ValueError('Could not find tag "FinInstrm" in XML file')

        for fin_instrm_item in fin_instrm:
            fin_instrm_gnl_attrbts = fin_instrm_item.find('FinInstrmGnlAttrbts')
            if len(fin_instrm_gnl_attrbts) <= 0:
                raise ValueError('Could not find tag "FinInstrmGnlAttrbts" in XML file')

            row = {}
            for attribute in header:
                if attribute == 'Issr':
                    row[attribute] = fin_instrm_item.find('Issr').text
                    if len(row[attribute]) <= 0:
                        raise ValueError('Could not find tag "Issr" in XML file')
                else:
                    tag = attribute.split('.'[-1])
                    row[attribute] = fin_instrm_gnl_attrbts.find(tag).text
                    if len(row[attribute]) <= 0:
                        raise ValueError(f'Could not find tag "{tag}" in XML file')


            table.append(pd.Series(row))
    except Exception as e:
        logger.error(f"An error occurred while searching for tags in the xml: {e}")


    logger.info('XML parsed and converted into table')
    return table