import glob

import pandas as pd
from bs4 import BeautifulSoup

class FinAttrClass:
    def __init__(self, data, columns):
        self.df = pd.DataFrame(data=data, columns=columns)

    def store_csv(self, store_path):
        return self.df.to_csv(store_path, index=False, sep = ';')
    
    def count_a(self, source_column, target_column):
        self.df[target_column] = self.df[source_column].apply(lambda x: str(x).count('a'))

    def contains_a(self, source_column, target_column):
        self.df[target_column] = self.df[source_column].apply(lambda x: 'YES' if x > 0 else 'NO')

    @classmethod
    def read_csv(cls, read_path, columns):
        data = pd.read_csv(read_path, sep=';')
        return cls(data.to_dict(orient='records'), columns)

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

def parse_xml(xml: str = None, header: list = None) -> list:
    """
    Given an an XML file as string, it will find all the required attributes inside FinInstrm > FinInstrmGnlAttrbts > [...]
    Args:
    - xml (str): xml parsed as string
    Return:
    - xml_file (pd.DataFrame): after selecting only the desired fields from the xml, they are stored as a Pandas Dataframe
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

    # fin_instrm_df = pd.DataFrame(table, columns = header)
    return table

header = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']

xml_file = read_local_xml(read_path = 'downloads/')
fin_instrm_table = parse_xml(xml = xml_file, header=header)

fin_instrm = FinAttrClass(data = fin_instrm_table, columns = header)

fin_instrm.store_csv('downloads/raw_dltins.csv')

fin_instrm.count_a('FinInstrmGnlAttrbts.FullNm', 'a_count')
fin_instrm.store_csv('downloads/transformed_1_dltins.csv')

fin_instrm.contains_a('a_count', 'contains_a')
fin_instrm.store_csv('downloads/transformed_2_dltins.csv')

# fin_instrm_df = fin_instrm.df

# fin_instrm_df1 = FinAttrClass.read_csv('downloads/raw_dltins.csv', columns=header)