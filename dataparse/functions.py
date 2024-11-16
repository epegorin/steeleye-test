import glob
import pandas as pd
from bs4 import BeautifulSoup


def read_local_xml(read_path: str = 'downloads/') -> str:
    # TODO add docstring

    xml_files = glob.glob(f'{read_path}*.xml')

    # TODO if length of xml_files != 1 ...

    with open(xml_files[0], 'r') as f:
        xml_file = f.read(4000) # TODO remove 4000

    return xml_file


def parse_xml(xml: str = None) -> pd.DataFrame:
    # TODO add docstring

    header = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']
    table = []

    data = BeautifulSoup(xml, features='xml')

    fin_instrm = data.find_all('FinInstrm')

    for fin_instrm_item in fin_instrm:
        Issr = fin_instrm_item.find('Issr').text
        fin_instrm_gnl_attrbts = fin_instrm_item.find('FinInstrmGnlAttrbts')

        row = {}
        for attribute in header[:-1]:
            row[attribute] = fin_instrm_gnl_attrbts.find(attribute.split('.'[-1])).text
        row['Issr'] = Issr

        table.append(pd.Series(row))

    fin_instrm_df = pd.DataFrame(table, columns = header)
    return fin_instrm_df

def df_to_csv(df: pd.DataFrame = None, store_path: str = 'downloads/', delimiter: str = ';') -> None:
    # TODO add docstring

    df.to_csv(f'{store_path}raw_dltins.csv', index=False, sep = delimiter)

def count_a(full_nm):
    return 0 if full_nm is None else full_nm.count('a')
def contains_a(a_count):
    return 'YES' if a_count > 0 else 'NO'

def transform_csv(func, df: pd.DataFrame, column: str = None):

    df['a_count'] = df[column].apply(func)
    return df


xml_file = read_local_xml(read_path = 'downloads/')
fin_instrm_df = parse_xml(xml = xml_file)
# df_to_csv(df = fin_instrm_df)
transformed_df = transform_csv(func = count_a, df=fin_instrm_df, column = 'FinInstrmGnlAttrbts.FullNm')
transformed_df = transform_csv(func = contains_a, df=transformed_df, column = 'a_count')
print(transformed_df)
