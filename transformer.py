from dataingest.functions import get_primary_xml, get_secondary_url_from_xml, get_secondary_xml
from dataparse.functions import read_local_xml, parse_xml
from dataparse.functions import FinAttrClass

url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

decoded_xml = get_primary_xml(url=url)
secondary_download_link = get_secondary_url_from_xml(decoded_xml=decoded_xml)

get_secondary_xml(url=secondary_download_link)

header = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']

xml_file = read_local_xml(read_path = 'downloads/')
fin_instrm_table = parse_xml(xml = xml_file, header=header)

fin_instrm = FinAttrClass(data = fin_instrm_table, columns = header)

fin_instrm.store_csv('downloads/raw_dltins.csv')

raw_dltins = FinAttrClass.read_csv('downloads/raw_dltins.csv')
raw_dltins.count_char('FinInstrmGnlAttrbts.FullNm', 'a_count', 'a')
raw_dltins.store_csv('downloads/transformed_1_dltins.csv')

transformed_1_dltins = FinAttrClass.read_csv('downloads/transformed_1_dltins.csv')
transformed_1_dltins.contains_char('a_count', 'contains_a')
transformed_1_dltins.store_csv('downloads/transformed_2_dltins.csv')

transformed_2_dltins = FinAttrClass.read_csv('downloads/transformed_2_dltins.csv')
