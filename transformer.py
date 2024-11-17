from dataingest.functions import get_primary_xml, get_secondary_url_from_xml, get_secondary_xml
from dataparse.functions import read_local_xml, parse_xml
from dataupload.functions import fsspec_upload

from dataparse.functions import FinAttrClass

# Step 1, data ingest

url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

decoded_xml = get_primary_xml(url=url)
secondary_download_link = get_secondary_url_from_xml(decoded_xml=decoded_xml)

get_secondary_xml(url=secondary_download_link)

# Step 2, data parse

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

# Step 3, data upload

# General variables
csv_local_path = 'downloads/'
file_name = 'transformed_2_dltins.csv'
target_path = ''
target = 's3'

if target == 's3': # AWS specific variables
    aws_bucket_name = 'steeleye_test'
    aws_region = 'eu-central-1'
    aws_access_key = '<iam_user_access_key>' #Key-pairs should not be hardcoded in the code, but since creating an aws profile is not avaiable for this scenario, this will be sufficient
    aws_secret_key = '<iam_user_secret_key>'
    client_kwargs = {'region_name':aws_region, 'aws_access_key_id':aws_access_key, 'aws_secret_access_key':aws_secret_key}
    target_path = aws_bucket_name # This assumes that the azure container and the s3 bucket have the same name
elif target == 'azure': #Azure specific variables
    account_name = "steeleye"
    account_key = "<account-key>"
    azure_container_name = "steeleye_test"
    client_kwargs = {'account_name':account_name, 'account_key':account_key}
    target_path = f"azure://{account_name}:{account_key}@{azure_container_name}"
else:
    # TODO add invalid target prompt
    pass

fsspec_upload(client_kwargs, target_path, file_name, csv_local_path, target)
