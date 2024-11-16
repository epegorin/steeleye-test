from dataingest.functions import get_primary_xml, get_secondary_url_from_xml, get_secondary_xml


url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"

decoded_xml = get_primary_xml(url=url)
secondary_download_link = get_secondary_url_from_xml(decoded_xml=decoded_xml)

get_secondary_xml(url=secondary_download_link)
