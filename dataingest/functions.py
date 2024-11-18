from zipfile import ZipFile
from urllib.request import urlopen, urlretrieve
import logging

from bs4 import BeautifulSoup


def get_primary_xml(logger: logging.Logger, url: str) -> str:
    """
    Given an URL containing an XML, this function retrieves, and return the XML as a string.
    As the XML is available online, it is more efficient to just read it other than downloading and opening a local copy
    Args:
    - url (str): the url from where the XML must be read
    Return:
    - decoded_xml (str): received XML, decoded with utf-8
    """
    # TODO add if clause for invalid URL

    logger.info(f'URL to get XML from: {url}')
    try:
        xml = urlopen(url = url)
        decoded_xml = xml.read().decode('utf-8')
    except Exception as e:
        raise ValueError(f'Could not open and decode URL into XML: {e}')   

    logger.info('XML converted to local string')
    return decoded_xml


def get_secondary_url_from_xml(logger: logging.Logger, decoded_xml: str) -> str:
    """
    Given an XML containing an download URL, this function retrieves, and return the second URL of the list as a string
    Args:
    - decoded_xml (str): the xml from where the download URL must be retrieved
    Return:
    - secondary_download_link (str): the second download URL from the xml, identified by "download_link" in the XML
    """
    # TODO add if clause for invalid keys in XML
    
    secondary_download_link = ''

    data = BeautifulSoup(decoded_xml, features='xml')
    try:
        doc_data = data.find_all('doc')
        secondary_download_link = doc_data[1].find('str', {'name': 'download_link'}).text
        if len(secondary_download_link) <= 0:
                raise ValueError('Could not find tag "download_link" in XML file')
    except Exception as e:
        logger.error(f"An error occurred while searching for dowload_link xml: {e}")
    
    logger.info('URL for the second file extracted from the first XML')
    return secondary_download_link


def get_secondary_xml(logger: logging.Logger, url: str, store_path: str = 'downloads/') -> None:
    """
    Given an download_link for a ZIP file containing an XML, this function retrieves the ZIP, unzips it, and saves the XML file locally
    Both files are stored in /downloads/ folder
    Args:
    - url (str): the url from where the ZIP must be downloaded
    Return:
    - None
    """
    # TODO add if not zip clause
    # TODO guarantee only 1 xml is created

    zip_filename = url.split('/')[-1]
    urlretrieve(url=url, filename=store_path+zip_filename)
    with ZipFile(store_path+zip_filename, 'r') as zObject:
        zObject.extractall(path=store_path)

    logger.info(f'Secondary XML extracted from the downloaded zip and stored locally in: {store_path}')
