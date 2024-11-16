from zipfile import ZipFile
from urllib.request import urlopen, urlretrieve

from bs4 import BeautifulSoup


def get_primary_xml(url: str) -> str:
    """
    Given an URL containing an XML, this function retrieves, and return the XML as a string.
    As the XML is available online, it is more efficient to just read it other than downloading and opening a local copy
    Args:
    - url (str): the url from where the XML must be read
    Return:
    - decoded_xml (str): received XML, decoded with utf-8
    """
    # TODO add if clause for invalid URL
    # TODO add loggings

    xml = urlopen(url=url)
    decoded_xml = xml.read().decode("utf-8")

    return decoded_xml


def get_secondary_url_from_xml(decoded_xml: str) -> str:
    """
    Given an XML containing an download URL, this function retrieves, and return the second URL of the list as a string
    Args:
    - decoded_xml (str): the xml from where the download URL must be retrieved
    Return:
    - secondary_download_link (str): the second download URL from the xml, identified by "download_link" in the XML
    """
    # TODO add if clause for invalid keys in XML
    # TODO add loggins

    data = BeautifulSoup(decoded_xml, features="xml")
    doc_data = data.find_all("doc")
    secondary_download_link = doc_data[1].find("str", {"name": "download_link"}).text

    return secondary_download_link


def get_secondary_xml(url: str) -> None:
    """
    Given an download_link for a ZIP file containing an XML, this function retrieves the ZIP, unzips it, and saves the XML file locally
    Both files are stored in /downloads/ folder
    Args:
    - url (str): the url from where the ZIP must be downloaded
    Return:
    - None
    """
    # TODO add if not zip clause

    zip_filename = url.split("/")[-1]
    urlretrieve(url=url, filename=f"downloads/{zip_filename}")
    with ZipFile(f"downloads/{zip_filename}", "r") as zObject:
        zObject.extractall(path="downloads/")
