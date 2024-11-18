import pytest
import logging

from dataingest.functions import get_secondary_url_from_xml
from dataparse.functions import parse_xml


@pytest.fixture
def sample_primary_xml():
    xml_data = """
    <root>
        <doc>
            <str name="download_link">https://example.com/not_second_file</str>
        </doc>
        <doc>
            <str name="download_link">https://example.com/second_file</str>
        </doc>
        <doc>
            <str name="download_link">https://example.com/not_second_file</str>
        </doc>        
    </root>
    """
    return xml_data

@pytest.fixture
def sample_secondary_xml():
    xml_data = """
    <root>
        <FinInstrm>
            <FinInstrmGnlAttrbts>
                <Id>AT0000A2BJ35</Id>
                <FullNm>Turbo Long Open End Zertifikat auf SAP SE</FullNm>
                <ClssfctnTp>RFSTCB</ClssfctnTp>
                <CmmdtyDerivInd>false</ClssfctnTp>
                <NtnlCcy>EUR</ClssfctnTp>
            </FinInstrmGnlAttrbts>
            <Issr>529900M2F7D5795H1A49</Issr>
        </FinInstrm>
    </root>
    """
    return xml_data

def test_get_secondary_url_from_xml(sample_primary_xml):
    logger = logging.getLogger("test_logger")
    
    secondary_url = get_secondary_url_from_xml(logger, sample_primary_xml)
    
    assert secondary_url == "https://example.com/second_file"


def test_parse_xml(sample_secondary_xml):
    logger = logging.getLogger("test_logger")
    
    header = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', "Issr"]

    table = parse_xml(logger, sample_secondary_xml, header)

    assert (table[0] == ['AT0000A2BJ35', 'Turbo Long Open End Zertifikat auf SAP SE', 'RFSTCB', 'false', 'EUR', '529900M2F7D5795H1A49']).all()