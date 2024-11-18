import pytest

from dataparse.functions import FinAttrClass

@pytest.fixture
def simple_count():
    data = [
        ['Maria'],
        ['Carlos'],
        ['Rui'],
    ]
    columns = ['name']
    return data, columns

@pytest.fixture
def simple_contain():
    data = [
        ['Maria', 2],
        ['Carlos', 1],
        ['Rui', 0],
    ]
    columns = ['name', 'count_char']
    return data, columns

def test_count_char(simple_count):
    data, columns = simple_count
    fin_instrm = FinAttrClass(data, columns)
    source_column = 'name'
    target_column = 'count_char'

    fin_instrm.count_char(source_column, target_column)

    assert target_column in fin_instrm.df.columns
    assert fin_instrm.df["count_char"].dtype == 'int64'
    assert (fin_instrm.df["count_char"] >= 0).all()

def test_contains_char(simple_contain):
    data, columns = simple_contain
    fin_instrm = FinAttrClass(data, columns)
    source_column = 'count_char'
    target_column = 'contains_char'

    fin_instrm.contains_char(source_column, target_column)

    assert target_column in fin_instrm.df.columns
    assert (fin_instrm.df["contains_char"].isin(['YES', 'NO'])).all()



