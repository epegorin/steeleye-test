## Description
This repo is intended to host a solution to host an ETL solution for a proposed challenge.
The proposed solution integrates Python, Pandas, and AWS/Azure

## Requirements

1. The code must download the xml from this link ->
https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-
01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100
2. From the xml, please parse through to the second download link whose file_type is DLTINS and
download the zip
3. Extract the xml from the zip.
4. Convert the contents of the xml into a CSV with the following header:
- FinInstrmGnlAttrbts.Id
- FinInstrmGnlAttrbts.FullNm
- FinInstrmGnlAttrbts.ClssfctnTp
- FinInstrmGnlAttrbts.CmmdtyDerivInd
- FinInstrmGnlAttrbts.NtnlCcy
- Issr
5. Create a new column in the CSV file which is named `a_count`. For each row, this column will contain the
number of times the lower-case character “a” is present in the corresponding column
`FinInstrmGnlAttrbts.FullNm` (0 when missing).
6. Create a new column in the CSV file which is named `contains_a`. For each row, this column will contain
the string value “YES” if `a_count` is greater than 0, “NO” otherwise.
7. Store the csv in an AWS S3 bucket (there is no need to create an actual S3 bucket).
8. Bonus points if you use the `fsspec` library to enable your code written in point 7) to support Microsoft Azure
blob storage. Tip: read through the `fsspec` documentation and explore how you can implement cloudagnostic code.
