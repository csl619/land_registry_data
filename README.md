# Land Registry Data Converter

A small script to convert a land registry data file into a json file using Apache Beam.

The source datafiles can be found at: 
[GOV.UK - Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)

## Installation

#### Setting up Virtual Environment
On cloning the repository, you will need to navigate to the repository folder and set up a virtual environment:
```
python3 -m venv venv **creates the virtual environment called venv**
```

Once the virtual environment is created, activate it and install the required packages via the requirements.txt file.

```
source venv/bin/activate
pip3 install -r requirements.txt
```
    
## Run Locally

Download the source data file you wish to use from the link and save to your desired location.
For testing I used the 2020 file found at [GOV.UK - Price Paid Data 2020](http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2020.txt)
```python
wget 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2020.txt'
```
Run `process.py` this has optional command-line arguements for both `--input` and `--output` if these are not entered it will use the default values.
```
python process.py --input **your downloaded file path goes here** --output **what you wish to have the output file called (exclude the extension)**
```
A sample output file using the `sample_data.txt` file can be found in the repository called `land_registry_data.ndjson`