import mysql.connector
import pandas as pd

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Compl3xity12!",
    database="HCPCS_DATA"
)

# Read data as chunks. Append the chunks to a list and concatenate into a dataframe and then Process dataframe

unuseful_col = ['HCPCS_BETOS_CD', 'PSPS_ERROR_IND_CD', 'PSPS_HCPCS_ASC_IND_CD', ]
data_chunks = pd.read_csv("/Users/buahinak/Downloads/PSPS2018.csv", chunksize=100000,
                          usecols=lambda column: column not in unuseful_col, iterator=True)

chunks_iterable = []

for chunk in data_chunks:
    chunks_iterable.append(chunk)

HCPCS_data = pd.concat(chunks_iterable)

# Changes entries with 'NaN' to None
HCPCS_data = HCPCS_data.where((pd.notnull(HCPCS_data)), None)

curs = connection.cursor()

# Create database

DB_query = """CREATE DATABASE HCPCS_DATA"""
# curs.execute(DB_query)

# Split relevant data into 3 tables including a glossary for the data

"""Table 1: Primary Code identifiers for medicare claims including carrier information and service information
    Columns: HCPS_Code,HCPCS_INITIAL_MODIFIER_CD, HCPCS_SECOND_MODIFIER_CD, PROVIDER_SPEC_CD, CARRIER_NUM,
             PRICING_LOCALITY_CD, TYPE_OF_SERVICE_CD,PLACE_OF_SERVICE_CD """

serviceinfo_query = """CREATE TABLE Service_Codes (
                                    HCPCS_Code VARCHAR(20),
                                    HCPCS_INITIAL_MODIFIER_CD VARCHAR(20),
                                    HCPCS_SECOND_MODIFIER_CD VARCHAR(20),
                                    PROVIDER_SPEC_CD VARCHAR(20),
                                    CARRIER_NUM INT(20),
                                    PRICING_LOCALITY_CD VARCHAR(20), 
                                    TYPE_OF_SERVICE_CD VARCHAR(20),
                                    PLACE_OF_SERVICE_CD VARCHAR(20))
                                    """
# curs.execute(serviceinfo_query)

# Insert relevant columns from HCPCS data frame into Service_Codes Table

for row in HCPCS_data.itertuples():
    insert_query = """ INSERT INTO HCPCS_DATA.Service_Codes (
                                    HCPCS_Code,
                                    HCPCS_INITIAL_MODIFIER_CD,
                                    HCPCS_SECOND_MODIFIER_CD,
                                    PROVIDER_SPEC_CD,
                                    CARRIER_NUM,
                                    PRICING_LOCALITY_CD,
                                    TYPE_OF_SERVICE_CD,
                                    PLACE_OF_SERVICE_CD)
                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                   """
    entry = (row.HCPCS_CD, row.HCPCS_INITIAL_MODIFIER_CD, row.HCPCS_SECOND_MODIFIER_CD,
             row.PROVIDER_SPEC_CD, row.CARRIER_NUM, row.PRICING_LOCALITY_CD,
             row.TYPE_OF_SERVICE_CD, row.PLACE_OF_SERVICE_CD)
    # curs.execute(insert_query,entry)

"""
Table 2: Physician/Supplier Charges(Submitted Charges, NCH Payment etc.)
Columns: HCPCS_INITIAL_MODIFIER_CD,HCPCS_SECOND_MODIFIER_CD,PSPS_SUBMITTED_SERVICE_CNT,PSPS_SUBMITTED_CHARGE_AMT,PSPS_ALLOWED_CHARGE_AMT,
         PSPS_DENIED_SERVICES_CNT,PSPS_DENIED_CHARGE_AMT,PSPS_ASSIGNED_SERVICES_CNT,PSPS_NCH_PAYMENT_AMT
*Note: Included both Initial and Second modifier in table for possible join purposes
"""

PS_query = """CREATE TABLE Physician_Supplier_Data (
                      HCPCS_INITIAL_MODIFIER_CD VARCHAR(10),
                      HCPCS_SECOND_MODIFIER_CD VARCHAR(10),
                      PSPS_SUBMITTED_SERVICE_CNT INT(10),
                      PSPS_SUBMITTED_CHARGE_AMT FLOAT(10,2),
                      PSPS_ALLOWED_CHARGE_AMT FLOAT(10,2),
                      PSPS_DENIED_SERVICES_CNT INT(10),
                      PSPS_DENIED_CHARGE_AMT FLOAT(10,2),
                      PSPS_ASSIGNED_SERVICES_CNT INT(10),
                      PSPS_NCH_PAYMENT_AMT FLOAT(10,2))"""
# curs.execute(PS_query)

# Insert relevant data from HCPCS data frame into Physician_Supplier_Data table

for row in HCPCS_data.itertuples():
    insert_query1 = """INSERT INTO HCPCS_DATA.Physician_Supplier_Data (
                              HCPCS_INITIAL_MODIFIER_CD,
                              HCPCS_SECOND_MODIFIER_CD,
                              PSPS_SUBMITTED_SERVICE_CNT,
                              PSPS_SUBMITTED_CHARGE_AMT,
                              PSPS_ALLOWED_CHARGE_AMT,
                              PSPS_DENIED_SERVICES_CNT,
                              PSPS_DENIED_CHARGE_AMT,
                              PSPS_ASSIGNED_SERVICES_CNT,
                              PSPS_NCH_PAYMENT_AMT)
                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
    entry = (row.HCPCS_CD, row.PSPS_SUBMITTED_SERVICE_CNT, row.PSPS_SUBMITTED_CHARGE_AMT,
             row.PSPS_ALLOWED_CHARGE_AMT, row.PSPS_DENIED_SERVICES_CNT, row.PSPS_DENIED_CHARGE_AMT,
             row.PSPS_ASSIGNED_SERVICES_CNT, row.PSPS_NCH_PAYMENT_AMT)
    # curs.execute(insert_query1,entry)

"""Table3: Key for data using PSPS Record Layout
   Columns: Name, Description"""

hcpcs_dict = {"HCPCS_Code": "Collection of codes that represent procedures, supplies, products and "
                            "services which may be provided to Medicare beneficiaries "
                            "and to individuals enrolled in private health insurance programs.",
              "HCPCS_INITIAL_MODIFIER_CD": "A first modifier to the HCPCS procedure code to enable "
                                           "a more specific procedure identification for the line item "
                                           "service on the non-institutional claim.",
              "PROVIDER_SPEC_CD": "CMS specialty code used for pricing the line item service on the "
                                  "noninstitutional claim.",
              "CARRIER_NUM": "The identification number assigned by CMS to a carrier authorized to "
                             "process claims from a physician or supplier.",
              "PRICING_LOCALITY_CD": "Code denoting the carrier-specific locality used for pricing "
                                     "the service for this line item on the carrier claim (non-DMERC)."
                                     "For DMERCs, this field contains the beneficiary SSA State Code ",
              "TYPE_OF_SERVICE_CD": "Code indicating the type of service, as defined in the CMS Medicare"
                                    " Carrier Manual, for this line item on the non-institutional claim.",
              "PLACE_OF_SERVICE_CD": "The code indicating the place of service, as defined in the "
                                     "Medicare Carrier Manual, for this line item on the non-institutional"
                                     "claim.",
              "HCPCS_SECOND_MODIFIER_CD": "A second modifier to the HCPCS procedure code to "
                                          "make it more specific than the first modifier code to identify "
                                          "the line item procedures for this claim.",
              "PSPS_SUBMITTED_SERVICE_CNT": "The count of the total number of submitted services.",
              "PSPS_SUBMITTED_CHARGE_AMT": "The amount of charges submitted by the provider to Medicare.",
              "PSPS_ALLOWED_CHARGE_AMT": "The amount that is approved (allowed) for Medicare.",
              "PSPS_DENIED_SERVICES_CNT": "The count of the number of submitted services that are denied"
                                          " by Medicare.",
              "PSPS_DENIED_CHARGE_AMT": "The amount of submitted charges for which Medicare payment "
                                        "was denied.",
              "PSPS_ASSIGNED_SERVICES_CNT": "The count of the number of services from providers accepting"
                                            "Medicare assignment.",
              "PSPS_NCH_PAYMENT_AMT": "The amount of payment made from the trust fund (after deductible"
                                      " and coinsurance amounts have been paid)."}

# Convert dictionary into a DataFrame

hcpcs_ddf = pd.DataFrame(list(hcpcs_dict.items()), columns=['Name', 'Description'])
key_query = """CREATE TABLE PSPS_Glossary (
                      Name VARCHAR(1000),
                      Description VARCHAR(1000))"""
# curs.execute(key_query)

# Insert dictionary dataframe into PSPS_Glossary table

for row in hcpcs_ddf.itertuples():
    insertdict_query = """INSERT INTO HCPCS_DATA.PSPS_Glossary (Name, Description)
                          VALUES(%s, %s)"""
    entry = (row.Name, row.Description)
    # curs.execute(insertdict_query, entry)

connection.commit()
connection.close()
curs.close()
