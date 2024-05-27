import streamlit as st
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

with st.sidebar:
    st.image("https://www.onepointltd.com/wp-content/uploads/2020/03/inno2.png")
    st.title("Roche Data Collector")
    st.info("This is a software used to filter CMS Part-B,CMS Part-D and OpenPayments data and combine them")
st.header("CMS Part-B Data")
spark=SparkSession.builder.appName('roche_1p_data_collector').getOrCreate()
#Reading the datasets
cms_b=spark.read.csv('Dataset/cms_b.csv',header=True,inferSchema=True)
cms_b_brand_names=spark.read.csv('Dataset/cms_b_unique_hcpcs.csv',header=True,inferSchema=True)
cms_b_brand_names=cms_b_brand_names.drop('HCPCS_Desc') #To resolve streamlit column duplicate error

#Joining CSM-B Data with Brand Name Mappings
fullcms_b=cms_b.join(cms_b_brand_names,'HCPCS_Cd','inner')

#Reading the speciality mapping file
speciality_filter=spark.read.csv('Dataset/speciality_mappings.csv',header=True,inferSchema=True)

#Joining CSM-B Data with Speciality Mappings
fullcms_b=fullcms_b.join(speciality_filter,fullcms_b.Rndrng_NPI==speciality_filter.NPI,'inner')

#Dropping Unwanted columns
fullcms_b=fullcms_b.drop('NPI','Speciality','HCPCS_Cd','Rndrng_Prvdr_Last_Org_Name','Rndrng_Prvdr_First_Name','Rndrng_Prvdr_MI', 'Rndrng_Prvdr_Crdntls','Rndrng_Prvdr_Gndr','Rndrng_Prvdr_Ent_Cd','Rndrng_Prvdr_St1','Rndrng_Prvdr_St2', 'Rndrng_Prvdr_City','Rndrng_Prvdr_State_Abrvtn','Rndrng_Prvdr_State_FIPS','Rndrng_Prvdr_Zip5','Rndrng_Prvdr_RUCA', 'Rndrng_Prvdr_RUCA_Desc','Rndrng_Prvdr_Cntry','Rndrng_Prvdr_Type','Rndrng_Prvdr_Mdcr_Prtcptg_Ind','HCPCS_Desc', 'HCPCS_Drug_Ind','Place_Of_Srvc','Tot_Bene_Day_Srvcs','Avg_Sbmtd_Chrg','Avg_Mdcr_Alowd_Amt','Avg_Mdcr_Pymt_Amt','Avg_Mdcr_Stdzd_Amt','HCPCS_Desc')

#Renaming Columns
fullcms_b=fullcms_b.withColumnRenamed('BRAND','Drug_Name').withColumnRenamed('Tot_Srvcs','Tot_Clms').withColumnRenamed('Rndrng_NPI','NPI')

#Reordering Columns
fullcms_b=fullcms_b.select(['NPI','Drug_Name','Tot_Benes','Tot_Clms'])

#Converting Datatype of Tot_Clms to int
fullcms_b = fullcms_b.withColumn("Tot_Clms",fullcms_b.Tot_Clms.cast("int"))   

# Display in Streamlit
st.dataframe(fullcms_b)
st.write("Shape of Dataset :",(fullcms_b.count(),len(fullcms_b.columns)))

#----------------------------------CMS-D-----------------------------------------------------

st.header('CMS Part-D Data')
cms_d=spark.read.csv('Dataset/cms_d.csv',header=True,inferSchema=True)

#Dropping unwanted columns
cms_d=cms_d.drop('Prscrbr_Last_Org_Name', 'Prscrbr_First_Name', 'Prscrbr_City', 'Prscrbr_State_Abrvtn', 'Prscrbr_State_FIPS', 'Prscrbr_Type', 'Prscrbr_Type_Src','Tot_30day_Fills', 'Tot_Day_Suply', 'Tot_Drug_Cst', 'GE65_Sprsn_Flag', 'GE65_Tot_Clms', 'GE65_Tot_30day_Fills', 'GE65_Tot_Drug_Cst', 'GE65_Tot_Day_Suply', 'GE65_Bene_Sprsn_Flag', 'GE65_Tot_Benes')

#Reading cms-d brand dataset
cms_d_brand_names=spark.read.csv('Dataset/cms_d_gnrc_name.csv',header=True,inferSchema=True)

#Dropping 'GNRC_NAME' to prevent streamlit duplication error
cms_d_brand_names=cms_d_brand_names.drop('Gnrc_Name')

#Converting values to lowercase 
cms_d=cms_d.withColumn('Brnd_Name',lower(cms_d['Brnd_Name']))
cms_d=cms_d.withColumn('Gnrc_Name',lower(cms_d['Gnrc_Name']))

#Combining CMS-D Data with Brand Name Mapping Data
fullcms_d=cms_d.join(cms_d_brand_names,'Brnd_Name','inner') 

#Dropping Duplicated columns
#fullcms_d=fullcms_d.drop('Brnd_Name','Gnrc_Name')

#Reading Speciality Mapping Data
speciality_filter=spark.read.csv('Dataset/speciality_mappings.csv',header=True,inferSchema=True)

#Combining CMS-D Data with Speciality Mapping Data
fullcms_d=fullcms_d.join(speciality_filter,fullcms_d.Prscrbr_NPI==speciality_filter.NPI,'inner')

#Dropping Duplicated columns
fullcms_d=fullcms_d.drop('Brnd_Name','Gnrc_Name','NPI','Speciality')

#Reordering Columns
fullcms_d=fullcms_d.select('Prscrbr_NPI','BRAND','Tot_Benes','Tot_Clms')

#Renaming Columns
fullcms_d=fullcms_d.withColumnRenamed('Prscrbr_NPI','NPI').withColumnRenamed('BRAND','Drug_Name')

#Filling Null values in Tot_Benes with 0
fullcms_d=fullcms_d.fillna(value=0)

#Aggregating based on NPI and Drug_Name
fullcms_d = fullcms_d.groupBy("NPI","Drug_Name").agg(sum("Tot_Benes").alias("Tot_Benes"),sum("Tot_Clms").alias("Tot_Clms"))

st.dataframe(fullcms_d)
st.write("Shape of Dataset :",(fullcms_d.count(),len(fullcms_d.columns)))

#------------------------Appending CMS-B and CMS-D Datasets----------------------------------------
st.header('CMS-B & CMS-D Merged Data')
mergedcms=fullcms_b.union(fullcms_d)

#Aggregating Data
mergedcms = mergedcms.groupBy("NPI","Drug_Name").agg(sum("Tot_Benes").alias("Tot_Benes"),sum("Tot_Clms").alias("Tot_Clms"))

#Converting Data type of Tot_Benes and Tot_Clms Column to int
final_mergedcms = mergedcms.withColumn("Tot_Benes",mergedcms.Tot_Benes.cast("int"))
final_mergedcms = final_mergedcms.withColumn("Tot_Clms",final_mergedcms.Tot_Clms.cast("int"))

#Pivot the Data
final_mergedcms=final_mergedcms.groupBy("NPI").pivot("Drug_Name").sum('Tot_Benes','Tot_Clms')

#Filling NaN Values with 0
final_mergedcms=final_mergedcms.fillna(value=0)
st.dataframe(final_mergedcms)
st.write("Shape of Dataset :",(final_mergedcms.count(),len(final_mergedcms.columns)))

#------------------------------------Open Payments----------------------------------------------------
st.header("Open Payments Data")
#Reading the Data
opendata=spark.read.csv('Dataset/Open_Payments_Physician_Profile/OP_DTL_GNRL_PGYR2022_P01182024.csv',header=True,inferSchema=True)

#Dropping Unwanted columns
opendata=opendata.drop('Change_Type', 'Covered_Recipient_Type', 'Teaching_Hospital_CCN', 'Teaching_Hospital_ID', 'Teaching_Hospital_Name', 'Covered_Recipient_Profile_ID','Covered_Recipient_First_Name', 'Covered_Recipient_Middle_Name', 'Covered_Recipient_Last_Name', 'Covered_Recipient_Name_Suffix', 'Recipient_Primary_Business_Street_Address_Line1', 'Recipient_Primary_Business_Street_Address_Line2', 'Recipient_City', 'Recipient_State', 'Recipient_Zip_Code', 'Recipient_Country', 'Recipient_Province', 'Recipient_Postal_Code', 'Covered_Recipient_Primary_Type_1', 'Covered_Recipient_Primary_Type_2', 'Covered_Recipient_Primary_Type_3', 'Covered_Recipient_Primary_Type_4', 'Covered_Recipient_Primary_Type_5', 'Covered_Recipient_Primary_Type_6','Covered_Recipient_Specialty_3','Covered_Recipient_Specialty_4', 'Covered_Recipient_Specialty_5', 'Covered_Recipient_Specialty_6', 'Covered_Recipient_License_State_code1', 'Covered_Recipient_License_State_code2', 'Covered_Recipient_License_State_code3', 'Covered_Recipient_License_State_code4', 'Covered_Recipient_License_State_code5', 'Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name', 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID', 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State', 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country','City_of_Travel', 'State_of_Travel', 'Country_of_Travel', 'Physician_Ownership_Indicator', 'Third_Party_Payment_Recipient_Indicator', 'Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value', 'Charity_Indicator', 'Third_Party_Equals_Covered_Recipient_Indicator', 'Contextual_Information', 'Delay_in_Publication_Indicator', 'Record_ID', 'Dispute_Status_for_Publication', 'Related_Product_Indicator', 'Covered_or_Noncovered_Indicator_1', 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1', 'Product_Category_or_Therapeutic_Area_1', 'Associated_Drug_or_Biological_NDC_1', 'Associated_Device_or_Medical_Supply_PDI_1', 'Covered_or_Noncovered_Indicator_2', 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2', 'Product_Category_or_Therapeutic_Area_2', 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2', 'Associated_Drug_or_Biological_NDC_2', 'Associated_Device_or_Medical_Supply_PDI_2', 'Covered_or_Noncovered_Indicator_3', 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3', 'Product_Category_or_Therapeutic_Area_3', 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3', 'Associated_Drug_or_Biological_NDC_3', 'Associated_Device_or_Medical_Supply_PDI_3', 'Covered_or_Noncovered_Indicator_4', 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4', 'Product_Category_or_Therapeutic_Area_4', 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4', 'Associated_Drug_or_Biological_NDC_4', 'Associated_Device_or_Medical_Supply_PDI_4', 'Covered_or_Noncovered_Indicator_5', 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5', 'Product_Category_or_Therapeutic_Area_5', 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5', 'Associated_Drug_or_Biological_NDC_5', 'Associated_Device_or_Medical_Supply_PDI_5', 'Program_Year', 'Payment_Publication_Date')

#Dropping rows with Null NPI'S
opendata=opendata.dropna(subset='Covered_Recipient_NPI')

#Converting values to uppercase 
opendata = opendata.withColumn('Covered_Recipient_Specialty_1',upper(opendata['Covered_Recipient_Specialty_1'])).withColumn('Covered_Recipient_Specialty_2',upper(opendata['Covered_Recipient_Specialty_2'])).withColumn('Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',upper(opendata['Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1']))

#Combining Open Payments Data with Speciality Mapping Data
#Filtering based on Speciality (Gastro)
speciality_filter=spark.read.csv('Dataset/speciality_mappings.csv',header=True,inferSchema=True)
opendata=opendata.join(speciality_filter,opendata.Covered_Recipient_NPI==speciality_filter.NPI,'inner')

#Dropping Duplicated Columns
opendata=opendata.drop('NPI','Speciality')

#Combining Open Payments Data with Drug Name Mappings
#Filtering based on Drug_Names or Chemical Names
drug_name_filter=spark.read.csv('Dataset/openpay_drug_mappings.csv',header=True,inferSchema=True)
opendata=opendata.join(drug_name_filter,opendata.Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1==drug_name_filter.Drug_Name,'inner')  
opendata=opendata.drop('Drug_Name','Covered_Recipient_Specialty_2')

#Mapping values of Nature_of_Payment to New Values
opendata = opendata.withColumn("Nature_of_Payment",
                    when(col("Nature_of_Payment_or_Transfer_of_Value") == "Food and Beverage", "FOOD&BEVERAGE")
                    .when(col("Nature_of_Payment_or_Transfer_of_Value") == "Consulting Fee", "CONSULTING")
                    .when(col("Nature_of_Payment_or_Transfer_of_Value") == "Travel and Lodging", "TRAVEL")
                    .when(col("Nature_of_Payment_or_Transfer_of_Value") == "Education", "EDUCATION")
                    .when(col("Nature_of_Payment_or_Transfer_of_Value").rlike("Compensation"), "SPEAKER")
                    .otherwise("OTHERS_GENERAL"))


from pyspark.sql import functions as sf
#Converting Data type of Total_Amount_of_Payment_USDollars to int
opendata_pivot = opendata.withColumn("Total_Amount_of_Payment_USDollars", opendata.Total_Amount_of_Payment_USDollars.cast("int"))

#Aggregating the data based on NPI and Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1
opendata_pivot=opendata_pivot.groupBy("Covered_Recipient_NPI","Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1").pivot("Nature_of_Payment").sum('Total_Amount_of_Payment_USDollars')

#Filling Null values with 0
opendata_pivot=opendata_pivot.fillna(value=0)

#Aggregating and Pivoting
opendata_pivot=opendata_pivot.groupBy("Covered_Recipient_NPI").pivot("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1").sum('CONSULTING','EDUCATION','FOOD&BEVERAGE','OTHERS_GENERAL','SPEAKER','TRAVEL')

#Filling Null values with 0
opendata_pivot=opendata_pivot.fillna(value=0)

st.dataframe(opendata_pivot)
st.write("Shape of the Dataset :",(opendata_pivot.count(),len(opendata_pivot.columns)))

#Renaming NPI Column Name
opendata_pivot=opendata_pivot.withColumnRenamed('Covered_Recipient_NPI','Payment_Recipient_NPI')

#------------------------------CMS & OpenPayments Merged----------------------------------------
st.header('CMS & OpenPayments Merged Data')
cms_open_merged=opendata_pivot.join(final_mergedcms,opendata_pivot.Payment_Recipient_NPI == final_mergedcms.NPI,'outer')    

#Filling Null values with 0 
cms_open_merged=cms_open_merged.fillna(value=0)

st.dataframe(cms_open_merged)
st.write("Shape of the Dataset :",(cms_open_merged.count(),len(cms_open_merged.columns)))
pcmsopen=cms_open_merged.toPandas()
pcmsopen.to_csv('Merged_CMS_Open.csv')