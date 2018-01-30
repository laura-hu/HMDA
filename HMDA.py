#########################
###       HMDA        ###
#########################
# Author: Eliot Abrams and Laura Hu
# Purpose: Parse HMDA Data
 
# TS files contain information on the bank / mortgage company reporting the application
# LAR files contain information on the mortgage and home equity loan applications to specified bank / mortgage companies
 
 
#########################
###       Setup       ###
#########################
import os
import pandas as pd
import gzip
import multiprocessing as mp # For parallelizing
 
 
#########################
###     Functions     ###
#########################

def get_all_files(root_path):
    file_list = []
    for root, _, filenames in os.walk(root_path):
        for filename in filenames:
            file_list.append(os.path.join(root, filename))
    return file_list
 
def unzip(read_path, save_path):
	with gzip.open(read_path, 'rb') as f_in, open(save_path, 'wb') as f_out:
		shutil.copyfileobj(f_in, f_out)
	return

def read_ts(file):
    fh = open(file, 'r')
    table = pd.DataFrame()
    for line in fh:
        date = line[0:4]
        agency_code = line[4]
        respondent_id = line[5:15]
        respondent_mail_name = line[15:45]
        respondent_mail_address = line[45:85]
        respondent_mail_city = line[85:110]
        respondent_mail_state = line[110:112]
        respondent_mail_zip = line[112:122]
        if ('2000' in file_path or '2001' in file_path or '2002' in file_path
            or '2003' in file_path):
            parent_name = ''
            parent_address = ''
            parent_city = ''
            parent_state = ''
            parent_zip = ''
            edit_status = ''
            tax_id = line[123:133]			
        else:
            parent_name = line[122:152]
            parent_address = line[152:192]
            parent_city = line[192:217]
            parent_state = line[217:219]
            parent_zip = line[219:229]
            edit_status = line[229:230]
            tax_id = line[230:240]
        table.append([date, agency_code, respondent_id, 
			respondent_mail_name, respondent_mail_address, 
		    respondent_mail_city, respondent_mail_state, 
			respondent_mail_zip, parent_name, parent_address, 
			parent_city, parent_state, parent_zip, edit_status, 
			tax_id])
    return table

def read_lar(file):
    fh = open(file, 'r')
    table = pd.DataFrame(data=None)
    for line in fh:
        date = line[0:4]
        respondent_id = line[4:14]
        agency_code = line[14]
        loan_type = line[15]
        loan_purpose = line[16]
        occupancy = line[17]
        loan_amount = line[18:23]
        action_taken = line[23]
        msa_of_property = line[24:29]
        state_code = line[29:31]
        county_code = line[31:34]
        census_tract_no = line[34:41]
        applicant_sex = line[41]
        coapplicant_sex = line[42]
        applicant_income = line[43:47]
        purchaser_type = line[47]
        denial_reason_1 = line[48]
        denial_reason_2 = line[49]
        lars_yearly_data.append([date, respondent_id, agency_code, 
            loan_type, loan_purpose, occupancy, loan_amount, 
            action_taken, msa_of_property, state_code, county_code, 
            census_tract_no, applicant_sex, coapplicant_sex, 
            applicant_income, purchaser_type, denial_reason_1, 
            denial_reason_2])
    return table
 
#########################
###       Main        ###
#########################
 
### Unzip files
root_path = "C:\\Users\Laura\Documents\HMDA"
file_list = get_all_files(root_path)
zips = [path for path in file_list if '.zip' in path]
#print (zips)
#save_path = "C:\\Users\Laura\Documents\HMDA\\Unzipped"
#for path in zips:
 #   unzip(path, save_path)
 
### Import files
# Doesn't seem to split the columns

ts_data = pd.DataFrame(columns=["Date", "Agency Code", 
	"Respondent ID", "Respondent Mail Name", "Respondent Mail Address", 
	"Respondent Mail City", "Respondent Mail State", "Respondent Mail Zip", 
	"Parent Name", "Parent Address", "Parent City", "Parent State", 
	"Parent Zip", "Edit Status", "Tax ID"])
lars_data = pd.DataFrame(columns=["Date", "Respondent ID", 
	"Agency Code", "Type of Loan", "Purpose of Loan", "Occupancy", 
	"Amount of Loan", "Type of Action Taken", "MSA of Property", "State Code", 
	"County Code", "Census Tract Number", "Applicant Sex", "Co-applicant Sex", 
	"Applicant Income", "Type of Purchaser", "Denial Reason 1", 
	"Denial Reason 2", "Home Equity?"])
for file_path in file_list:
	data = pd.read_table(file_path) 

# I think the columns are defined by the lengths specified in the file layout documentation

	with open(file_path, 'r') as fh:
		if "ts" in file_path:
		    ts_yearly_data = read_ts(file_path)
		    ts_data.append(ts_yearly_data)
		elif "lar" in file_path:
		    lars_yearly_data = read_lars(file_path)
		    lars_data.append(lars_yearly_data)

### Clean files
# Replace codes with the text description
# Ensure numbers are in as floats
# Ideally mark if the LAR entry is a home equity loan

# Replacing file codes with text description 
ts_data = ts_data.replace({'Agency Code' : { '1' : "OCC", '2': "FRS", 
	'3': "FDIC",'4': "OTS", '5': "NCUA", '7': "HUD", '9': "CFPB"}})
ts_data = ts_data.replace({'Edit Status' : 
	{ ' ': "RECORD HAS NO EDIT FAILURES", '5': "VALIDITY EDIT FAILURE(S)", 
	'6': "QUALITY EDIT FAILURE(S)", '7': "VALIDITY AND QUALITY EDIT FAILURES"}})
	
lars_data = lars_data.replace({'Agency Code' : { '1' : "OCC", '2': "FRS", 
	'3': "FDIC",'4': "OTS", '5': "NCUA", '7': "HUD", '9': "CFPB"}})
	
ts_data['Date'] = pd.to_datetime(ts_data['Date'])

for row in lars_data.itertuples():
    if row["Purpose of Loan"] == "3":
        row["Home Equity?"] = 1
    else:
        row["Home Equity?"] = 0

#Standardizing respondent ID and zip codes
ts_data['Respondent Mail Zip'] = ts_data['Respondent Mail Zip'].str[0:5]
ts_data['Parent Zip'] = ts_data['Parent Zip'].str[0:5]
ts_data['Respondent ID'] = ts_data['Respondent ID'].str.replace('-', '0')

# Capitalizing text objects
ts_data['Respondent Mail Name'] = ts_data['Respondent Mail Name'].str.upper()
ts_data['Respondent Mail Address'] = ts_data['Respondent Mail Address'].str.upper()
ts_data['Respondent Mail City'] = ts_data['Respondent Mail City'].str.upper()
ts_data['Parent Name'] = ts_data['Parent Name'].str.upper()
ts_data['Parent Address'] = ts_data['Parent Address'].str.upper()
ts_data['Parent City'] = ts_data['Parent City'].str.upper()

print (ts_data)

### Merge files
# Merge the TS and LAR data
# Ideally add 1) zip code and 2) census data too
 
### Save