###############################################################################
# Script name: code.py
# Purpose: Salesforce API Connection Test
# Author: Phillip Hungerford
# Date Created: 2022-10-06
# Email: phillip.hungerford@gmail.com
###############################################################################

# =============================================================================
# STEP 1: Make sure you have salesforce installed and load the package
# =============================================================================

# Install package
#!pip install simple_salesforce

# Load package
from simple_salesforce import Salesforce
import operator as op
import pandas as pd 
from datetime import datetime # for creating timestamp
# =============================================================================
# STEP 2: Connect to the Salesforce API
# =============================================================================

# You will need a security token which you can get from the Salesforce Website

def fGetCredentials():
    
    lines = []
    with open("C:/Users/philliph/Documents/repo/resources/credentials-salesforce.txt") as f:
        lines = f.readlines()

    count = 0
    for line in lines:
        count += 1
        #print(f'line {count}: {line}')  
        
    username = lines[0].strip('\n')
    password = lines[1].strip('\n')
    security_token = lines[2].strip('\n')
    
    return(username, password, security_token)

Credentials = fGetCredentials()

# Connect to Salesforce
sf = Salesforce(
username = Credentials[0], 
password = Credentials[1], 
security_token = Credentials[2])

# =============================================================================
# STEP 3: Examine Key Tables of Interest (Contact, Leads, Opportunities)
# =============================================================================
# List Tables you can access through the API (N=627)
df_field = pd.DataFrame(sf.query("SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, DataType FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName IN ('Account', 'Contact', 'Opportunity', 'Lead')")['records'])
df_field['TableName'] = df_field['EntityDefinition'].map(op.itemgetter('QualifiedApiName'))
df_field[['TableName', 'QualifiedApiName', 'DataType']].head()

# Show all unique Tables (n = 4, Account, Contact, Lead, Opportunity)
df_field['TableName'].unique()

# Variables within a table are 'QualifiedApiName' 

# =============================================================================
# STEP 4: Extract Data
# =============================================================================

#------------------------------------------------------------------------------
# LEADS

# Examine variables within the Leads table
LeadVariables = df_field.loc[df_field['TableName']=='Lead']
LeadVariables = LeadVariables[['TableName', 'QualifiedApiName', 'DataType']]
LeadVariables.head()

# Grab leads data from these columns (Put the variables you want in the select statement below)
LeadsRaw = sf.query_all(
    "SELECT Id, Name, Email, MobilePhone, LeadSource, LeadSourceDetail__c, CreatedDate, Brand__c, UTM_Campaign_Tag__c, Status FROM Lead"
)
LeadsRaw = pd.DataFrame(LeadsRaw['records']).drop(columns='attributes')
LeadsRaw.head()

# Tidy dates to local time
LeadsRaw['CreatedDate'] = pd.to_datetime(LeadsRaw['CreatedDate'], utc=True)
LeadsRaw['CreatedDate'] = LeadsRaw['CreatedDate'].dt.tz_convert('Australia/Sydney')
LeadsRaw['Email'] = LeadsRaw['Email'].str.strip().str.lower()

#------------------------------------------------------------------------------
# OPPORTUNITIES 

# Explore variales in opportunities data. 
OpportunityVariables = df_field.loc[df_field['TableName']=='Opportunity']
OpportunityVariables = OpportunityVariables[['TableName', 'QualifiedApiName', 'DataType']]
OpportunityVariables.head()

# Grab opportunity data from these columns (Put the variables you want in the select statement below)
OppsRaw = sf.query_all(
    "SELECT Id, ContactId, ContactFirstName__c, LeadSource, CreatedDate, CloseDate, StageName, Brand__c FROM Opportunity"
)
OppsRaw = pd.DataFrame(OppsRaw['records']).drop(columns='attributes')
OppsRaw.head()

#------------------------------------------------------------------------------
# CONTACTS (Contact Details to merge with Opportunities)

# When a lead turns into an opportunity, a contact is created in the contact table. Merge mobile and email information to the opportunity table as these variables are needed for analysis. 
# examine columns in Account table
table = df_field.loc[df_field['TableName']=='Contact']

# extract contact details
ContactDetails = sf.query_all(
    "SELECT Id, Name, MobilePhone, Email FROM Contact"
)
ContactDetails = pd.DataFrame(ContactDetails['records']).drop(columns='attributes')
ContactDetails = ContactDetails.rename(columns={"Id": "ContactId"})
ContactDetails.head()

# Merge
OppsRawContact = OppsRaw.merge(ContactDetails, left_on='ContactId', right_on='ContactId')

print(LeadsRaw.shape)
print(OppsRaw.shape)
print(OppsRawContact.shape)

# =============================================================================
# SAVE
# =============================================================================
now = datetime.now()
print('Current DateTime:', now)
print('Type:', type(now))
TimeStamp = str(pd.to_datetime(now).date()).replace('-','')

DirectorySave = "C:/Desktop/Data/Raw/"
LeadsRaw.to_csv((DirectorySave + TimeStamp + '-salesforce-leads.csv'), index=False)
OppsRawContact.to_csv((DirectorySave + TimeStamp + '-salesforce-opps.csv'), index=False)

###############################################################################
#################################### END ######################################
###############################################################################