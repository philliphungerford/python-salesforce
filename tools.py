###############################################################################
# Script name: tools.py
# Purpose: Download data from Salesforce 
# Author: Phillip Hungerford
# Date Created: 2022-10-06
# Email: phillip.hungerford@gmail.com
###############################################################################

# Load package
from simple_salesforce import Salesforce
import pandas as pd 

def fCreateFY(leads, date_var):
    '''
    Creates financial year from a date variable
    '''
    leads['Month'] = pd.to_datetime(leads[date_var]).dt.month_name()
    leads['Year'] = leads[date_var].dt.year
    leads['AsQuarter'] = leads[date_var].dt.to_period('Q-JUN')
    leads['FinancialYear'] = leads['AsQuarter'].dt.qyear
    leads['YearMonth'] = leads['FinancialYear'].astype(str) + leads['Month'].astype(str)
    return(leads)

def fExtractSalesforceLeads(username, password, security_token):
    '''
    Gets all salesforce leads
    '''
    # connect to salesforce
    sf = Salesforce(
    username=username, 
    password=password, 
    security_token=security_token)
    
    LeadsRaw = sf.query_all(
    "SELECT Id, Name, Email, MobilePhone, LeadSource, LeadSourceDetail__c, CreatedDate, Brand__c FROM Lead")

    LeadsRaw = pd.DataFrame(LeadsRaw['records']).drop(columns='attributes')
    
    # CLEAN LEADS
    LeadsRaw['CreatedDate'] = pd.to_datetime(LeadsRaw['CreatedDate'], utc=True)
    LeadsRaw['CreatedDate'] = LeadsRaw['CreatedDate'].dt.tz_convert('Australia/Sydney')
    LeadsRaw['Email'] = LeadsRaw['Email'].str.strip().str.lower()

    # 4. Fill missings for lead source detail
    LeadsRaw['LeadSourceDetail__c'] = LeadsRaw['LeadSourceDetail__c'].fillna(LeadsRaw['LeadSource'])

    # 6. Add date details
    LeadsClean = fCreateFY(LeadsRaw, 'CreatedDate')

    return(LeadsClean)

def fExtractSalesforceOpportunities(username, password, security_token):
    '''
    Gets all salesforce opportunities
    '''
    # connect to salesforce
    sf = Salesforce(
    username=username, 
    password=password, 
    security_token=security_token)
    
    # download opps data
    OppsRaw = sf.query_all(
        "SELECT Id, ContactId, ContactFirstName__c, LeadSource, LeadSourceDetail__c, CreatedDate, CloseDate, StageName, Brand__c FROM Opportunity")
    OppsRaw = pd.DataFrame(OppsRaw['records']).drop(columns='attributes')
    
    # extract contact details
    ContactDetails = sf.query_all("SELECT Id, Name, MobilePhone, Email FROM Contact")
    ContactDetails = pd.DataFrame(ContactDetails['records']).drop(columns='attributes')
    ContactDetails = ContactDetails.rename(columns={"Id": "ContactId"})
    
    OppsRaw = OppsRaw.merge(ContactDetails, left_on='ContactId', right_on='ContactId')
    
    # CLEAN OPPS
    OppsRaw['CreatedDate'] = pd.to_datetime(OppsRaw['CreatedDate'], utc=True)
    OppsRaw['CreatedDate'] = OppsRaw['CreatedDate'].dt.tz_convert('Australia/Sydney')

    OppsRaw['CloseDate'] = pd.to_datetime(OppsRaw['CloseDate'], utc=True)
    OppsRaw['CloseDate'] = OppsRaw['CloseDate'].dt.tz_convert('Australia/Sydney')

    OppsRaw['Email'] = OppsRaw['Email'].str.strip().str.lower()
    OppsRaw['LeadSourceDetail__c'] = OppsRaw['LeadSourceDetail__c'].fillna(OppsRaw['LeadSource'])

    # 4. Fill missings for lead source detail
    OppsRaw['LeadSourceDetail__c'] = OppsRaw['LeadSourceDetail__c'].fillna(OppsRaw['LeadSource'])

    # 6. Add date details
    OppsClean = fCreateFY(OppsRaw, 'CreatedDate')
    
    return(OppsClean)

###############################################################################
#################################### END ######################################
###############################################################################