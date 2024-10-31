def currsmok_to_sql(smoking_codes, db_server, db_name, schema):  

    import pandas as pd
    pd.options.mode.chained_assignment = None
    import re
    import numpy as np
    
    vasqip_list = pd.DataFrame(vasqip_list)
    currsmok_hf = pd.DataFrame(currsmok_hf)
    currsmok_tiu = pd.DataFrame(currsmok_tiu)

    #vasqip_list contains the patient information such as scrssn, surgerydatetime
    #currsmok_hf contains the healthfactorcategory, patientsid, healthfactortype and can be linked to vasqip_list for scrssn and surgerydatetime
    #currsmok_tiu contains the TIU documents and can be linked with vasqip_list for scrssn and surgerydatetime

    #removing all unneccessay phrases
    pattern_list = ['Quitting tobacco use - B23','Quit smoking clinic','handouts are available for some of these issues quitting','could be caused by things like high blood pressure or smoking','no smoking','visiting hours Smoking policy','Discourage smoking Discourage excessive intake','stop smoking','educat', 'FOR AT LEAST 24 HOURS AVOID THE FOLLOWING SMOKING CHEWING','To refrain from smoking at least 8 hours prior to surgery','Do not drink alcohol or smoke','caesation','no etoh or tobacco','do not smoke','FOLLOW UP RETURN APPOINTMENT','RESEARCH']
    pattern_list = "|".join(pattern_list)
    TIU = TIU[~TIU['ReportText'].str.contains(pattern_list,case=False)]


    #snippet extraction of tokens surrounding target words
    def extract_snippet(text):
        pattern = r'((?:\b\w+\b\s*){0,20})(?:\b(non-smoker|ex-smoker|smoking|smoke|smoker|cig|cigarette|tob|tobacco|PPD)\b)((?:\s*\S+\s*){0,15})'
        matches = re.findall(pattern, text, re.IGNORECASE)
        snippets = []

        for match in matches:
            snippet=''.join(match)
            snippets.append(snippet)
        return snippets

    TIU['snippet']=TIU['ReportText'].apply(lambda x: ', '.join(extract_snippet(x)))


    #text preprocessing for snippet2
    def preprocess_text(text):
        text = text.lower()
        text = re.sub(r'\bN\b|(Current)............: [NO]|not|non|never|denied|deny|denies|\bnot(?:\s+a)?\s+current\b|\( -\)','no',text,flags=re.IGNORECASE)
        text = re.sub(r'active tobacco use:?\b|\bactive smoker:\b|\bcurrent tobacco user:\b|\btobacco\b|\bsmoking\b|\bsmoker\b|\btobacco use\b|\bof smoke\b|\bsmoked\b|\btob\b|\bActive , Smoking\b|do you currently smoke smokeless smoke pipes','smoke',text,flags=re.IGNORECASE)
        text = re.sub(r'\b(?:denies|denied|deny)\s+(?:current|active)\b','denied',text,flags=re.IGNORECASE)
        text = re.sub(r'smoke: no|smoke free','no smoke',text,flags=re.IGNORECASE)

        text = re.sub(r'No Hypertension|no diabetes|no family history of CAD|no history of alcohol abuse|no dyslipidemia|no hyperlipidemia','',text,flags=re.IGNORECASE)

        text = re.sub(r'\bY\b','yes',text,flags=re.IGNORECASE)

        text = re.sub(r'\s+',' ',text)

        text = re.sub(r'\[x\]No|smoke (X)','non user',text,flags=re.IGNORECASE)
        text = re.sub(r'\[\]No|smoke (0)|no interested in quitting','current user',text,flags=re.IGNORECASE)

        text = re.sub(r'\[\]Yes','',text,flags=re.IGNORECASE)
        text = re.sub(r'\[\]No','',text,flags=re.IGNORECASE)
        text = re.sub(r'\bexercise and to quit smoking and refrain from smoking, alcohol and illicit drug use\b|\bcurrent medications\b','', text, flags=re.IGNORECASE)  

        text = re.sub(r'\balcohol\b|\bdrinking\b|\bdrink\b|\billicit\b|\alcohol use\b','etoh',text,flags=re.IGNORECASE)
        text = re.sub(r'\b(?:illicit\s+)?substances?\b|\billicit drug\b','drug',text,flags=re.IGNORECASE)

        text = re.sub(r'\b(?:etoh|etoh\s+use):\s+(\w+)\b|\b(?:drug|drug\s+use):\s+(\w+)\b|review.*?partake','',text,flags=re.IGNORECASE)
        text = re.sub(r'\babuse\b|\bchronic\b|\bin\b|\bearly\b|\buse\b|\bimportance of no smoke\b|\bdo no smoke\b|\b(?:no\shistory\s)?(?:no\s)?current\ drug\ use\b|current status|\bwilling to quit?\b|medication to quit smoke','',text,flags=re.IGNORECASE)
        text = re.sub(r'[^\w\s]',' ',text)

        text = re.sub(r'\bquit\w+\b|\beven if quit:\b', 'smoke quit',text,flags=re.IGNORECASE)

        text = re.sub(r'\b(cigarettes?|cigars?)\b','',text,flags=re.IGNORECASE)

        text = re.sub(r'\bhx\b|\bhistory of\b','history',text,flags=re.IGNORECASE)

        text = re.sub(r'\bhe\b|\bshe\b|\bpatient\b|\bpt\b','',text,flags=re.IGNORECASE)
        text = re.sub(r'\bcurrent drug\b|\bformer etoh\b|\betoh quit\b|\betoh no\b|\bquit etoh\b|\bcocaine\s*quit\b|\bcurrent etoh\b|exercise and to quit smoke and refrain from smoke','',text,flags=re.IGNORECASE)

        text = re.sub(r'\bcurrent no\b|\bno current\b|\bno currently\b|\bno any current\b|\byes xno\b|\byes x no\b','no',text,flags=re.IGNORECASE)
        
        text = re.sub(r'\s+',' ',text)   
        text = re.sub(r'chewable.*?lozenges|chewable.*?oral|chewable.*?lozenge|lozenge.*?daily|chewable.*?smoke|(lozenge|lozenges).*?smoke','',text,flags=re.IGNORECASE)
        return text

    TIU['snippet2']=TIU['snippet'].apply(preprocess_text)

    #text preprocessing for snippet3
    def smoke(text):
        text = text.lower()
        text = re.sub(r'\btobacco\b|\bsmoking\b|\bsmoker\b|\btobacco use\b|\bof smoke\b|\bsmokes\b|\btob\b|\bActive , Smoking\b', 'smoke', text, flags=re.IGNORECASE)
        text = re.sub(r'\bdenied\b|\bdeny\b','denies',text,flags=re.IGNORECASE)
        return text

    TIU['snippet3'] = TIU['snippet'].apply(smoke)

    #using preprocessed snippet 3 to do an additional snippet extraction to directly detect smoke or smoke use statuses for those that are explicitly stated
    def extract_status(text):
        pattern = r'smoke:\s*(\w+(?:\s+\w+){0,5})|smoke-\s*(\w+(?:\s+\w+){0,5})|smoke use:\s*(\w+(?:\s+\w+){0,5})'
        match = re.search(pattern,text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        else:
            return 'blank'
       

    TIU['status2'] = TIU['snippet3'].apply(extract_status)
    TIU['status2'] = TIU['status2'].astype(str) 

    print('TIU notes preprocessing complete...')

    #regular expression search to categorize snippet 2
    def label_currsmok(row):
        if pd.notna(row['snippet2']):               
            if re.search(r'used to|none|denies|never|does not smoke|nonsmoker|no|neg|smoked|quit|na|previous|prior|former',row['status2'],flags=re.IGNORECASE):
                return 'NON SMOKER'
            elif re.search(r'current|smoke|daily|yes|week|days|day|\bcurrent\s+smoke\b(?!\s*no\b)|ongoing smoke',row['status2'],flags=re.IGNORECASE):
                return 'CURRENT SMOKER'
            elif re.search(r'smoke\s+(?:dependence|disorder)\s+remission|\b(?:former|remote|past|ex|previous|prior)\s*(?:\w+\s+)?smoke\b|\b(?:smoke\s+(?:previous|former|prior|remote|ex|past))\b',row['snippet2'],flags=re.IGNORECASE):
                return 'FORMER SMOKER'
            elif re.search(r'smoke non user|no history (?:sof)?\s?smoke|current smoke\? no|no smoke', row['snippet2'], flags=re.IGNORECASE):
                return 'NON SMOKER'
            elif re.search(r'smoke current user|\b(?:active|current|recurrent|ongoing|continue to)\s*(?:\w+\s+)?smoke\b|\b(?:smoke\s+(?:active|recurrent|yes|current))\b|still smoke',row['snippet2'], flags=re.IGNORECASE):
                return 'CURRENT SMOKER'
            elif re.search(r'\b(?:to|will|discussed|about|goal|interest(?:ed)?\ in|if|must|can|no\ intention|plans\ on)\s(?:smoke\s)?quit\b',row['snippet2'],flags=re.IGNORECASE):
                return 'CURRENT SMOKER'
            elif re.search(r'remote (smoke|history|minimal smoke)|\b(?!(?:must|if|to|can|will|had|of)\b(?:\s+\b(?:must|if|to|can|wil|had)\b\s+)etoh\s+quit\b)(?=.*\bquit\b)(?!.*\betoh\s+quit\b)',row['snippet2'],flags=re.IGNORECASE):
                return 'QUIT SMOKER'
            else:
                return 'UNKNOWN'
        else:
            return 'UNKNOWN'

    TIU['status']=TIU.apply(label_currsmok, axis=1)

    #datediff for TIU
    TIU['vasqipDateTime']=pd.to_datetime(TIU['vasqipDateTime'])
    TIU['ReferenceDateTime']=pd.to_datetime(TIU['ReferenceDateTime'])
    TIU['datediff_tiu']=(TIU['vasqipDateTime']-TIU['ReferenceDateTime']).dt.days

    #remove rows with blank snippets and unknowns
    TIU_filtered = TIU[TIU['snippet'].str.strip()!='']
    TIU_filtered = TIU_filtered[TIU_filtered['status']!='UNKNOWN']

    #choose the document closest and prior to surgery date, if none is prior choose closest to after surgery date

    groups=TIU_filtered.groupby('surgerySID')
    def filter_group(group):
        group=group.sort_values(by='datediff_tiu', ascending=True)
        min_nonnegative_datediff = group[(group['datediff_tiu'] > 0)&(group['status']!='UNKNOWN')].head(1)  #>0
        if min_nonnegative_datediff.empty:
            #return group.tail(1)
            return min_nonnegative_datediff

        elif group['status'].eq('UNKNOWN').all():
            return group.tail(1)
        else:
            return group.head(1)

       

 

    result_df = groups.apply(filter_group).reset_index(drop=True)
    #change categorized status to binary variable

    def label_smoker(status):
        if (status=='CURRENT SMOKER'):
            return 1
        elif (status=='UNKNOWN'):
            return ''
        else:
            return 0

       

    result_df['TIU_binary']=result_df['status'].apply(lambda x: label_smoker(x))
    TIU = result_df[['surgerySID','surgeryDateTime','TIU_binary','datediff_tiu']]
    print('TIU coding complete...merging all sources...')

    #confirming type values for merging
    healthfactor.loc[:, 'surgerySID'] = (healthfactor['surgerySID'].astype('int64'))
    TIU.loc[:, 'surgerySID'] = (TIU['surgerySID'].astype('int64'))
    healthfactor['surgeryDateTime'] = (healthfactor['surgeryDateTime'].astype('datetime64[ns]'))
    TIU['surgeryDateTime'] = (TIU['surgeryDateTime'].astype('datetime64[ns]'))

    #merging TIU and HealthFactors
    combined = pd.merge(surgeries, healthfactor, on=['surgerySID','surgeryDateTime'], how='left')
    combined = pd.merge(combined, TIU, on=['surgerySID','surgeryDateTime'], how='left')
    combined = combined[['surgerySID','surgeryDateTime','datediff_hf','datediff_tiu','HF_binary','TIU_binary','scrssn']]

    #determing whether health factors or TIU has precendence using datediff columns
    def determine(row):
        def get_numeric_value(column_name):
            return row[column_name] if pd.notna(row[column_name]) else np.inf

        has_valid_diff = any(0 < val < 365 for val in [get_numeric_value(col) for col in ['datediff_hf','datediff_tiu']])      
        if has_valid_diff:
            min_column = min(['datediff_hf','datediff_tiu'], key=lambda col: get_numeric_value(col))
        else:
            min_column = min(['datediff_hf','datediff_tiu'], key=lambda col: abs(get_numeric_value(col)))

        if min_column == 'datediff_hf':
            return row['HF_binary']
        elif min_column == 'datediff_tiu':
            return row['TIU_binary']


    combined = combined.dropna(subset = ['HF_binary','TIU_binary'], how='all')       
    combined['smokingstatus'] = combined.apply(lambda row: determine(row),axis=1)

    print('All sources merged...complete!')

    combined.rename(columns={'smokingstatus': 'CURRSMOK'}, inplace=True)

    currsmok = combined[['scrssn','surgeryDateTime','CURRSMOK']]
    currsmok = pd.merge(surgeries, currsmok, on=['scrssn','surgeryDateTime'], how='left')
    currsmok['CURRSMOK'] = currsmok['CURRSMOK'].fillna(0)

   
    with engine.connect() as conn:

        currsmok.to_sql('CURRSMOK', conn, schema=schema, if_exists='replace', index=False,

                           dtype={"scrssn": sql.types.NVARCHAR(length=9)})

 

 

if __name__ == "__main__":

 

    import argparse
    import pandas as pd
    import re
    import numpy as np
    import sqlalchemy as sql
    from sqlalchemy import text
    import os

 
    parser = argparse.ArgumentParser()

    parser.add_argument('-p','--smoking_codes', type =str, required = True,

                        help = 'path to healthfactors smoking codes')

    parser.add_argument('-s','--server', type = str, required = True,

                        help = 'SQL server for this project')

    parser.add_argument('-d','--database', type = str, required = True,

                        help = 'SQL database where the cardiac VASQIP data lives')

    parser.add_argument('-a','--schema', type = str, required = True,

                        help = 'SQL schema used for this cohort of surgeries')

 

    args = parser.parse_args()

 

    smoking_codes = pd.read_csv(args.smoking_codes)
    db_server = args.server
    db_name = args.database
    schema = args.schema
    currsmok_to_sql(smoking_codes, db_server, db_name, schema)

 