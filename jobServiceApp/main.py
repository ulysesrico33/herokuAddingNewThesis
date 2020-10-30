#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on july 22nd 2020

@author: quart

Important data to develop the code:

-Link to get thesis of any period ( ID changes only):     
https://sjf.scjn.gob.mx/SJFSist/Paginas/DetalleGeneralV2.aspx?ID=#&Clase=DetalleTesisBL&Semanario=0

This service will add new thesis from the website

Info:
1)The last thesis in database for 10th period is 2,021,818
2)So it is good to start from that ID onwards
3)30 secs for every read

"""

import json
from selenium import webdriver
import chromedriver_autoinstaller
from bs4 import BeautifulSoup
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import requests 
import os
#import writeFile as wf

#Global variables

date_null='1000-01-01'
msg_error="Custom Error"
thesis_id=[ 'lblTesisBD','lblInstancia','lblFuente','lblLocMesAño','lblEpoca','lblLocPagina','lblTJ','lblRubro','lblTexto','lblPrecedentes']
thesis_class=['publicacion']
precedentes_list=['francesa','nota']
ls_months=['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre']
thesis_added=False
pathtohere=os.getcwd()

#Chrome configuration for heroku

chrome_options= webdriver.ChromeOptions()
chrome_options.binary_location=os.environ.get("GOOGLE_CHROME_BIN")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")

browser=webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"),chrome_options=chrome_options)


#End of chrome configuration
"""
#Chrome configuration for local machine
chromedriver_autoinstaller.install()
browser=webdriver.Chrome()
"""



def main():
    print('Running program...')
    #The limits in readUrl may vary up to the need of the search
    res=readUrl(1,2022256,2500000)  
    print("Main program is done")
  
  
"""
readUrl

Reads the url from the jury web site
"""

def readUrl(sense,l_bot,l_top):
    
    res=''
    #Can use noTesis as test variable too
    noTesis=0
    strField=''
    
    
    print('Starting process...')
    
    
    #Import JSON file     
    with open(pathtohere+'/jobServiceApp/thesis_json_base.json') as f:
        json_thesis = json.load(f)
    #Onwars for    
    if sense==1:
        for x in range(l_bot,l_top):
            print('Current thesis:',str(x))
            res=prepareThesis(x,json_thesis)
            # "m" means it is a missing space, no thesis found, then stop the program
            if res=='m':
                break
            #wf.appendInfoToFile(pathToHere+'tests/',str(x)+'.json',json.dumps(json_thesis))
            if res!='':
                thesis_added=cassandraBDProcess(res) 
                #thesis_added=True 
                if thesis_added==True:
                    noTesis=noTesis+1
                    print('Thesis ready: ',noTesis, "-ID: ",x)
                    #if noTesis==3:
                    #   break
    #Backwards For             
    else:
        for x in range(l_top,l_bot,-1): 
            print('Current thesis:',str(x))
            res=prepareThesis(x,json_thesis)
            #wf.appendInfoToFile(pathToHere+'tests/',str(x)+'.json',json.dumps(json_thesis))
            if res!='':
                #Upload thsis to Cassandra 
                thesis_added=cassandraBDProcess(res) 
                #thesis_added=True 
                if thesis_added==True:
                    noTesis=noTesis+1
                    print('Thesis ready: ',noTesis, "-ID: ",x)
                    #if noTesis==3:
                    #    break 
                                   
    browser.quit()  
    
    return 'It is all done'

   
          
def cassandraBDProcess(json_thesis):
    
    global thesis_added
    global row

    #Connect to Cassandra
    objCC=CassandraConnection()
    cloud_config= {

        'secure_connect_bundle': pathtohere+'/jobServiceApp/secure-connect-dbquart.zip'
         
    }
    
   
    auth_provider = PlainTextAuthProvider(objCC.cc_user,objCC.cc_pwd)
    #Get values for query
    #Ejemplo : Décima Época
    thesis_added=False
    
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.default_timeout=70
    row=''
    idThesis=json_thesis['id_thesis']
    heading=json_thesis['heading']
    #Check wheter or not the record exists
           
    querySt="select id_thesis from thesis.tbthesis where id_thesis="+str(idThesis)+" and heading='"+heading+"'"
                
    future = session.execute_async(querySt)
    row=future.result()
        
    if row: 
        thesis_added=False
        cluster.shutdown()
    else:
        #Insert Data as JSON
        json_thesis=json.dumps(json_thesis)
        #wf.appendInfoToFile(dirquarttest,str(idThesis)+'.json', json_thesis)                
        insertSt="INSERT INTO thesis.tbthesis JSON '"+json_thesis+"';" 
        future = session.execute_async(insertSt)
        future.result()  
        thesis_added=True
        cluster.shutdown()     
                    
                         
    return thesis_added
     

"""
prepareThesis:
    Reads the url where the service is fetching data from thesis
"""

def prepareThesis(id_thesis,json_thesis): 
    
    result=''
    strIdThesis=str(id_thesis) 
    url="https://sjf.scjn.gob.mx/SJFSist/Paginas/DetalleGeneralV2.aspx?ID="+strIdThesis+"&Clase=DetalleTesisBL&Semanario=0"
    response= requests.get(url)
    status= response.status_code
    if status==200:
        browser.get(url)
        #30 seconds of waiting
        time.sleep(30)
        thesis_html = BeautifulSoup(browser.page_source, 'lxml')
        title=thesis_html.find('title')
        title_text=title.text
        if title_text.strip()!= msg_error:  
            #Clear Json  
            json_thesis['id_thesis']=''
            json_thesis['lst_precedents'].clear()
            json_thesis['thesis_number']=''
            json_thesis['instance']=''
            json_thesis['source']=''
            json_thesis['book_number']=''  
            json_thesis['publication_date']='' 
            json_thesis['dt_publication_date']=''
            json_thesis['period']=''
            json_thesis['page']=''
            json_thesis['jurisprudence_type']=''
            json_thesis['type_of_thesis']=''
            json_thesis['subject']=''
            json_thesis['subject_1']=''
            json_thesis['subject_2']=''
            json_thesis['subject_3']=''
            json_thesis['heading']=''
            json_thesis['text_content']=''
            json_thesis['publication']=''
            json_thesis['multiple_subjects']=''
            
            
            json_thesis['id_thesis']=int(strIdThesis)
            #Fet values from header, and body of thesis
            for obj in thesis_id:  
                field=thesis_html.find(id=obj)
                if field.text != '':   
                    strField=field.text.strip()
                    if obj==thesis_id[0]:
                        json_thesis['thesis_number']=strField
                    if obj==thesis_id[1]:
                        json_thesis['instance']=strField
                    if obj==thesis_id[2]:
                        json_thesis['source']=strField
                    #Special Case    
                    if obj==thesis_id[3]: 
                        json_thesis['book_number']=strField  
                        json_thesis['publication_date']=strField
                        json_thesis['dt_publication_date']=getCompleteDate(strField) 
                                                
                    if obj==thesis_id[4]:
                        json_thesis['period']=strField
                        if strField=='Quinta Época':
                            json_thesis['period_number']=5
                        if strField=='Sexta Época':
                            json_thesis['period_number']=6
                        if strField=='Séptima Época':
                            json_thesis['period_number']=7
                        if strField=='Octava Época':
                            json_thesis['period_number']=8        
                        if strField=='Novena Época':
                            json_thesis['period_number']=9
                        if strField=='Décima Época':
                            json_thesis['period_number']=10       
                    if obj==thesis_id[5]:
                        json_thesis['page']=strField
                    #Special case :
                    #Type of jurispricende: pattern => (Type of thesis () )
                    if obj==thesis_id[6]:
                        strField=strField.replace(')','')
                        chunks=strField.split('(')
                        count=len(chunks)
                        if count==2: 
                            json_thesis['type_of_thesis']=chunks[0]
                            json_thesis['subject']=chunks[1]
                            subjectChunks=chunks[1].strip()
                            if subjectChunks.find(',')!=-1:
                                subjectChunks=subjectChunks.split(',')
                                if len(subjectChunks)>1: 
                                    if len(subjectChunks)==3:
                                        json_thesis['subject_3']=subjectChunks[2]
                                    json_thesis['subject_1']=subjectChunks[0]
                                    json_thesis['subject_2']=subjectChunks[1]
                                    json_thesis['multiple_subjects']=True
                                else:
                                    json_thesis['multiple_subjects']=False
                                            
                        if count==3:
                            json_thesis['jurisprudence_type']=chunks[0]
                            json_thesis['type_of_thesis']=chunks[1]
                            json_thesis['subject']=chunks[2]
                            subjectChunks=chunks[2].strip()
                            if subjectChunks.find(',')!=-1:
                                subjectChunks=subjectChunks.split(',')
                                if len(subjectChunks)>1: 
                                    if len(subjectChunks)==3:
                                        json_thesis['subject_3']=subjectChunks[2]
                                    json_thesis['subject_1']=subjectChunks[0]
                                    json_thesis['subject_2']=subjectChunks[1]
                                    json_thesis['multiple_subjects']=True  
                                else:
                                    json_thesis['multiple_subjects']=False    

                    if obj==thesis_id[7]:
                        json_thesis['heading']=strField.replace("'",',')
                    if obj==thesis_id[8]:
                        json_thesis['text_content']=strField.replace("'",',') 
                    if obj==thesis_id[9]:  
                        children=thesis_html.find_all(id=obj)
                        for child in children:
                            for p in precedentes_list:   
                                preced=child.find_all(class_=p)
                                for ele in preced:
                                    if ele.text!='':
                                        strValue=ele.text.strip()
                                        json_thesis['lst_precedents'].append(strValue.replace("'",','))

                
            for obj in thesis_class:
                field=thesis_html.find(class_=obj)
                if field.text != '':   
                    strField=field.text.strip()
                    if obj==thesis_class[0]:
                        json_thesis['publication']=strField
   
        thesis_html=''
        result=json_thesis
        
        #For some reason I can not write the else statemente for the if title different for Custom
        #Error, so I must set this condition to know if that ID doesn't have a thesis
        if title_text.strip()== msg_error:
            print('Missing thesis at ID:',strIdThesis)
            result='m'
                  
    else:
        print('Server failure:',strIdThesis)
        result=''
        
    return  result



def getCompleteDate(pub_date):
    pub_date=pub_date.strip()
    if pub_date!='':
        if pub_date.find(':')!=-1:
            chunks=pub_date.split(':')
            date_chunk=str(chunks[1].strip())
            data=date_chunk.split(' ')
            month=str(data[3].strip())
            day=str(data[1].strip())
            year=str(data[5].strip())
        elif pub_date.find(' ')!=-1:
            # Day month year and hour
            chunks=pub_date.split(' ')
            #day=str(chunks[1].strip())
            month=str(chunks[0].strip())
            year=str(chunks[2].strip()) 
            day=''    
        month_lower=month.lower()
        for item in ls_months:
            if month_lower==item:
                month=str(ls_months.index(item)+1)
                if len(month)==1:
                    month='0'+month
                    break
        if day=='':
            day='01'        
                
    completeDate=year+'-'+month+'-'+day                   
    return completeDate



def getIDLimit(sense,l_bot,l_top,period):
    
    if period==10:
        strperiod='Décima Época'
  
    #Onwars for    
    if(sense==1):
        for x in range(l_bot,l_top):
            res=searchInUrl(x,strperiod)
            if res==1:
                break
                
    #Backwards For             
    if(sense==2):
        for x in range(l_top,l_bot,-1): 
            res=searchInUrl(x,strperiod)
            if res==1:
                break
           
            
def searchInUrl(x,strperiod):
    strIdThesis=str(x) 
    url="https://sjf.scjn.gob.mx/SJFSist/Paginas/DetalleGeneralV2.aspx?ID="+strIdThesis+"&Clase=DetalleTesisBL&Semanario=0"
    response= requests.get(url)
    status= response.status_code
    if status==200:
        print('ID:',str(x))
        browser.get(url)
        time.sleep(1)
        thesis_html = BeautifulSoup(browser.page_source, 'lxml')
        title=thesis_html.find('title')
        title_text=title.text
        if title_text.strip() != msg_error:
            thesis_period=thesis_html.find(id='lblEpoca')
            data=thesis_period.text
            if data!='':
                if data.strip()=='Décima Época':
                    print('ID for ',strperiod,' found in :',strIdThesis)
                    return 1
               
                    

    
class CassandraConnection():
    cc_user='quartadmin'
    cc_pwd='P@ssw0rd33'
    
  
        
if __name__=='__main__':
    main()        
