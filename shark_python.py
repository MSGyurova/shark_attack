#!/usr/bin/env python
# coding: utf-8

# In[11]:


import csv 
from datetime import datetime, timedelta
import pyodbc 


# In[12]:


conn = pyodbc.connect('DSN=kubricksql;UID=de14;PWD=password')
cur = conn.cursor()


# In[13]:


sharkfile = r'c:\data\GSAF5.csv'


# In[14]:


attack_dates = []
isfatal = []
case_number = []
country = []
activity = []
age = []
gender = []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader:
        attack_dates.append(row['Date'])
        case_number.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[15]:


data =  zip(attack_dates, case_number, country, activity, age, gender, isfatal)


# In[16]:


cur.execute('truncate table mirela.shark')


# In[17]:


q = 'INSERT INTO mirela.shark (attack_date, case_number, country, activity, age, gender, isfatal) VALUES (?, ?, ?, ?, ?, ?, ?)'


# In[19]:


for d in data:
        try:
            cur.execute(q,d)
            conn.commit()
        except:
            conn.rollback()


# In[ ]:




