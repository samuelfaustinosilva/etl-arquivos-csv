#!/usr/bin/env python
# coding: utf-8

# ### Bibliotecas

# In[214]:


get_ipython().system('pip install google-auth')


# In[260]:


get_ipython().system('pip install pandas-gbq')


# In[311]:


get_ipython().system('pip install --upgrade pandas-gbq')


# In[312]:


import pandas as pd
from IPython.display import Image
from google.oauth2  import service_account


# ### Extração de dados

# In[313]:


contratos = pd.read_csv('tabela_contratos.csv')


# In[314]:


contratos


# In[315]:


datas = pd.read_csv('tabela_datas.csv')


# In[316]:


datas


# In[317]:


empresas = pd.read_csv('tabela_empresas.csv')


# In[318]:


empresas


# ### Transformação dos dados

# In[319]:


Image('https://upload.wikimedia.org/wikipedia/commons/thumb/9/9d/SQL_Joins.svg/1024px-SQL_Joins.svg.png')


# In[320]:


contratos_mod = contratos.merge(empresas, 
                               left_on='fk_empresa_contratada',
                               right_on='id_empresa',
                               how='left')


# In[321]:


contratos_mod


# In[322]:


contratos_mod.drop(columns=['fk_empresa_contratada','id_empresa'], inplace=True)


# In[323]:


contratos_mod


# In[324]:


contratos_final = contratos_mod.merge(datas,
                                     left_on='inicio_vigencia',
                                     right_on='id_data',
                                     how='left')


# In[325]:


contratos_final


# In[326]:


contratos_final.drop(columns=['inicio_vigencia','id_data'], inplace=True)


# In[327]:


contratos_final


# In[328]:


contratos_final.rename(columns={'data': 'data_inicio_vigencia'}, inplace=True)


# In[329]:


contratos_final


# In[330]:


contratos_finalissima = contratos_final.merge(datas,
                                             left_on='termino_vigencia',
                                             right_on= 'id_data',
                                             how='left')


# In[331]:


contratos_finalissima


# In[332]:


contratos_finalissima.rename(columns={'data':'data_termino_vigencia'}, inplace=True)


# In[333]:


contratos_finalissima


# In[376]:


contratos_finalissima.drop(columns=['termino_vigencia','id_data'], inplace=True)


# In[377]:


contratos_finalissima


# In[378]:


contratos_finalissima.count()


# In[379]:


contratos_finalissima.dtypes


# In[380]:


contratos_finalissima.data_inicio_vigencia = pd.to_datetime(contratos_finalissima.data_inicio_vigencia,
                                                           format='%d/%m/%Y').dt.date


# In[381]:


contratos_finalissima.data_termino_vigencia = pd.to_datetime(contratos_finalissima.data_termino_vigencia,
                                                            format='%d/%m/%Y').dt.date


# In[382]:


for i in contratos_finalissima.data_termino_vigencia:
    print(i)
    print(pd.to_datetime(i))


# In[383]:


contratos_finalissima.data_termino_vigencia = contratos_finalissima.data_termino_vigencia.str.replace('31/09/2017','30/09/2017')


# In[384]:


contratos_finalissima.data_termino_vigencia = pd.to_datetime(contratos_finalissima.data_termino_vigencia,
                                                            format='%d/%m/%Y').dt.date


# In[385]:


contratos_finalissima.head(5)


# In[387]:


contratos_finalissima['tempo_contrato'] = (contratos_finalissima['data_termino_vigencia'] - 
                                           contratos_finalissima['data_inicio_vigencia'])


# In[373]:


contratos_finalissima


# In[343]:


contratos_finalissima.nome_contrato.value_counts()


# In[388]:


contratos_finalissima[contratos_finalissima.nome_contrato == '004/16']


# In[389]:


contratos_finalissima.tempo_contrato.value_counts()


# In[390]:


contratos_finalissima = contratos_finalissima[contratos_finalissima.tempo_contrato > 0]


# In[391]:


contratos_finalissima.reset_index(drop=True, inplace=True)


# In[392]:


contratos_finalissima.tail()


# ### Carregamento dos dados

# In[349]:


credentials = service_account.Credentials.from_service_account_file(filename='GQB.json',
                                                                   scopes=["https://www.googleapis.com/auth/cloud-platform"])


# contratos_finalissima.to_gbq(credentials=credentials, 
#                             destination_table='curso_etl.etl_csv',
#                             if_exists='replace',
#                             table_schema=[{'name': 'data_inicio_vigencia', 'type': 'DATE'},
#                                          {'name': 'data_termino_vigencia', 'type': 'DATE'}])

# In[393]:


contratos_finalissima.to_gbq(credentials=credentials, 
                            destination_table='curso_etl.etl_csv',
                            if_exists='replace')
                            


# In[ ]:





# In[ ]:




