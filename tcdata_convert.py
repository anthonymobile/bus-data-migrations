#!/usr/bin/env python
# coding: utf-8

# In[13]:


# get file list
import glob
filelist = glob.glob('./nyc/transitcenter/data/*.xz')
filelist.sort()

# In[15]:


# extract each into a df
# dump df to parquet
import pandas as pd
from lzma import LZMAError
 
for f in filelist:
    try:
        df = pd.read_csv(f)
        try:
            df.to_parquet(f'{f.split(".csv")[0]}.parquet')
            print (f)
        except Exception as e:
            print (f'{e}:{f}')
                    
    except LZMAError:
        print (f'LZMAError:{f}')
    except Exception as e:
        print (f'{e}:{f}')


# In[ ]:




