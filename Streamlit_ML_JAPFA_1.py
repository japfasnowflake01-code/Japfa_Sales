



import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(f":chicken: JAPFA  BEAST APP :chicken: ")
st.write(
  """Visit at our store or Place order online
  **And if you're new to JAPFA BEAST,** check
  out our Website 
  [japfabest](https://japfabest.in).
  """
)



# Get the current credentials
session = get_active_session()



# SQL Text
sql_stmt = """
SELECT
   BILLING_DOCUMENT, ITEM, BILLING_TYPE, BILLING_DATE,
            BILLING_QTY_IN_SKU, MATERIAL, ITEM_DESCRIPTION,
            MATERIAL_GROUP, ASSIGNMENT
FROM
    ML_JAPFA.IND_UTIL.ZSD_BAS_SALESDOC_ALL_STAGE_T;

    
"""
# SQL Text
sql_stmtt = """

select * from ML_JAPFA.IND.ZSD_BAS_SALESDOC_ALL_AGG_T;;
    
"""



sf_df = session.sql(sql_stmt).collect()
sf_dff = session.sql(sql_stmtt).collect()

pd_df = pd.DataFrame(sf_df, 
                    
                    columns = ['BILLING_DOCUMENT', 'ITEM', 'BILLING_TYPE', 'BILLING_DATE',
            'BILLING_QTY_IN_SKU', 'MATERIAL', 'ITEM_DESCRIPTION',
            'MATERIAL_GROUP', 'ASSIGNMENT'])

pd_dff = pd.DataFrame(sf_dff, 
                    
                    columns = ['BILLING_DOCUMENT', 'ITEM', 'BILLING_TYPE', 'BILLING_DATE',
            'BILLING_QTY_IN_SKU', 'MATERIAL', 'ITEM_DESCRIPTION',
            'MATERIAL_GROUP', 'ASSIGNMENT', 'FINAL_QTY'])

# Display total record count above each dataframe
st.markdown(f"### Total Records in Stage Table: {len(pd_df)}")
st.dataframe(pd_df)

st.markdown(f"### Total Records in Aggregate Table: {len(pd_dff)}")
st.dataframe(pd_dff)




