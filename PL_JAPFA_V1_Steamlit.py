# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

session = get_active_session()


# Page configuration
st.set_page_config(
    page_title="JAPFA Sales Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f2f2f3;
        padding: 15px;
        border-radius: 10px;
    }
    h1 {
        color: #ff8c00;
    }
    </style>
    """, unsafe_allow_html=True)



# Cache data loading functions
@st.cache_data(ttl=600)
def load_sales_dashboard():
    query = "SELECT * FROM PL_JAPFA.IND_BI.VW_SALES_DASHBOARD"
    return session.sql(query).to_pandas()

@st.cache_data(ttl=600)
def load_sales_by_date():
    query = "SELECT * FROM PL_JAPFA.IND_BI.VW_SALES_BY_DATE ORDER BY DATE_KEY ASC"
    return session.sql(query).to_pandas()

@st.cache_data(ttl=600)
def load_sales_by_product():
    query = "SELECT * FROM PL_JAPFA.IND_BI.VW_SALES_BY_PRODUCT ORDER BY TOTAL_FINAL_QTY DESC"
    return session.sql(query).to_pandas()

@st.cache_data(ttl=600)
def load_sales_by_billing_type():
    query = "SELECT * FROM PL_JAPFA.IND_BI.VW_SALES_BY_BILLING_TYPE"
    return session.sql(query).to_pandas()

# Load data
try:
    with st.spinner('Loading data...'):
        df_dashboard = load_sales_dashboard()
        df_by_date = load_sales_by_date()
        df_by_product = load_sales_by_product()
        df_by_billing = load_sales_by_billing_type()

    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    if not df_dashboard.empty:
        # Year-Month filter
        year_months = sorted(df_dashboard['YEAR_MONTH'].unique())
        selected_months = st.sidebar.multiselect(
            "Select Month(s)",
            options=year_months,
            default=year_months[-3:] if len(year_months) >= 3 else year_months
        )
        
        # Product Category filter
        categories = sorted(df_dashboard['PRODUCT_CATEGORY'].unique())
        selected_categories = st.sidebar.multiselect(
            "Select Product Category",
            options=categories,
            default=categories
        )
        
        # Billing Type filter
        billing_types = sorted(df_dashboard['BILLING_TYPE_DESC'].unique())
        selected_billing = st.sidebar.multiselect(
            "Select Billing Type",
            options=billing_types,
            default=billing_types
        )
        
        # Material Group filter
        material_groups = sorted(df_dashboard['MATERIAL_GROUP'].unique())
        selected_material_groups = st.sidebar.multiselect(
            "Select Material Group",
            options=material_groups,
            default=material_groups
        )
        
        # Apply filters
        df_filtered = df_dashboard[
            (df_dashboard['YEAR_MONTH'].isin(selected_months)) &
            (df_dashboard['PRODUCT_CATEGORY'].isin(selected_categories)) &
            (df_dashboard['BILLING_TYPE_DESC'].isin(selected_billing)) &
            (df_dashboard['MATERIAL_GROUP'].isin(selected_material_groups))
        ]
    else:
        df_filtered = df_dashboard
        st.sidebar.warning("No data available for filtering")

    # Refresh button
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # KPI Metrics
    st.header("📈 Key Performance Indicators")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_docs = df_filtered['BILLING_DOCUMENT'].nunique()
        st.metric("Total Documents", f"{total_docs:,}")
    
    with col2:
        total_qty = df_filtered['BILLING_QTY_IN_SKU'].sum()
        st.metric("Total Billing Qty", f"{total_qty:,.0f}")
    
    with col3:
        total_returns = df_filtered['RETURN_QTY'].sum()
        st.metric("Total Returns", f"{total_returns:,.0f}")
    
    with col4:
        final_qty = df_filtered['FINAL_QTY'].sum()
        st.metric("Final Quantity", f"{final_qty:,.0f}")
    
    with col5:
        return_rate = (total_returns / total_qty * 100) if total_qty > 0 else 0
        st.metric("Return Rate", f"{return_rate:.2f}%")

    st.markdown("---")

    # Row 1: Sales Trend and Billing Type Distribution
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("📅 Sales Trend Over Time")
        if not df_by_date.empty:
            # Filter date data based on selected months
            if selected_months:
                df_date_filtered = df_by_date[df_by_date['YEAR_MONTH'].isin(selected_months)]
            else:
                df_date_filtered = df_by_date
            
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=df_date_filtered['DATE_KEY'],
                y=df_date_filtered['TOTAL_FINAL_QTY'],
                mode='lines+markers',
                name='Final Quantity',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=8),
                fill='tozeroy',
                fillcolor='rgba(31, 119, 180, 0.2)'
            ))
            fig_trend.update_layout(
                xaxis_title="Date",
                yaxis_title="Total Final Quantity",
                hovermode='x unified',
                height=400,
                showlegend=False
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("No date data available")

    with col_right:
        st.subheader("💳 Sales by Billing Type")
        if not df_by_billing.empty:
            fig_billing = px.pie(
                df_by_billing,
                values='TOTAL_FINAL_QTY',
                names='BILLING_TYPE_DESC',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_billing.update_traces(
                textposition='inside', 
                textinfo='percent+label',
                textfont_size=12
            )
            fig_billing.update_layout(height=400)
            st.plotly_chart(fig_billing, use_container_width=True)
            
            # Display billing type details
            st.dataframe(
                df_by_billing[['BILLING_TYPE', 'BILLING_TYPE_DESC', 'TOTAL_DOCUMENTS', 'TOTAL_FINAL_QTY']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No billing type data available")

    st.markdown("---")

    # Row 2: Product Category and Top Products
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.subheader("🏷️ Sales by Product Category")
        if not df_filtered.empty:
            category_sales = df_filtered.groupby('PRODUCT_CATEGORY').agg({
                'FINAL_QTY': 'sum',
                'BILLING_DOCUMENT': 'nunique'
            }).reset_index()
            category_sales.columns = ['PRODUCT_CATEGORY', 'TOTAL_QTY', 'TOTAL_DOCS']
            category_sales = category_sales.sort_values('TOTAL_QTY', ascending=True)
            
            fig_category = px.bar(
                category_sales,
                y='PRODUCT_CATEGORY',
                x='TOTAL_QTY',
                orientation='h',
                color='TOTAL_QTY',
                color_continuous_scale='Blues',
                text='TOTAL_QTY'
            )
            fig_category.update_traces(
                texttemplate='%{text:,.0f}', 
                textposition='outside'
            )
            fig_category.update_layout(
                xaxis_title="Total Final Quantity",
                yaxis_title="Product Category",
                height=400,
                showlegend=False
            )
            st.plotly_chart(fig_category, use_container_width=True)
        else:
            st.info("No product category data available")

    with col_right2:
        st.subheader("🏆 Top 10 Products")
        if not df_by_product.empty:
            top_products = df_by_product.head(10)
            
            fig_products = px.bar(
                top_products,
                x='TOTAL_FINAL_QTY',
                y='ITEM_DESCRIPTION',
                orientation='h',
                color='PRODUCT_CATEGORY',
                text='TOTAL_FINAL_QTY',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_products.update_traces(
                texttemplate='%{text:,.0f}', 
                textposition='outside'
            )
            fig_products.update_layout(
                xaxis_title="Final Quantity",
                yaxis_title="Product",
                height=400,
                yaxis={'categoryorder': 'total ascending'}
            )
            st.plotly_chart(fig_products, use_container_width=True)
        else:
            st.info("No product data available")

    st.markdown("---")

    # Row 3: Monthly Comparison
    st.subheader("📊 Monthly Sales Comparison")
    if not df_filtered.empty:
        monthly_sales = df_filtered.groupby('YEAR_MONTH').agg({
            'BILLING_QTY_IN_SKU': 'sum',
            'RETURN_QTY': 'sum',
            'FINAL_QTY': 'sum',
            'BILLING_DOCUMENT': 'nunique'
        }).reset_index()
        monthly_sales.columns = ['YEAR_MONTH', 'BILLING_QTY', 'RETURNS', 'FINAL_QTY', 'DOCUMENTS']
        
        fig_monthly = go.Figure()
        fig_monthly.add_trace(go.Bar(
            x=monthly_sales['YEAR_MONTH'],
            y=monthly_sales['BILLING_QTY'],
            name='Billing Quantity',
            marker_color='lightblue'
        ))
        fig_monthly.add_trace(go.Bar(
            x=monthly_sales['YEAR_MONTH'],
            y=monthly_sales['RETURNS'],
            name='Returns',
            marker_color='salmon'
        ))
        fig_monthly.add_trace(go.Bar(
            x=monthly_sales['YEAR_MONTH'],
            y=monthly_sales['FINAL_QTY'],
            name='Final Quantity',
            marker_color='lightgreen'
        ))
        
        fig_monthly.update_layout(
            barmode='group',
            xaxis_title="Month",
            yaxis_title="Quantity",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_monthly, use_container_width=True)
        
        # Show monthly stats table
        st.dataframe(monthly_sales, use_container_width=True, hide_index=True)
    else:
        st.info("No monthly data available")

    st.markdown("---")

    # Row 4: Material Group Analysis
    st.subheader("📦 Sales by Material Group")
    col_mg1, col_mg2 = st.columns(2)
    
    with col_mg1:
        if not df_filtered.empty:
            material_group_sales = df_filtered.groupby('MATERIAL_GROUP').agg({
                'FINAL_QTY': 'sum',
                'BILLING_DOCUMENT': 'nunique'
            }).reset_index()
            material_group_sales.columns = ['MATERIAL_GROUP', 'TOTAL_QTY', 'TOTAL_DOCS']
            material_group_sales = material_group_sales.sort_values('TOTAL_QTY', ascending=False)
            
            fig_mg = px.treemap(
                material_group_sales,
                path=['MATERIAL_GROUP'],
                values='TOTAL_QTY',
                color='TOTAL_QTY',
                color_continuous_scale='RdYlGn',
                title='Material Group Distribution (Treemap)'
            )
            fig_mg.update_layout(height=400)
            st.plotly_chart(fig_mg, use_container_width=True)
        else:
            st.info("No material group data available")
    
    with col_mg2:
        if not df_filtered.empty:
            # Material Group Table
            st.markdown("**Material Group Summary**")
            st.dataframe(
                material_group_sales.style.format({
                    'TOTAL_QTY': '{:,.0f}',
                    'TOTAL_DOCS': '{:,}'
                }),
                use_container_width=True,
                hide_index=True,
                height=350
            )
        else:
            st.info("No material group data available")

    st.markdown("---")

    # Detailed Data Table
    st.header("📋 Detailed Sales Data")
    
    # Add search and display options
    col_search, col_rows = st.columns([3, 1])
    with col_search:
        search_term = st.text_input("🔎 Search in data", "")
    with col_rows:
        rows_to_show = st.selectbox("Rows to display", [10, 25, 50, 100, "All"], index=1)
    
    if search_term:
        mask = df_filtered.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
        display_df = df_filtered[mask]
    else:
        display_df = df_filtered
    
    # Apply row limit
    if rows_to_show != "All":
        display_df = display_df.head(rows_to_show)
    
    # Display summary stats
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        st.metric("Filtered Records", f"{len(display_df):,}")
    with col_s2:
        st.metric("Total Records", f"{len(df_filtered):,}")
    with col_s3:
        st.metric("Unique Products", f"{df_filtered['MATERIAL_GROUP'].nunique():,}")
    
    # Display dataframe
    st.dataframe(
        display_df,
        use_container_width=True,
        height=400
    )
    
    # Download button
    csv = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv,
        file_name=f"japfa_sales_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

    # Footer
    st.markdown("---")
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        st.caption("📊 **Data Source:** PL_JAPFA.IND_BI Views")
    with col_f2:
        st.caption(f"🕐 **Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col_f3:
        st.caption("🔄 **Auto-refresh:** Every 10 minutes")

except Exception as e:
    st.error(f"⚠️ An error occurred: {str(e)}")
    st.info("Please ensure you have proper Snowflake connection configured and the required views exist.")
    
    # Show connection info
    with st.expander("📌 Required Views"):
        st.code("""
        - PL_JAPFA.IND_BI.VW_SALES_DASHBOARD
        - PL_JAPFA.IND_BI.VW_SALES_BY_DATE
        - PL_JAPFA.IND_BI.VW_SALES_BY_PRODUCT
        - PL_JAPFA.IND_BI.VW_SALES_BY_BILLING_TYPE
        """, language="sql")
# Title
st.title("📊 JAPFA Sales Analytics Dashboard :chicken::egg:")
st.markdown("**Real-time Sales Performance & Analytics**")
st.markdown("---")

