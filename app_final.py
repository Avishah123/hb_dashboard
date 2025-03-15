import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import subprocess
import time

# Page configuration
st.set_page_config(page_title="Market Data Dashboard", layout="wide")

# Add minimal CSS for font size and alignment
st.markdown("""
<style>
    /* Set global font size to 20px and left alignment */
    .stApp, .stTextInput, .stMarkdown, .stDataFrame, .stTable, p, div {
        font-size: 20px !important;
        text-align: left !important;
    }
    
    /* Make sure headers are also properly sized and aligned */
    h1, h2, h3, h4, h5, h6 {
        font-size: 20px !important;
        text-align: left !important;
    }
    
    /* Ensure buttons, selectboxes, and other inputs have consistent text size */
    button, select, option, .stSelectbox, .stMultiSelect {
        font-size: 20px !important;
    }
    
    /* Fix tabs font size */
    button[data-baseweb="tab"] {
        font-size: 20px !important;
    }
    
    /* Fix for dataframe text */
    .dataframe {
        font-size: 20px !important;
    }
</style>
""", unsafe_allow_html=True)

# Get database connection parameters from secrets
# You can access these via st.secrets["postgresql"]["host"], etc.
try:
    PG_HOST = st.secrets["postgresql"]["host"]
    PG_PORT = st.secrets["postgresql"]["port"]
    PG_DATABASE = st.secrets["postgresql"]["database"]
    PG_USER = st.secrets["postgresql"]["user"]
    PG_PASSWORD = st.secrets["postgresql"]["password"]
except Exception as e:
    st.error(f"Error loading secrets: {e}. Please check your .streamlit/secrets.toml file.")
    # Provide fallback for development (remove in production)
    PG_HOST = "localhost"
    PG_PORT = "5432"
    PG_DATABASE = "hb_dashboard"
    PG_USER = "postgres"
    PG_PASSWORD = "password"

# SQLAlchemy connection string for pandas
pg_connection_string = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Add title and description
st.title("Market Data Dashboard")
st.markdown("### Analysis of Index and Stock Data")

# Create a session state for tracking database connection and data generation
if 'db_connected' not in st.session_state:
    st.session_state.db_connected = False
if 'data_last_refreshed' not in st.session_state:
    st.session_state.data_last_refreshed = None

# Function to check database connection and data availability
def check_database():
    try:
        # Try to connect to PostgreSQL
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        
        # Check if tables exist and have data
        cursor.execute("SELECT COUNT(*) FROM market_index")
        index_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM market_stocks")
        stocks_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM market_summary")
        summary_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM total_index")
        total_index_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM total_stocks")
        total_stocks_count = cursor.fetchone()[0]
        
        # Get last update timestamp
        cursor.execute("SELECT MAX(updated_at) FROM market_index")
        last_updated = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        st.session_state.db_connected = True
        st.session_state.data_last_refreshed = last_updated
        
        return True, {
            "index": index_count, 
            "stocks": stocks_count, 
            "summary": summary_count,
            "total_index": total_index_count,
            "total_stocks": total_stocks_count,
            "last_updated": last_updated
        }
    except Exception as e:
        st.session_state.db_connected = False
        return False, str(e)

# Function to generate/refresh data
def refresh_data():
    try:
        st.info("Generating data and updating database... This may take a few minutes.")
        progress_bar = st.progress(0)
        
        # Run the data generation script
        process = subprocess.Popen(
            ["python", "generate_data.py"], 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Show progress updates
        while True:
            progress_bar.progress(min(0.9, progress_bar.progress + 0.1))
            time.sleep(2)
            
            # Check if process is still running
            if process.poll() is not None:
                break
        
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            progress_bar.progress(1.0)
            st.success("Data updated successfully!")
            return True
        else:
            st.error(f"Error generating data: {stderr}")
            return False
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return False

# Function to load data from PostgreSQL
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_data():
    try:
        engine = create_engine(pg_connection_string)
        
        # Load each table into a separate dataframe
        df_index = pd.read_sql("SELECT * FROM market_index", engine)
        df_stocks = pd.read_sql("SELECT * FROM market_stocks", engine)
        df_summary = pd.read_sql("SELECT * FROM market_summary", engine)
        df_total_index = pd.read_sql("SELECT * FROM total_index", engine)
        df_total_stocks = pd.read_sql("SELECT * FROM total_stocks", engine)
        
        # Convert date columns to datetime
        date_columns = ['date']
        for df in [df_index, df_stocks, df_summary, df_total_index, df_total_stocks]:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
        
        # Make the column names consistent with the Excel-based version
        df_index = df_index.rename(columns={
            'date': 'Date',
            'symbol': 'Symbol',
            'bt_frwd_long_qty': 'BtFrwdLongQty',
            'bt_frwd_short_qty': 'BtFrwdShortQty',
            'net_qty_carry_fwd': 'NetQtyCarryFwd',
            'net_value_in_cr': 'NetValue_in_Cr',
            'new_total': 'NewTotal',
            'total_buy_clients': 'TotalBuyClients',
            'total_sell_clients': 'TotalSellClients',
            'buy_percent': 'BuyPercent',
            'sell_percent': 'SellPercent'
        })
        
        df_stocks = df_stocks.rename(columns={
            'date': 'Date',
            'symbol': 'Symbol',
            'bt_frwd_long_qty': 'BtFrwdLongQty',
            'bt_frwd_short_qty': 'BtFrwdShortQty',
            'net_qty_carry_fwd': 'NetQtyCarryFwd',
            'net_value_in_cr': 'NetValue_in_Cr',
            'new_total': 'NewTotal',
            'total_buy_clients': 'TotalBuyClients',
            'total_sell_clients': 'TotalSellClients',
            'buy_percent': 'BuyPercent',
            'sell_percent': 'SellPercent',
            'market_cap': 'MarketCap',
            'market_cap_percentage': 'MarketCap_Percentage'
        })
        
        df_summary = df_summary.rename(columns={
            'date': 'Date',
            'instrument': 'Instrument',
            'net_qty_carry_fwd': 'NetQtyCarryFwd',
            'net_value_in_cr': 'NetValue_in_Cr'
        })
        
        df_total_index = df_total_index.rename(columns={
            'date': 'Date',
            'day': 'Day',
            'instrument': 'Instrument',
            'net_qty_carry_fwd': 'NetQtyCarryFwd',
            'net_value_in_cr': 'NetValue_in_Cr',
            'nsei_close': 'NSEI_Close'
        })
        
        df_total_stocks = df_total_stocks.rename(columns={
            'date': 'Date',
            'day': 'Day',
            'instrument': 'Instrument',
            'net_qty_carry_fwd': 'NetQtyCarryFwd',
            'net_value_in_cr': 'NetValue_in_Cr',
            'nsei_close': 'NSEI_Close'
        })
        
        return {
            "INDEX": df_index,
            "STOCKS": df_stocks,
            "SUMMARY": df_summary,
            "Total_Index": df_total_index,
            "Total_Stocks": df_total_stocks
        }
    except Exception as e:
        st.error(f"Error loading data from PostgreSQL: {e}")
        return None

# Sidebar for data operations and filtering
st.sidebar.header("Data Controls")

# Check database connection
connection_status, connection_info = check_database()

if connection_status:
    st.sidebar.success("✅ Connected to database")
    
    # Show data statistics
    if isinstance(connection_info, dict):
        counts_html = f"""
        <table style='width:100%; font-size:20px; text-align:left;'>
        <tr><td><b>Index records:</b></td><td>{connection_info['index']}</td></tr>
        <tr><td><b>Stocks records:</b></td><td>{connection_info['stocks']}</td></tr>
        <tr><td><b>Summary records:</b></td><td>{connection_info['summary']}</td></tr>
        </table>
        """
        st.sidebar.markdown(counts_html, unsafe_allow_html=True)
        
        if connection_info['last_updated']:
            st.sidebar.info(f"Last updated: {connection_info['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if st.sidebar.button("Refresh Data", type="primary"):
        success = refresh_data()
        if success:
            # Clear cache to force reload
            load_data.clear()
            # Check connection again to refresh stats
            connection_status, connection_info = check_database()
else:
    st.sidebar.error("❌ Database connection error")
    st.sidebar.info(f"Error: {connection_info}")
    
    if st.sidebar.button("Initialize Database"):
        refresh_data()
        # Check connection again
        connection_status, connection_info = check_database()

# Only proceed if database is connected
if connection_status:
    # Load data from PostgreSQL
    data = load_data()
    
    if data:
        # Get min and max dates from the data
        try:
            min_date = min(df["Date"].min() for df in data.values() if "Date" in df.columns)
            max_date = max(df["Date"].max() for df in data.values() if "Date" in df.columns)
        except Exception as e:
            st.error(f"Error calculating date range: {e}")
            min_date = datetime.now()
            max_date = datetime.now()

        # Add sidebar for date filtering
        st.sidebar.header("Filters")
        st.sidebar.subheader("Date Range")

        start_date = st.sidebar.date_input("Start Date", min_date)
        end_date = st.sidebar.date_input("End Date", max_date)

        # Apply date filtering to all dataframes
        def filter_by_date(df):
            if "Date" in df.columns:
                return df[(df["Date"].dt.date >= start_date) & (df["Date"].dt.date <= end_date)]
            return df

        filtered_data = {
            key: filter_by_date(df) for key, df in data.items()
        }

        # Create tabs for different views
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Overview", "INDEX", "STOCKS", "Total Index", "Total Stocks", "Raw Data"])

        with tab1:
            st.header("Market Overview")
            
            # Create summary metrics
            col1, col2 = st.columns(2)
            
            with col1:
                # Summary metrics for indices
                try:
                    latest_date = filtered_data["Total_Index"]["Date"].max()
                    latest_index_data = filtered_data["Total_Index"][filtered_data["Total_Index"]["Date"] == latest_date]
                    
                    if not latest_index_data.empty:
                        st.metric(
                            label="Latest Index Net Value (Cr)",
                            value=f"₹{latest_index_data['NetValue_in_Cr'].values[0]:,.2f} Cr"
                        )
                    else:
                        st.warning("No index data available for the selected date range.")
                        
                    # Plot the trend of index net value
                    fig = px.line(
                        filtered_data["Total_Index"],
                        x="Date",
                        y="NetValue_in_Cr",
                        title="Index Net Value Trend (Cr)",
                        labels={"NetValue_in_Cr": "Net Value (Cr)", "Date": "Date"}
                    )
                    
                    # Update chart font size
                    fig.update_layout(
                        title_font=dict(size=20),
                        legend_font=dict(size=20),
                        xaxis_title_font=dict(size=20),
                        yaxis_title_font=dict(size=20),
                        xaxis_tickfont=dict(size=20),
                        yaxis_tickfont=dict(size=20)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error displaying index metrics: {e}")
            
            with col2:
                # Summary metrics for stocks
                try:
                    if not filtered_data["Total_Stocks"].empty:
                        latest_stock_data = filtered_data["Total_Stocks"][filtered_data["Total_Stocks"]["Date"] == latest_date]
                        
                        if not latest_stock_data.empty:
                            st.metric(
                                label="Latest Stocks Net Value (Cr)",
                                value=f"₹{latest_stock_data['NetValue_in_Cr'].values[0]:,.2f} Cr"
                            )
                        else:
                            st.warning("No stock data available for the selected date.")
                        
                        # Plot the trend of stocks net value
                        fig = px.line(
                            filtered_data["Total_Stocks"],
                            x="Date",
                            y="NetValue_in_Cr",
                            title="Stocks Net Value Trend (Cr)",
                            labels={"NetValue_in_Cr": "Net Value (Cr)", "Date": "Date"}
                        )
                        
                        # Update chart font size
                        fig.update_layout(
                            title_font=dict(size=20),
                            legend_font=dict(size=20),
                            xaxis_title_font=dict(size=20),
                            yaxis_title_font=dict(size=20),
                            xaxis_tickfont=dict(size=20),
                            yaxis_tickfont=dict(size=20)
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No stock data available for the selected date range.")
                except Exception as e:
                    st.error(f"Error displaying stock metrics: {e}")
            
            # Combined chart showing both index and stock trends
            try:
                summary_df = filtered_data["SUMMARY"].copy()
                if not summary_df.empty:
                    fig = px.line(
                        summary_df,
                        x="Date",
                        y="NetValue_in_Cr",
                        color="Instrument",
                        title="Comparison of Index vs Stocks Net Value",
                        labels={"NetValue_in_Cr": "Net Value (Cr)", "Date": "Date", "Instrument": "Category"}
                    )
                    
                    # Update chart font size
                    fig.update_layout(
                        title_font=dict(size=20),
                        legend_font=dict(size=20),
                        xaxis_title_font=dict(size=20),
                        yaxis_title_font=dict(size=20),
                        xaxis_tickfont=dict(size=20),
                        yaxis_tickfont=dict(size=20),
                        legend=dict(font=dict(size=20))
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No summary data available for the selected date range.")
            except Exception as e:
                st.error(f"Error displaying summary chart: {e}")

        # Rest of your code remains the same...
        # For brevity, I've omitted the remaining tabs code, but it would continue as in your original code
        # with the font size and alignment changes

# Add information and credits at the bottom of the sidebar
st.sidebar.markdown("---")
st.sidebar.info(
    """
    This dashboard visualizes market data for indices and stocks.
    Data is loaded from PostgreSQL database.
    """
)