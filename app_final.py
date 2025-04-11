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

# Database connection parameters - now using st.secrets

# Get connection details from secrets.toml
PG_HOST = st.secrets["postgresql"]["host"]
PG_PORT = st.secrets["postgresql"]["port"]
PG_DATABASE = st.secrets["postgresql"]["database"]
PG_USER = st.secrets["postgresql"]["user"]
PG_PASSWORD = st.secrets["postgresql"]["password"]

# SQLAlchemy connection string for pandas
pg_connection_string = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

if st.button("Test DB Connection"):
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=10
        )
        st.success("Connected successfully!")
        conn.close()
    except Exception as e:
        st.error(f"Connection failed: {e}")

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
        
        # Pass database credentials to the generate_data.py script via environment variables
        env = {
            "PG_HOST": PG_HOST,
            "PG_PORT": PG_PORT,
            "PG_DATABASE": PG_DATABASE,
            "PG_USER": PG_USER,
            "PG_PASSWORD": PG_PASSWORD
        }
        
        # Run the data generation script
        process = subprocess.Popen(
            ["python", "generate_data.py"], 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
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
        
        # Multiply MarketCap_Percentage by 100 if the column exists
        if 'MarketCap_Percentage' in df_stocks.columns:
            df_stocks['MarketCap_Percentage'] = df_stocks['MarketCap_Percentage'] * 100
            
        if 'MarketCap_Percentage' in df_index.columns:
            df_index['MarketCap_Percentage'] = df_index['MarketCap_Percentage'] * 100
        
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
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(["Overview", "INDEX", "STOCKS", "Total Index", "Total Stocks", "Percentage Change", "Percentage Change (NetQtyFwd)", "Raw Data"])

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

        with tab2:
            st.header("INDEX Details")
            
            # Filter options
            index_symbols = sorted(filtered_data["INDEX"]["Symbol"].unique()) if not filtered_data["INDEX"].empty else []
            default_indices = index_symbols[:3] if len(index_symbols) >= 3 else index_symbols
            selected_indices = st.multiselect("Select Indices", index_symbols, default=default_indices)
            
            if selected_indices:
                filtered_index_data = filtered_data["INDEX"][filtered_data["INDEX"]["Symbol"].isin(selected_indices)]
                
                # Show the data table
                st.subheader("Index Data Table")
                st.dataframe(filtered_index_data, use_container_width=True)
                
                # Create visualizations
                st.subheader("Index Analysis")
                
                # Net value by symbol
                try:
                    fig = px.bar(
                        filtered_index_data.groupby("Symbol")["NetValue_in_Cr"].sum().reset_index(),
                        x="Symbol",
                        y="NetValue_in_Cr",
                        title="Net Value by Index (Cr)",
                        labels={"NetValue_in_Cr": "Net Value (Cr)", "Symbol": "Index"}
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
                    st.error(f"Error displaying net value chart: {e}")
                
                # Buy vs Sell Percentages
                try:
                    if "BuyPercent" in filtered_index_data.columns and "SellPercent" in filtered_index_data.columns:
                        buy_sell_data = filtered_index_data.groupby("Symbol")[["BuyPercent", "SellPercent"]].mean().reset_index()
                        
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=buy_sell_data["Symbol"], y=buy_sell_data["BuyPercent"], name="Buy %", marker_color="green"))
                        fig.add_trace(go.Bar(x=buy_sell_data["Symbol"], y=buy_sell_data["SellPercent"], name="Sell %", marker_color="red"))
                        
                        fig.update_layout(
                            title="Average Buy vs Sell Percentages by Index",
                            xaxis_title="Index",
                            yaxis_title="Percentage",
                            barmode="group",
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                            title_font=dict(size=20),
                            legend_font=dict(size=20),
                            xaxis_title_font=dict(size=20),
                            yaxis_title_font=dict(size=20),
                            xaxis_tickfont=dict(size=20),
                            yaxis_tickfont=dict(size=20)
                        )
                        st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error displaying buy/sell percentages chart: {e}")
                
                # Market Cap Percentage (if available for INDEX)
                try:
                    if "MarketCap_Percentage" in filtered_index_data.columns:
                        market_cap_data = filtered_index_data.groupby("Symbol")["MarketCap_Percentage"].mean().reset_index()
                        
                        fig = px.bar(
                            market_cap_data,
                            x="Symbol",
                            y="MarketCap_Percentage",
                            title="Average Market Cap Percentage by Index",
                            labels={"MarketCap_Percentage": "Market Cap %", "Symbol": "Index"}
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
                    st.error(f"Error displaying market cap chart: {e}")
            else:
                st.info("Please select at least one index to display data.")

        with tab3:
            st.header("STOCKS Details")
            
            # Filter options
            stock_symbols = sorted(filtered_data["STOCKS"]["Symbol"].unique()) if not filtered_data["STOCKS"].empty else []
            default_stocks = stock_symbols[:3] if len(stock_symbols) >= 3 else stock_symbols
            selected_stocks = st.multiselect("Select Stocks", stock_symbols, default=default_stocks)
            
            if selected_stocks:
                filtered_stock_data = filtered_data["STOCKS"][filtered_data["STOCKS"]["Symbol"].isin(selected_stocks)]
                
                # Calculate NetQtyFwd Average Values
                # 1. For entire dataset
                entire_dataset_avg = data["STOCKS"][data["STOCKS"]["Symbol"].isin(selected_stocks)].groupby("Symbol")["NetQtyCarryFwd"].mean().reset_index()
                entire_dataset_avg = entire_dataset_avg.rename(columns={"NetQtyCarryFwd": "NetQtyFwd_Avg_All"})
                
                # 2. For 3-month period
                max_date = filtered_data["STOCKS"]["Date"].max()
                three_months_ago = max_date - pd.Timedelta(days=90)
                three_month_data = filtered_data["STOCKS"][
                    (filtered_data["STOCKS"]["Date"] >= three_months_ago) & 
                    (filtered_data["STOCKS"]["Symbol"].isin(selected_stocks))
                ]
                three_month_avg = three_month_data.groupby("Symbol")["NetQtyCarryFwd"].mean().reset_index()
                three_month_avg = three_month_avg.rename(columns={"NetQtyCarryFwd": "NetQtyFwd_Avg_3Months"})
                
                # Merge averages with the filtered data
                filtered_stock_data = pd.merge(
                    filtered_stock_data,
                    entire_dataset_avg,
                    on="Symbol",
                    how="left"
                )
                filtered_stock_data = pd.merge(
                    filtered_stock_data,
                    three_month_avg,
                    on="Symbol",
                    how="left"
                )
                
                # Show the data table with the new columns
                st.subheader("Stock Data Table")
                st.dataframe(filtered_stock_data, use_container_width=True)
                
                # Create visualizations
                st.subheader("Stock Analysis")
                
                # Net value by symbol
                try:
                    fig = px.bar(
                        filtered_stock_data.groupby("Symbol")["NetValue_in_Cr"].sum().reset_index(),
                        x="Symbol",
                        y="NetValue_in_Cr",
                        title="Net Value by Stock (Cr)",
                        labels={"NetValue_in_Cr": "Net Value (Cr)", "Symbol": "Stock"}
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
                    st.error(f"Error displaying net value chart: {e}")
                
                # Market Cap Percentage (if available)
                try:
                    if "MarketCap_Percentage" in filtered_stock_data.columns:
                        market_cap_data = filtered_stock_data.groupby("Symbol")["MarketCap_Percentage"].mean().reset_index()
                        
                        fig = px.bar(
                            market_cap_data,
                            x="Symbol",
                            y="MarketCap_Percentage",
                            title="Average Market Cap Percentage by Stock",
                            labels={"MarketCap_Percentage": "Market Cap %", "Symbol": "Stock"}
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
                    st.error(f"Error displaying market cap chart: {e}")
                    
                # Buy vs Sell Percentages
                try:
                    if "BuyPercent" in filtered_stock_data.columns and "SellPercent" in filtered_stock_data.columns:
                        buy_sell_data = filtered_stock_data.groupby("Symbol")[["BuyPercent", "SellPercent"]].mean().reset_index()
                        
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=buy_sell_data["Symbol"], y=buy_sell_data["BuyPercent"], name="Buy %", marker_color="green"))
                        fig.add_trace(go.Bar(x=buy_sell_data["Symbol"], y=buy_sell_data["SellPercent"], name="Sell %", marker_color="red"))
                        
                        fig.update_layout(
                            title="Average Buy vs Sell Percentages by Stock",
                            xaxis_title="Stock",
                            yaxis_title="Percentage",
                            barmode="group",
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                            title_font=dict(size=20),
                            legend_font=dict(size=20),
                            xaxis_title_font=dict(size=20),
                            yaxis_title_font=dict(size=20),
                            xaxis_tickfont=dict(size=20),
                            yaxis_tickfont=dict(size=20)
                        )
                        st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error displaying buy/sell percentages chart: {e}")
            else:
                st.info("Please select at least one stock to display data.")

        with tab4:
            st.header("Total Index Analysis")
            
            total_index_df = filtered_data["Total_Index"].copy()
            
            # Display data table
            st.subheader("Total Index Data")
            st.dataframe(total_index_df, use_container_width=True)
            
            # Line chart for NetValue_in_Cr
            try:
                if not total_index_df.empty:
                    fig = px.line(
                        total_index_df,
                        x="Date",
                        y="NetValue_in_Cr",
                        title="Index Net Value Over Time (Cr)",
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
                    st.warning("No total index data available for the selected date range.")
            except Exception as e:
                st.error(f"Error displaying net value chart: {e}")
            
            # If NSEI_Close is available, show comparison with index performance
            try:
                if "NSEI_Close" in total_index_df.columns and not total_index_df.empty:
                    st.subheader("Index Net Value vs Nifty Performance")
                    
                    # Create two y-axes chart
                    fig = go.Figure()
                    
                    # First trace for Net Value
                    fig.add_trace(
                        go.Scatter(
                            x=total_index_df["Date"],
                            y=total_index_df["NetValue_in_Cr"],
                            name="Net Value (Cr)",
                            line=dict(color="blue")
                        )
                    )
                    
                    # Second trace for Nifty close price
                    fig.add_trace(
                        go.Scatter(
                            x=total_index_df["Date"],
                            y=total_index_df["NSEI_Close"],
                            name="Nifty Close",
                            line=dict(color="red"),
                            yaxis="y2"
                        )
                    )
                    
                    # Update layout for two y-axes
                    fig.update_layout(
                        title="Net Value vs Nifty Performance",
                        xaxis=dict(title="Date"),
                        yaxis=dict(title="Net Value (Cr)"),
                        yaxis2=dict(
                            title=dict(text="Nifty Close", font=dict(color="red", size=20)),
                            tickfont=dict(color="red", size=20),
                            anchor="x",
                            overlaying="y",
                            side="right"
                        ),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        title_font=dict(size=20),
                        legend_font=dict(size=20),
                        xaxis_title_font=dict(size=20),
                        yaxis_title_font=dict(size=20),
                        xaxis_tickfont=dict(size=20),
                        yaxis_tickfont=dict(size=20)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error displaying Nifty comparison chart: {e}")

        with tab5:
            st.header("Total Stocks Analysis")
            
            total_stocks_df = filtered_data["Total_Stocks"].copy()
            
            # Display data table
            st.subheader("Total Stocks Data")
            st.dataframe(total_stocks_df, use_container_width=True)
            
            # Line chart for NetValue_in_Cr
            try:
                if not total_stocks_df.empty:
                    fig = px.line(
                        total_stocks_df,
                        x="Date",
                        y="NetValue_in_Cr",
                        title="Stocks Net Value Over Time (Cr)",
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
                    st.warning("No total stocks data available for the selected date range.")
            except Exception as e:
                st.error(f"Error displaying net value chart: {e}")
            
            # If NSEI_Close is available, show comparison with market performance
            try:
                if "NSEI_Close" in total_stocks_df.columns and not total_stocks_df.empty:
                    st.subheader("Stocks Net Value vs Nifty Performance")
                    
                    # Create two y-axes chart
                    fig = go.Figure()
                    
                    # First trace for Net Value
                    fig.add_trace(
                        go.Scatter(
                            x=total_stocks_df["Date"],
                            y=total_stocks_df["NetValue_in_Cr"],
                            name="Net Value (Cr)",
                            line=dict(color="blue")
                        )
                    )
                    
                    # Second trace for Nifty close price
                    fig.add_trace(
                        go.Scatter(
                            x=total_stocks_df["Date"],
                            y=total_stocks_df["NSEI_Close"],
                            name="Nifty Close",
                            line=dict(color="red"),
                            yaxis="y2"
                        )
                    )
                    
                    # Update layout for two y-axes
                    fig.update_layout(
                        title="Net Value vs Nifty Performance",
                        xaxis=dict(title="Date"),
                        yaxis=dict(title="Net Value (Cr)"),
                        yaxis2=dict(
                            title=dict(text="Nifty Close", font=dict(color="red", size=20)),
                            tickfont=dict(color="red", size=20),
                            anchor="x",
                            overlaying="y",
                            side="right"
                        ),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        title_font=dict(size=20),
                        legend_font=dict(size=20),
                        xaxis_title_font=dict(size=20),
                        yaxis_title_font=dict(size=20),
                        xaxis_tickfont=dict(size=20),
                        yaxis_tickfont=dict(size=20)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error displaying Nifty comparison chart: {e}")
                
        # Percentage Change Analysis
        with tab6:
            st.header("Percentage Change Analysis")
            
            # Create subtabs for STOCKS and INDEX
            pct_tab1, pct_tab2 = st.tabs(["STOCKS Percentage Change", "INDEX Percentage Change"])
            
            with pct_tab1:
                st.subheader("STOCKS Percentage Change Analysis")
                
                # User inputs for percentage threshold and time period
                col1, col2 = st.columns(2)
                
                with col1:
                    stocks_pct_threshold = st.number_input("Percentage Change Threshold (%)", 
                                                          min_value=1, max_value=100, value=10, key="stocks_pct")
                
                with col2:
                    stocks_days_lookback = st.number_input("Days Lookback Period", 
                                                          min_value=1, max_value=365, value=7, key="stocks_days")
                
                # Apply analysis to STOCKS
                if 'Date' in filtered_data["STOCKS"].columns:
                    # Get the current max date in the filtered data
                    max_date = filtered_data["STOCKS"]["Date"].max()
                    
                    # Calculate lookback date
                    lookback_date = max_date - pd.Timedelta(days=stocks_days_lookback)
                    
                    # Get all data from the lookback period
                    lookback_data = filtered_data["STOCKS"][filtered_data["STOCKS"]["Date"] >= lookback_date]
                    
                    if not lookback_data.empty:
                        # Get the earliest and latest dates for each stock in the lookback period
                        earliest_data = lookback_data.sort_values("Date").groupby("Symbol").first().reset_index()
                        latest_data = lookback_data.sort_values("Date").groupby("Symbol").last().reset_index()
                        
                        # Rename columns to avoid confusion
                        earliest_data = earliest_data.rename(columns={
                            "Date": "StartDate",
                            "NetValue_in_Cr": "StartValue"
                        })
                        
                        latest_data = latest_data.rename(columns={
                            "Date": "EndDate",
                            "NetValue_in_Cr": "EndValue"
                        })
                        
                        # Select only necessary columns
                        earliest_data = earliest_data[["Symbol", "StartDate", "StartValue"]]
                        latest_data = latest_data[["Symbol", "EndDate", "EndValue"]]
                        
                        # Merge data
                        merged_data = pd.merge(earliest_data, latest_data, on="Symbol")
                        
                        # Calculate days between
                        merged_data["DaysBetween"] = (merged_data["EndDate"] - merged_data["StartDate"]).dt.days
                        
                        # Calculate percentage change
                        merged_data["PctChange"] = ((merged_data["EndValue"] - merged_data["StartValue"]) / 
                                                   merged_data["StartValue"].replace(0, float('nan'))) * 100
                        
                        # Replace infinite values with NaN (happens when StartValue is 0)
                        merged_data["PctChange"].replace([float('inf'), float('-inf')], float('nan'), inplace=True)
                        
                        # Filter based on threshold and minimum days
                        min_days_required = max(1, stocks_days_lookback * 0.5)  # At least 50% of requested lookback period
                        valid_data = merged_data[merged_data["DaysBetween"] >= min_days_required]
                        
                        # Drop rows with NaN percentage change
                        valid_data = valid_data.dropna(subset=["PctChange"])
                        
                        significant_changes = valid_data[
                            (valid_data["PctChange"] >= stocks_pct_threshold) | 
                            (valid_data["PctChange"] <= -stocks_pct_threshold)
                        ]
                        
                        # Sort by absolute percentage change (descending)
                        significant_changes["AbsPctChange"] = significant_changes["PctChange"].abs()
                        significant_changes = significant_changes.sort_values("AbsPctChange", ascending=False)
                        
                        # Display results
                        if not significant_changes.empty:
                            # Add formatted columns for display
                            significant_changes["StartValue_Formatted"] = significant_changes["StartValue"].apply(
                                lambda x: '₹{:,.2f} Cr'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["EndValue_Formatted"] = significant_changes["EndValue"].apply(
                                lambda x: '₹{:,.2f} Cr'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["PctChange_Formatted"] = significant_changes["PctChange"].apply(
                                lambda x: '{:+.2f}%'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            
                            # Display the table
                            display_cols = [
                                "Symbol", "StartDate", "EndDate", "StartValue_Formatted", 
                                "EndValue_Formatted", "PctChange_Formatted", "DaysBetween"
                            ]
                            
                            st.subheader(f"Stocks with ≥{stocks_pct_threshold}% Change in Net Value (Last ~{stocks_days_lookback} days)")
                            st.dataframe(significant_changes[display_cols], use_container_width=True)
                            
                            # Create visualization
                            fig = px.bar(
                                significant_changes,
                                x="Symbol",
                                y="PctChange",
                                title=f"Percentage Change in Net Value (≥{stocks_pct_threshold}%)",
                                labels={"PctChange": "% Change", "Symbol": "Stock"},
                                color="PctChange",
                                color_continuous_scale="RdBu",
                                hover_data=["StartValue", "EndValue", "DaysBetween"]
                            )
                            
                            # Update layout
                            fig.update_layout(
                                title_font=dict(size=20),
                                legend_font=dict(size=20),
                                xaxis_title_font=dict(size=20),
                                yaxis_title_font=dict(size=20),
                                xaxis_tickfont=dict(size=20),
                                yaxis_tickfont=dict(size=20)
                            )
                            
                            # Add a horizontal line at y=0
                            fig.add_shape(
                                type="line",
                                x0=-0.5,
                                y0=0,
                                x1=len(significant_changes) - 0.5,
                                y1=0,
                                line=dict(color="black", width=1, dash="dash")
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Add interactive time series visualization for selected stock
                            st.subheader("Detailed Time Series Analysis")
                            
                            # Create a selection box for user to select a stock
                            selected_stock = st.selectbox(
                                "Select a stock to view detailed trend:",
                                options=significant_changes["Symbol"].tolist(),
                                key="stock_detail_selector"
                            )
                            
                            if selected_stock:
                                # Get all data for the selected stock within the lookback period
                                stock_trend_data = lookback_data[lookback_data["Symbol"] == selected_stock]
                                
                                if not stock_trend_data.empty:
                                    # Create trend visualization
                                    st.subheader(f"{selected_stock} Net Value Trend (Last {stocks_days_lookback} Days)")
                                    
                                    # Show beginning and ending values
                                    start_val = stock_trend_data.sort_values("Date")["NetValue_in_Cr"].iloc[0]
                                    end_val = stock_trend_data.sort_values("Date")["NetValue_in_Cr"].iloc[-1]
                                    pct_change = ((end_val - start_val) / start_val) * 100 if start_val != 0 else float('nan')
                                    
                                    metric_cols = st.columns(3)
                                    with metric_cols[0]:
                                        st.metric("Starting Value", f"₹{start_val:,.2f} Cr")
                                    with metric_cols[1]:
                                        st.metric("Ending Value", f"₹{end_val:,.2f} Cr")
                                    with metric_cols[2]:
                                        st.metric("Change", f"{pct_change:+.2f}%", 
                                                 delta_color="normal" if pct_change >= 0 else "inverse")
                                    
                                    # Create the line chart
                                    fig = px.line(
                                        stock_trend_data.sort_values("Date"),
                                        x="Date",
                                        y="NetValue_in_Cr",
                                        title=f"{selected_stock} Net Value Trend",
                                        labels={"NetValue_in_Cr": "Net Value (Cr)", "Date": "Date"},
                                        markers=True
                                    )
                                    
                                    # Add a reference line for the starting value
                                    fig.add_shape(
                                        type="line",
                                        x0=stock_trend_data["Date"].min(),
                                        y0=start_val,
                                        x1=stock_trend_data["Date"].max(),
                                        y1=start_val,
                                        line=dict(color="gray", width=1, dash="dash")
                                    )
                                    
                                    # Customize chart appearance
                                    fig.update_layout(
                                        title_font=dict(size=20),
                                        legend_font=dict(size=20),
                                        xaxis_title_font=dict(size=20),
                                        yaxis_title_font=dict(size=20),
                                        xaxis_tickfont=dict(size=20),
                                        yaxis_tickfont=dict(size=20)
                                    )
                                    
                                    # Determine color based on trend
                                    line_color = "green" if pct_change >= 0 else "red"
                                    fig.update_traces(line_color=line_color)
                                    
                                    # Add annotations for start and end points
                                    fig.add_annotation(
                                        x=stock_trend_data["Date"].min(),
                                        y=start_val,
                                        text="Start",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    fig.add_annotation(
                                        x=stock_trend_data["Date"].max(),
                                        y=end_val,
                                        text="End",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                    
                                    # Additional analysis metrics
                                    with st.expander("Additional Statistics"):
                                        stats_df = pd.DataFrame({
                                            "Metric": ["Mean Value", "Max Value", "Min Value", "Standard Deviation", "Days Tracked"],
                                            "Value": [
                                                f"₹{stock_trend_data['NetValue_in_Cr'].mean():,.2f} Cr",
                                                f"₹{stock_trend_data['NetValue_in_Cr'].max():,.2f} Cr",
                                                f"₹{stock_trend_data['NetValue_in_Cr'].min():,.2f} Cr",
                                                f"₹{stock_trend_data['NetValue_in_Cr'].std():,.2f} Cr",
                                                f"{len(stock_trend_data)} days"
                                            ]
                                        })
                                        st.dataframe(stats_df, use_container_width=True)
                                else:
                                    st.warning(f"No trend data available for {selected_stock} within the selected time period.")
                            
                            # Additionally, show separate tables for rising and falling stocks
                            rising_stocks = significant_changes[significant_changes["PctChange"] > 0]
                            falling_stocks = significant_changes[significant_changes["PctChange"] < 0]
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader(f"Rising Stocks (≥{stocks_pct_threshold}%)")
                                if not rising_stocks.empty:
                                    st.dataframe(rising_stocks[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No stocks found with a rise ≥{stocks_pct_threshold}%.")
                            
                            with col2:
                                st.subheader(f"Falling Stocks (≥{stocks_pct_threshold}%)")
                                if not falling_stocks.empty:
                                    st.dataframe(falling_stocks[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No stocks found with a fall ≥{stocks_pct_threshold}%.")
                        else:
                            st.info(f"No stocks found with changes ≥{stocks_pct_threshold}% in the last ~{stocks_days_lookback} days.")
                    else:
                        st.warning(f"Insufficient data for the selected lookback period of {stocks_days_lookback} days.")
                else:
                    st.warning("Date column not found in STOCKS data.")
            
            with pct_tab2:
                st.subheader("INDEX Percentage Change Analysis")
                
                # User inputs for percentage threshold and time period
                col1, col2 = st.columns(2)
                
                with col1:
                    index_pct_threshold = st.number_input("Percentage Change Threshold (%)", 
                                                         min_value=1, max_value=100, value=5, key="index_pct")
                
                with col2:
                    index_days_lookback = st.number_input("Days Lookback Period", 
                                                         min_value=1, max_value=365, value=7, key="index_days")
                
                # Apply analysis to INDEX
                if 'Date' in filtered_data["INDEX"].columns:
                    # Get the current max date in the filtered data
                    max_date = filtered_data["INDEX"]["Date"].max()
                    
                    # Calculate lookback date
                    lookback_date = max_date - pd.Timedelta(days=index_days_lookback)
                    
                    # Get all data from the lookback period
                    lookback_data = filtered_data["INDEX"][filtered_data["INDEX"]["Date"] >= lookback_date]
                    
                    if not lookback_data.empty:
                        # Get the earliest and latest dates for each index in the lookback period
                        earliest_data = lookback_data.sort_values("Date").groupby("Symbol").first().reset_index()
                        latest_data = lookback_data.sort_values("Date").groupby("Symbol").last().reset_index()
                        
                        # Rename columns to avoid confusion
                        earliest_data = earliest_data.rename(columns={
                            "Date": "StartDate",
                            "NetValue_in_Cr": "StartValue"
                        })
                        
                        latest_data = latest_data.rename(columns={
                            "Date": "EndDate",
                            "NetValue_in_Cr": "EndValue"
                        })
                        
                        # Select only necessary columns
                        earliest_data = earliest_data[["Symbol", "StartDate", "StartValue"]]
                        latest_data = latest_data[["Symbol", "EndDate", "EndValue"]]
                        
                        # Merge data
                        merged_data = pd.merge(earliest_data, latest_data, on="Symbol")
                        
                        # Calculate days between
                        merged_data["DaysBetween"] = (merged_data["EndDate"] - merged_data["StartDate"]).dt.days
                        
                        # Calculate percentage change
                        merged_data["PctChange"] = ((merged_data["EndValue"] - merged_data["StartValue"]) / 
                                                   merged_data["StartValue"].replace(0, float('nan'))) * 100
                        
                        # Replace infinite values with NaN (happens when StartValue is 0)
                        merged_data["PctChange"].replace([float('inf'), float('-inf')], float('nan'), inplace=True)
                        
                        # Filter based on threshold and minimum days
                        min_days_required = max(1, index_days_lookback * 0.5)  # At least 50% of requested lookback period
                        valid_data = merged_data[merged_data["DaysBetween"] >= min_days_required]
                        
                        # Drop rows with NaN percentage change
                        valid_data = valid_data.dropna(subset=["PctChange"])
                        
                        significant_changes = valid_data[
                            (valid_data["PctChange"] >= index_pct_threshold) | 
                            (valid_data["PctChange"] <= -index_pct_threshold)
                        ]
                        
                        # Sort by absolute percentage change (descending)
                        significant_changes["AbsPctChange"] = significant_changes["PctChange"].abs()
                        significant_changes = significant_changes.sort_values("AbsPctChange", ascending=False)
                        
                        # Display results
                        if not significant_changes.empty:
                            # Add formatted columns for display
                            significant_changes["StartValue_Formatted"] = significant_changes["StartValue"].apply(
                                lambda x: '₹{:,.2f} Cr'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["EndValue_Formatted"] = significant_changes["EndValue"].apply(
                                lambda x: '₹{:,.2f} Cr'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["PctChange_Formatted"] = significant_changes["PctChange"].apply(
                                lambda x: '{:+.2f}%'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            
                            # Display the table
                            display_cols = [
                                "Symbol", "StartDate", "EndDate", "StartValue_Formatted", 
                                "EndValue_Formatted", "PctChange_Formatted", "DaysBetween"
                            ]
                            
                            st.subheader(f"Indices with ≥{index_pct_threshold}% Change in Net Value (Last ~{index_days_lookback} days)")
                            st.dataframe(significant_changes[display_cols], use_container_width=True)
                            
                            # Create visualization
                            fig = px.bar(
                                significant_changes,
                                x="Symbol",
                                y="PctChange",
                                title=f"Percentage Change in Net Value (≥{index_pct_threshold}%)",
                                labels={"PctChange": "% Change", "Symbol": "Index"},
                                color="PctChange",
                                color_continuous_scale="RdBu",
                                hover_data=["StartValue", "EndValue", "DaysBetween"]
                            )
                            
                            # Update layout
                            fig.update_layout(
                                title_font=dict(size=20),
                                legend_font=dict(size=20),
                                xaxis_title_font=dict(size=20),
                                yaxis_title_font=dict(size=20),
                                xaxis_tickfont=dict(size=20),
                                yaxis_tickfont=dict(size=20)
                            )
                            
                            # Add a horizontal line at y=0
                            fig.add_shape(
                                type="line",
                                x0=-0.5,
                                y0=0,
                                x1=len(significant_changes) - 0.5,
                                y1=0,
                                line=dict(color="black", width=1, dash="dash")
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Add interactive time series visualization for selected index
                            st.subheader("Detailed Time Series Analysis")
                            
                            # Create a selection box for user to select an index
                            selected_index = st.selectbox(
                                "Select an index to view detailed trend:",
                                options=significant_changes["Symbol"].tolist(),
                                key="index_detail_selector"
                            )
                            
                            if selected_index:
                                # Get all data for the selected index within the lookback period
                                index_trend_data = lookback_data[lookback_data["Symbol"] == selected_index]
                                
                                if not index_trend_data.empty:
                                    # Create trend visualization
                                    st.subheader(f"{selected_index} Net Value Trend (Last {index_days_lookback} Days)")
                                    
                                    # Show beginning and ending values
                                    start_val = index_trend_data.sort_values("Date")["NetValue_in_Cr"].iloc[0]
                                    end_val = index_trend_data.sort_values("Date")["NetValue_in_Cr"].iloc[-1]
                                    pct_change = ((end_val - start_val) / start_val) * 100 if start_val != 0 else float('nan')
                                    
                                    metric_cols = st.columns(3)
                                    with metric_cols[0]:
                                        st.metric("Starting Value", f"₹{start_val:,.2f} Cr")
                                    with metric_cols[1]:
                                        st.metric("Ending Value", f"₹{end_val:,.2f} Cr")
                                    with metric_cols[2]:
                                        st.metric("Change", f"{pct_change:+.2f}%", 
                                                 delta_color="normal" if pct_change >= 0 else "inverse")
                                    
                                    # Create the line chart
                                    fig = px.line(
                                        index_trend_data.sort_values("Date"),
                                        x="Date",
                                        y="NetValue_in_Cr",
                                        title=f"{selected_index} Net Value Trend",
                                        labels={"NetValue_in_Cr": "Net Value (Cr)", "Date": "Date"},
                                        markers=True
                                    )
                                    
                                    # Add a reference line for the starting value
                                    fig.add_shape(
                                        type="line",
                                        x0=index_trend_data["Date"].min(),
                                        y0=start_val,
                                        x1=index_trend_data["Date"].max(),
                                        y1=start_val,
                                        line=dict(color="gray", width=1, dash="dash")
                                    )
                                    
                                    # Customize chart appearance
                                    fig.update_layout(
                                        title_font=dict(size=20),
                                        legend_font=dict(size=20),
                                        xaxis_title_font=dict(size=20),
                                        yaxis_title_font=dict(size=20),
                                        xaxis_tickfont=dict(size=20),
                                        yaxis_tickfont=dict(size=20)
                                    )
                                    
                                    # Determine color based on trend
                                    line_color = "green" if pct_change >= 0 else "red"
                                    fig.update_traces(line_color=line_color)
                                    
                                    # Add annotations for start and end points
                                    fig.add_annotation(
                                        x=index_trend_data["Date"].min(),
                                        y=start_val,
                                        text="Start",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    fig.add_annotation(
                                        x=index_trend_data["Date"].max(),
                                        y=end_val,
                                        text="End",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                    
                                    # Additional analysis metrics
                                    with st.expander("Additional Statistics"):
                                        stats_df = pd.DataFrame({
                                            "Metric": ["Mean Value", "Max Value", "Min Value", "Standard Deviation", "Days Tracked"],
                                            "Value": [
                                                f"₹{index_trend_data['NetValue_in_Cr'].mean():,.2f} Cr",
                                                f"₹{index_trend_data['NetValue_in_Cr'].max():,.2f} Cr",
                                                f"₹{index_trend_data['NetValue_in_Cr'].min():,.2f} Cr",
                                                f"₹{index_trend_data['NetValue_in_Cr'].std():,.2f} Cr",
                                                f"{len(index_trend_data)} days"
                                            ]
                                        })
                                        st.dataframe(stats_df, use_container_width=True)
                                else:
                                    st.warning(f"No trend data available for {selected_index} within the selected time period.")
                            
                            # Additionally, show separate tables for rising and falling indices
                            rising_indices = significant_changes[significant_changes["PctChange"] > 0]
                            falling_indices = significant_changes[significant_changes["PctChange"] < 0]
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader(f"Rising Indices (≥{index_pct_threshold}%)")
                                if not rising_indices.empty:
                                    st.dataframe(rising_indices[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No indices found with a rise ≥{index_pct_threshold}%.")
                            
                            with col2:
                                st.subheader(f"Falling Indices (≥{index_pct_threshold}%)")
                                if not falling_indices.empty:
                                    st.dataframe(falling_indices[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No indices found with a fall ≥{index_pct_threshold}%.")
                        else:
                            st.info(f"No indices found with changes ≥{index_pct_threshold}% in the last ~{index_days_lookback} days.")
                    else:
                        st.warning(f"Insufficient data for the selected lookback period of {index_days_lookback} days.")
                else:
                    st.warning("Date column not found in INDEX data.")

        # Percentage Change (NetQtyFwd) Tab
        with tab7:
            st.header("Percentage Change Analysis (NetQtyCarryFwd)")
            
            # Create subtabs for STOCKS and INDEX
            qty_tab1, qty_tab2 = st.tabs(["STOCKS Qty Percentage Change", "INDEX Qty Percentage Change"])
            
            with qty_tab1:
                st.subheader("STOCKS NetQtyCarryFwd Percentage Change Analysis")
                
                # User inputs for percentage threshold and time period
                col1, col2 = st.columns(2)
                
                with col1:
                    stocks_qty_pct_threshold = st.number_input("Percentage Change Threshold (%)", 
                                                              min_value=1, max_value=100, value=10, key="stocks_qty_pct")
                
                with col2:
                    stocks_qty_days_lookback = st.number_input("Days Lookback Period", 
                                                              min_value=1, max_value=365, value=7, key="stocks_qty_days")
                
                # Apply analysis to STOCKS
                if 'Date' in filtered_data["STOCKS"].columns:
                    # Get the current max date in the filtered data
                    max_date = filtered_data["STOCKS"]["Date"].max()
                    
                    # Calculate lookback date
                    lookback_date = max_date - pd.Timedelta(days=stocks_qty_days_lookback)
                    
                    # Get all data from the lookback period
                    lookback_data = filtered_data["STOCKS"][filtered_data["STOCKS"]["Date"] >= lookback_date]
                    
                    if not lookback_data.empty:
                        # Get the earliest and latest dates for each stock in the lookback period
                        earliest_data = lookback_data.sort_values("Date").groupby("Symbol").first().reset_index()
                        latest_data = lookback_data.sort_values("Date").groupby("Symbol").last().reset_index()
                        
                        # Rename columns to avoid confusion
                        earliest_data = earliest_data.rename(columns={
                            "Date": "StartDate",
                            "NetQtyCarryFwd": "StartQty"
                        })
                        
                        latest_data = latest_data.rename(columns={
                            "Date": "EndDate",
                            "NetQtyCarryFwd": "EndQty"
                        })
                        
                        # Select only necessary columns
                        earliest_data = earliest_data[["Symbol", "StartDate", "StartQty"]]
                        latest_data = latest_data[["Symbol", "EndDate", "EndQty"]]
                        
                        # Merge data
                        merged_data = pd.merge(earliest_data, latest_data, on="Symbol")
                        
                        # Calculate days between
                        merged_data["DaysBetween"] = (merged_data["EndDate"] - merged_data["StartDate"]).dt.days
                        
                        # Calculate percentage change
                        merged_data["PctChange"] = ((merged_data["EndQty"] - merged_data["StartQty"]) / 
                                                   merged_data["StartQty"].replace(0, float('nan'))) * 100
                        
                        # Replace infinite values with NaN (happens when StartQty is 0)
                        merged_data["PctChange"].replace([float('inf'), float('-inf')], float('nan'), inplace=True)
                        
                        # Filter based on threshold and minimum days
                        min_days_required = max(1, stocks_qty_days_lookback * 0.5)  # At least 50% of requested lookback period
                        valid_data = merged_data[merged_data["DaysBetween"] >= min_days_required]
                        
                        # Drop rows with NaN percentage change
                        valid_data = valid_data.dropna(subset=["PctChange"])
                        
                        significant_changes = valid_data[
                            (valid_data["PctChange"] >= stocks_qty_pct_threshold) | 
                            (valid_data["PctChange"] <= -stocks_qty_pct_threshold)
                        ]
                        
                        # Sort by absolute percentage change (descending)
                        significant_changes["AbsPctChange"] = significant_changes["PctChange"].abs()
                        significant_changes = significant_changes.sort_values("AbsPctChange", ascending=False)
                        
                        # Display results
                        if not significant_changes.empty:
                            # Add formatted columns for display
                            significant_changes["StartQty_Formatted"] = significant_changes["StartQty"].apply(
                                lambda x: '{:,.0f}'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["EndQty_Formatted"] = significant_changes["EndQty"].apply(
                                lambda x: '{:,.0f}'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["PctChange_Formatted"] = significant_changes["PctChange"].apply(
                                lambda x: '{:+.2f}%'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            
                            # Display the table
                            display_cols = [
                                "Symbol", "StartDate", "EndDate", "StartQty_Formatted", 
                                "EndQty_Formatted", "PctChange_Formatted", "DaysBetween"
                            ]
                            
                            st.subheader(f"Stocks with ≥{stocks_qty_pct_threshold}% Change in Net Quantity (Last ~{stocks_qty_days_lookback} days)")
                            st.dataframe(significant_changes[display_cols], use_container_width=True)
                            
                            # Create visualization
                            fig = px.bar(
                                significant_changes,
                                x="Symbol",
                                y="PctChange",
                                title=f"Percentage Change in Net Quantity (≥{stocks_qty_pct_threshold}%)",
                                labels={"PctChange": "% Change", "Symbol": "Stock"},
                                color="PctChange",
                                color_continuous_scale="RdBu",
                                hover_data=["StartQty", "EndQty", "DaysBetween"]
                            )
                            
                            # Update layout
                            fig.update_layout(
                                title_font=dict(size=20),
                                legend_font=dict(size=20),
                                xaxis_title_font=dict(size=20),
                                yaxis_title_font=dict(size=20),
                                xaxis_tickfont=dict(size=20),
                                yaxis_tickfont=dict(size=20)
                            )
                            
                            # Add a horizontal line at y=0
                            fig.add_shape(
                                type="line",
                                x0=-0.5,
                                y0=0,
                                x1=len(significant_changes) - 0.5,
                                y1=0,
                                line=dict(color="black", width=1, dash="dash")
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Add interactive time series visualization for selected stock
                            st.subheader("Detailed Time Series Analysis")
                            
                            # Create a selection box for user to select a stock
                            selected_stock = st.selectbox(
                                "Select a stock to view detailed trend:",
                                options=significant_changes["Symbol"].tolist(),
                                key="qty_stock_detail_selector"
                            )
                            
                            if selected_stock:
                                # Get all data for the selected stock within the lookback period
                                stock_trend_data = lookback_data[lookback_data["Symbol"] == selected_stock]
                                
                                if not stock_trend_data.empty:
                                    # Create trend visualization
                                    st.subheader(f"{selected_stock} Net Quantity Trend (Last {stocks_qty_days_lookback} Days)")
                                    
                                    # Show beginning and ending values
                                    start_val = stock_trend_data.sort_values("Date")["NetQtyCarryFwd"].iloc[0]
                                    end_val = stock_trend_data.sort_values("Date")["NetQtyCarryFwd"].iloc[-1]
                                    pct_change = ((end_val - start_val) / start_val) * 100 if start_val != 0 else float('nan')
                                    
                                    metric_cols = st.columns(3)
                                    with metric_cols[0]:
                                        st.metric("Starting Quantity", f"{start_val:,.0f}")
                                    with metric_cols[1]:
                                        st.metric("Ending Quantity", f"{end_val:,.0f}")
                                    with metric_cols[2]:
                                        st.metric("Change", f"{pct_change:+.2f}%", 
                                                 delta_color="normal" if pct_change >= 0 else "inverse")
                                    
                                    # Create the line chart
                                    fig = px.line(
                                        stock_trend_data.sort_values("Date"),
                                        x="Date",
                                        y="NetQtyCarryFwd",
                                        title=f"{selected_stock} Net Quantity Trend",
                                        labels={"NetQtyCarryFwd": "Net Quantity", "Date": "Date"},
                                        markers=True
                                    )
                                    
                                    # Add a reference line for the starting value
                                    fig.add_shape(
                                        type="line",
                                        x0=stock_trend_data["Date"].min(),
                                        y0=start_val,
                                        x1=stock_trend_data["Date"].max(),
                                        y1=start_val,
                                        line=dict(color="gray", width=1, dash="dash")
                                    )
                                    
                                    # Customize chart appearance
                                    fig.update_layout(
                                        title_font=dict(size=20),
                                        legend_font=dict(size=20),
                                        xaxis_title_font=dict(size=20),
                                        yaxis_title_font=dict(size=20),
                                        xaxis_tickfont=dict(size=20),
                                        yaxis_tickfont=dict(size=20)
                                    )
                                    
                                    # Determine color based on trend
                                    line_color = "green" if pct_change >= 0 else "red"
                                    fig.update_traces(line_color=line_color)
                                    
                                    # Add annotations for start and end points
                                    fig.add_annotation(
                                        x=stock_trend_data["Date"].min(),
                                        y=start_val,
                                        text="Start",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    fig.add_annotation(
                                        x=stock_trend_data["Date"].max(),
                                        y=end_val,
                                        text="End",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                    
                                    # Additional analysis metrics
                                    with st.expander("Additional Statistics"):
                                        stats_df = pd.DataFrame({
                                            "Metric": ["Mean Quantity", "Max Quantity", "Min Quantity", "Standard Deviation", "Days Tracked"],
                                            "Value": [
                                                f"{stock_trend_data['NetQtyCarryFwd'].mean():,.0f}",
                                                f"{stock_trend_data['NetQtyCarryFwd'].max():,.0f}",
                                                f"{stock_trend_data['NetQtyCarryFwd'].min():,.0f}",
                                                f"{stock_trend_data['NetQtyCarryFwd'].std():,.0f}",
                                                f"{len(stock_trend_data)} days"
                                            ]
                                        })
                                        st.dataframe(stats_df, use_container_width=True)
                                else:
                                    st.warning(f"No trend data available for {selected_stock} within the selected time period.")
                            
                            # Additionally, show separate tables for rising and falling stocks
                            rising_stocks = significant_changes[significant_changes["PctChange"] > 0]
                            falling_stocks = significant_changes[significant_changes["PctChange"] < 0]
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader(f"Rising Stocks (≥{stocks_qty_pct_threshold}%)")
                                if not rising_stocks.empty:
                                    st.dataframe(rising_stocks[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No stocks found with a rise ≥{stocks_qty_pct_threshold}%.")
                            
                            with col2:
                                st.subheader(f"Falling Stocks (≥{stocks_qty_pct_threshold}%)")
                                if not falling_stocks.empty:
                                    st.dataframe(falling_stocks[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No stocks found with a fall ≥{stocks_qty_pct_threshold}%.")
                        else:
                            st.info(f"No stocks found with changes ≥{stocks_qty_pct_threshold}% in the last ~{stocks_qty_days_lookback} days.")
                    else:
                        st.warning(f"Insufficient data for the selected lookback period of {stocks_qty_days_lookback} days.")
                else:
                    st.warning("Date column not found in STOCKS data.")
            
            with qty_tab2:
                st.subheader("INDEX NetQtyCarryFwd Percentage Change Analysis")
                
                # User inputs for percentage threshold and time period
                col1, col2 = st.columns(2)
                
                with col1:
                    index_qty_pct_threshold = st.number_input("Percentage Change Threshold (%)", 
                                                             min_value=1, max_value=100, value=5, key="index_qty_pct")
                
                with col2:
                    index_qty_days_lookback = st.number_input("Days Lookback Period", 
                                                             min_value=1, max_value=365, value=7, key="index_qty_days")
                
                # Apply analysis to INDEX
                if 'Date' in filtered_data["INDEX"].columns:
                    # Get the current max date in the filtered data
                    max_date = filtered_data["INDEX"]["Date"].max()
                    
                    # Calculate lookback date
                    lookback_date = max_date - pd.Timedelta(days=index_qty_days_lookback)
                    
                    # Get all data from the lookback period
                    lookback_data = filtered_data["INDEX"][filtered_data["INDEX"]["Date"] >= lookback_date]
                    
                    if not lookback_data.empty:
                        # Get the earliest and latest dates for each index in the lookback period
                        earliest_data = lookback_data.sort_values("Date").groupby("Symbol").first().reset_index()
                        latest_data = lookback_data.sort_values("Date").groupby("Symbol").last().reset_index()
                        
                        # Rename columns to avoid confusion
                        earliest_data = earliest_data.rename(columns={
                            "Date": "StartDate",
                            "NetQtyCarryFwd": "StartQty"
                        })
                        
                        latest_data = latest_data.rename(columns={
                            "Date": "EndDate",
                            "NetQtyCarryFwd": "EndQty"
                        })
                        
                        # Select only necessary columns
                        earliest_data = earliest_data[["Symbol", "StartDate", "StartQty"]]
                        latest_data = latest_data[["Symbol", "EndDate", "EndQty"]]
                        
                        # Merge data
                        merged_data = pd.merge(earliest_data, latest_data, on="Symbol")
                        
                        # Calculate days between
                        merged_data["DaysBetween"] = (merged_data["EndDate"] - merged_data["StartDate"]).dt.days
                        
                        # Calculate percentage change
                        merged_data["PctChange"] = ((merged_data["EndQty"] - merged_data["StartQty"]) / 
                                                   merged_data["StartQty"].replace(0, float('nan'))) * 100
                        
                        # Replace infinite values with NaN (happens when StartQty is 0)
                        merged_data["PctChange"].replace([float('inf'), float('-inf')], float('nan'), inplace=True)
                        
                        # Filter based on threshold and minimum days
                        min_days_required = max(1, index_qty_days_lookback * 0.5)  # At least 50% of requested lookback period
                        valid_data = merged_data[merged_data["DaysBetween"] >= min_days_required]
                        
                        # Drop rows with NaN percentage change
                        valid_data = valid_data.dropna(subset=["PctChange"])
                        
                        significant_changes = valid_data[
                            (valid_data["PctChange"] >= index_qty_pct_threshold) | 
                            (valid_data["PctChange"] <= -index_qty_pct_threshold)
                        ]
                        
                        # Sort by absolute percentage change (descending)
                        significant_changes["AbsPctChange"] = significant_changes["PctChange"].abs()
                        significant_changes = significant_changes.sort_values("AbsPctChange", ascending=False)
                        
                        # Display results
                        if not significant_changes.empty:
                            # Add formatted columns for display
                            significant_changes["StartQty_Formatted"] = significant_changes["StartQty"].apply(
                                lambda x: '{:,.0f}'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["EndQty_Formatted"] = significant_changes["EndQty"].apply(
                                lambda x: '{:,.0f}'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            significant_changes["PctChange_Formatted"] = significant_changes["PctChange"].apply(
                                lambda x: '{:+.2f}%'.format(x) if pd.notnull(x) else 'N/A'
                            )
                            
                            # Display the table
                            display_cols = [
                                "Symbol", "StartDate", "EndDate", "StartQty_Formatted", 
                                "EndQty_Formatted", "PctChange_Formatted", "DaysBetween"
                            ]
                            
                            st.subheader(f"Indices with ≥{index_qty_pct_threshold}% Change in Net Quantity (Last ~{index_qty_days_lookback} days)")
                            st.dataframe(significant_changes[display_cols], use_container_width=True)
                            
                            # Create visualization
                            fig = px.bar(
                                significant_changes,
                                x="Symbol",
                                y="PctChange",
                                title=f"Percentage Change in Net Quantity (≥{index_qty_pct_threshold}%)",
                                labels={"PctChange": "% Change", "Symbol": "Index"},
                                color="PctChange",
                                color_continuous_scale="RdBu",
                                hover_data=["StartQty", "EndQty", "DaysBetween"]
                            )
                            
                            # Update layout
                            fig.update_layout(
                                title_font=dict(size=20),
                                legend_font=dict(size=20),
                                xaxis_title_font=dict(size=20),
                                yaxis_title_font=dict(size=20),
                                xaxis_tickfont=dict(size=20),
                                yaxis_tickfont=dict(size=20)
                            )
                            
                            # Add a horizontal line at y=0
                            fig.add_shape(
                                type="line",
                                x0=-0.5,
                                y0=0,
                                x1=len(significant_changes) - 0.5,
                                y1=0,
                                line=dict(color="black", width=1, dash="dash")
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Add interactive time series visualization for selected index
                            st.subheader("Detailed Time Series Analysis")
                            
                            # Create a selection box for user to select an index
                            selected_index = st.selectbox(
                                "Select an index to view detailed trend:",
                                options=significant_changes["Symbol"].tolist(),
                                key="qty_index_detail_selector"
                            )
                            
                            if selected_index:
                                # Get all data for the selected index within the lookback period
                                index_trend_data = lookback_data[lookback_data["Symbol"] == selected_index]
                                
                                if not index_trend_data.empty:
                                    # Create trend visualization
                                    st.subheader(f"{selected_index} Net Quantity Trend (Last {index_qty_days_lookback} Days)")
                                    
                                    # Show beginning and ending values
                                    start_val = index_trend_data.sort_values("Date")["NetQtyCarryFwd"].iloc[0]
                                    end_val = index_trend_data.sort_values("Date")["NetQtyCarryFwd"].iloc[-1]
                                    pct_change = ((end_val - start_val) / start_val) * 100 if start_val != 0 else float('nan')
                                    
                                    metric_cols = st.columns(3)
                                    with metric_cols[0]:
                                        st.metric("Starting Quantity", f"{start_val:,.0f}")
                                    with metric_cols[1]:
                                        st.metric("Ending Quantity", f"{end_val:,.0f}")
                                    with metric_cols[2]:
                                        st.metric("Change", f"{pct_change:+.2f}%", 
                                                 delta_color="normal" if pct_change >= 0 else "inverse")
                                    
                                    # Create the line chart
                                    fig = px.line(
                                        index_trend_data.sort_values("Date"),
                                        x="Date",
                                        y="NetQtyCarryFwd",
                                        title=f"{selected_index} Net Quantity Trend",
                                        labels={"NetQtyCarryFwd": "Net Quantity", "Date": "Date"},
                                        markers=True
                                    )
                                    
                                    # Add a reference line for the starting value
                                    fig.add_shape(
                                        type="line",
                                        x0=index_trend_data["Date"].min(),
                                        y0=start_val,
                                        x1=index_trend_data["Date"].max(),
                                        y1=start_val,
                                        line=dict(color="gray", width=1, dash="dash")
                                    )
                                    
                                    # Customize chart appearance
                                    fig.update_layout(
                                        title_font=dict(size=20),
                                        legend_font=dict(size=20),
                                        xaxis_title_font=dict(size=20),
                                        yaxis_title_font=dict(size=20),
                                        xaxis_tickfont=dict(size=20),
                                        yaxis_tickfont=dict(size=20)
                                    )
                                    
                                    # Determine color based on trend
                                    line_color = "green" if pct_change >= 0 else "red"
                                    fig.update_traces(line_color=line_color)
                                    
                                    # Add annotations for start and end points
                                    fig.add_annotation(
                                        x=index_trend_data["Date"].min(),
                                        y=start_val,
                                        text="Start",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    fig.add_annotation(
                                        x=index_trend_data["Date"].max(),
                                        y=end_val,
                                        text="End",
                                        showarrow=True,
                                        arrowhead=1
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                    
                                    # Additional analysis metrics
                                    with st.expander("Additional Statistics"):
                                        stats_df = pd.DataFrame({
                                            "Metric": ["Mean Quantity", "Max Quantity", "Min Quantity", "Standard Deviation", "Days Tracked"],
                                            "Value": [
                                                f"{index_trend_data['NetQtyCarryFwd'].mean():,.0f}",
                                                f"{index_trend_data['NetQtyCarryFwd'].max():,.0f}",
                                                f"{index_trend_data['NetQtyCarryFwd'].min():,.0f}",
                                                f"{index_trend_data['NetQtyCarryFwd'].std():,.0f}",
                                                f"{len(index_trend_data)} days"
                                            ]
                                        })
                                        st.dataframe(stats_df, use_container_width=True)
                                else:
                                    st.warning(f"No trend data available for {selected_index} within the selected time period.")
                            
                            # Additionally, show separate tables for rising and falling indices
                            rising_indices = significant_changes[significant_changes["PctChange"] > 0]
                            falling_indices = significant_changes[significant_changes["PctChange"] < 0]
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader(f"Rising Indices (≥{index_qty_pct_threshold}%)")
                                if not rising_indices.empty:
                                    st.dataframe(rising_indices[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No indices found with a rise ≥{index_qty_pct_threshold}%.")
                            
                            with col2:
                                st.subheader(f"Falling Indices (≥{index_qty_pct_threshold}%)")
                                if not falling_indices.empty:
                                    st.dataframe(falling_indices[display_cols], use_container_width=True)
                                else:
                                    st.info(f"No indices found with a fall ≥{index_qty_pct_threshold}%.")
                        else:
                            st.info(f"No indices found with changes ≥{index_qty_pct_threshold}% in the last ~{index_qty_days_lookback} days.")
                    else:
                        st.warning(f"Insufficient data for the selected lookback period of {index_qty_days_lookback} days.")
                else:
                    st.warning("Date column not found in INDEX data.")

        # Raw Data tab
        with tab8:
            st.header("Raw Data")
            
            # Define columns to hide by default for each table
            columns_to_hide = {
                "INDEX": ["id", "created_at", "updated_at", "BtFrwdLongQty", "BtFrwdShortQty"],
                "STOCKS": ["id", "created_at", "updated_at", "BtFrwdLongQty", "BtFrwdShortQty"],
                "SUMMARY": ["id", "created_at", "updated_at"],
                "Total_Index": ["id", "created_at", "updated_at"],
                "Total_Stocks": ["id", "created_at", "updated_at"]
            }
            
            # Toggle for showing all columns
            show_all_columns = st.checkbox("Show All Columns", value=False)
            
            # Create subtabs for each table
            sheet_tabs = st.tabs(list(data.keys()))
            
            for i, sheet_name in enumerate(data.keys()):
                with sheet_tabs[i]:
                    st.subheader(f"{sheet_name} Data")
                    
                    # Filter columns if needed
                    display_df = data[sheet_name].copy()
                    
                    if not show_all_columns and sheet_name in columns_to_hide:
                        # Filter out columns that should be hidden
                        cols_to_hide = [col for col in columns_to_hide[sheet_name] if col in display_df.columns]
                        display_df = display_df.drop(columns=cols_to_hide)
                    
                    # Display the filtered dataframe
                    st.dataframe(display_df, use_container_width=True)
                    
                    # Add download button for each table (always with all columns)
                    csv = data[sheet_name].to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label=f"Download {sheet_name} as CSV",
                        data=csv,
                        file_name=f"{sheet_name}.csv",
                        mime="text/csv",
                    )
                    
                    # Show column selector for custom view
                    if st.checkbox(f"Custom Column Selection for {sheet_name}", value=False):
                        all_columns = list(data[sheet_name].columns)
                        selected_columns = st.multiselect(
                            f"Select columns to display for {sheet_name}",
                            all_columns,
                            default=[col for col in all_columns if col not in columns_to_hide.get(sheet_name, [])]
                        )
                        
                        if selected_columns:
                            st.dataframe(data[sheet_name][selected_columns], use_container_width=True)

        # Add information and credits
        st.sidebar.markdown("---")
        st.sidebar.info(
            """
            This dashboard visualizes market data for indices and stocks.
            Data is loaded from PostgreSQL database.
            """
        )
    else:
        st.warning("No data available in the database. Please use the sidebar to generate data.")
else:
    st.warning("Database connection required. Please check connection parameters and ensure the database is running.")
    
    # Show advanced connection troubleshooting but hide credentials
    with st.expander("Connection Troubleshooting"):
        st.markdown(f"""
        ### Database Connection Details
        
        - **Host**: {PG_HOST}
        - **Port**: {PG_PORT}
        - **Database**: {PG_DATABASE}
        - **User**: {PG_USER}
        
        ### Common Issues:
        
        1. **Database server not running**
           - Ensure PostgreSQL is running on the server
        
        2. **Network connectivity**
           - Check that firewall rules allow connections to port {PG_PORT}
           - Verify network connectivity to {PG_HOST}
           
        3. **Database not initialized**
           - Make sure the tables have been created using the setup script
           - Try clicking 'Initialize Database' in the sidebar
        
        4. **Secrets configuration**
           - Verify your `.streamlit/secrets.toml` file is properly configured
           - When deploying, ensure secrets are set in your deployment environment
        """)
