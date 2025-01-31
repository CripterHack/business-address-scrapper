"""Main Streamlit application for the Business Address Scraper."""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from streamlit_option_menu import option_menu
from st_aggrid import AgGrid, GridOptionsBuilder
from contextlib import contextmanager
from typing import Optional

from scraper.database import Database
from scraper.settings import DatabaseSettings
from scraper.metrics import MetricsManager
from scraper.cache import CacheManager
from scraper.exceptions import DatabaseError, CacheError

# Configuración de la página
st.set_page_config(
    page_title="Business Address Scraper",
    page_icon="🏢",
    layout="wide"
)

# Estilos CSS personalizados
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stButton>button {
        width: 100%;
    }
    .reportview-container .main .block-container {
        padding-top: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Singleton instances
_database: Optional[Database] = None
_metrics: Optional[MetricsManager] = None
_cache: Optional[CacheManager] = None

@contextmanager
def handle_errors():
    """Context manager for handling application errors."""
    try:
        yield
    except DatabaseError as e:
        st.error(f"Error de base de datos: {str(e)}")
    except CacheError as e:
        st.error(f"Error de caché: {str(e)}")
    except Exception as e:
        st.error(f"Error inesperado: {str(e)}")

def initialize_services():
    """Initialize all services as singletons."""
    global _database, _metrics, _cache
    
    if _database is None:
        _database = Database(DatabaseSettings())
    
    if _metrics is None:
        _metrics = MetricsManager()
    
    if _cache is None:
        _cache = CacheManager()

def get_database() -> Database:
    """Get database singleton instance."""
    if _database is None:
        initialize_services()
    return _database

def get_metrics() -> MetricsManager:
    """Get metrics singleton instance."""
    if _metrics is None:
        initialize_services()
    return _metrics

def get_cache() -> CacheManager:
    """Get cache singleton instance."""
    if _cache is None:
        initialize_services()
    return _cache

def main():
    """Main application."""
    # Barra lateral con menú de navegación
    with st.sidebar:
        selected = option_menu(
            "Main Menu",
            ["Dashboard", "Search", "Data Management", "Settings", "Metrics"],
            icons=['house', 'search', 'database', 'gear', 'graph-up'],
            menu_icon="cast",
            default_index=0
        )
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### System Status")
        
        # Show connection status
        db = get_database()
        metrics = get_metrics()
        cache = get_cache()
        
        try:
            db.execute("SELECT 1")
            st.sidebar.success("Database: Connected")
        except Exception as e:
            st.sidebar.error("Database: Connection error")
        
        if os.getenv('CACHE_TYPE') == 'redis':
            try:
                cache.backend.client.ping()
                st.sidebar.success("Redis: Connected")
            except:
                st.sidebar.error("Redis: Connection error")

    # Contenido principal basado en la selección
    if selected == "Dashboard":
        show_dashboard()
    elif selected == "Search":
        show_search()
    elif selected == "Data Management":
        show_data_management()
    elif selected == "Settings":
        show_configuration()
    elif selected == "Metrics":
        show_metrics()

def show_dashboard():
    """Display dashboard page."""
    st.title("📊 Dashboard")
    
    with handle_errors():
        # Métricas principales
        col1, col2, col3, col4 = st.columns(4)
        
        db = get_database()
        metrics = get_metrics()
        
        # Cache los resultados de las consultas frecuentes
        @st.cache_data(ttl=300)  # Cache for 5 minutes
        def get_dashboard_metrics():
            return {
                'total': len(db.fetch_all("SELECT id FROM businesses")),
                'verified': len(db.fetch_all("SELECT id FROM businesses WHERE verified = true")),
                'states': len(db.fetch_all("SELECT DISTINCT state FROM businesses")),
                'recent': len(db.fetch_all(
                    "SELECT id FROM businesses WHERE created_at > %s",
                    (datetime.now() - timedelta(days=7),)
                ))
            }
        
        metrics_data = get_dashboard_metrics()
        
        with col1:
            st.metric("Total Businesses", metrics_data['total'])
        with col2:
            st.metric("Verified Businesses", metrics_data['verified'])
        with col3:
            st.metric("Covered States", metrics_data['states'])
        with col4:
            st.metric("Added (7 days)", metrics_data['recent'])
        
        # Cache los datos de los gráficos
        @st.cache_data(ttl=300)
        def get_dashboard_charts():
            return {
                'states': pd.DataFrame(db.fetch_all("""
                    SELECT state, COUNT(*) as count 
                    FROM businesses 
                    GROUP BY state 
                    ORDER BY count DESC
                """)),
                'violations': pd.DataFrame(db.fetch_all("""
                    SELECT violation_type, COUNT(*) as count 
                    FROM businesses 
                    GROUP BY violation_type 
                    ORDER BY count DESC
                """))
            }
        
        charts_data = get_dashboard_charts()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Businesses by State")
            if not charts_data['states'].empty:
                fig = px.bar(charts_data['states'], x='state', y='count')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Violation Types")
            if not charts_data['violations'].empty:
                fig = px.pie(charts_data['violations'], values='count', names='violation_type')
                st.plotly_chart(fig, use_container_width=True)

def show_search():
    """Display search page."""
    st.title("🔍 Search for Businesses")
    
    with handle_errors():
        # Cache los datos de los filtros
        @st.cache_data(ttl=300)
        def get_filter_options():
            db = get_database()
            return {
                'states': sorted(list(set(
                    row['state'] for row in db.fetch_all("SELECT DISTINCT state FROM businesses")
                ))),
                'violations': sorted(list(set(
                    row['violation_type'] for row in db.fetch_all("SELECT DISTINCT violation_type FROM businesses")
                )))
            }
        
        filter_options = get_filter_options()
        
        # Filtros de búsqueda
        col1, col2, col3 = st.columns(3)
        
        with col1:
            state = st.selectbox(
                "State",
                options=["All"] + filter_options['states']
            )
        
        with col2:
            violation_type = st.selectbox(
                "Violation Type",
                options=["All"] + filter_options['violations']
            )
        
        with col3:
            verified = st.selectbox("Verification Status", ["All", "Verified", "Unverified"])
        
        # Build query safely
        query_parts = ["SELECT * FROM businesses WHERE 1=1"]
        params = []
        
        if state != "All":
            query_parts.append("AND state = %s")
            params.append(state)
        
        if violation_type != "All":
            query_parts.append("AND violation_type = %s")
            params.append(violation_type)
        
        if verified != "All":
            query_parts.append("AND verified = %s")
            params.append(verified == "Verified")
        
        # Add sorting and limit
        query_parts.append("ORDER BY created_at DESC LIMIT 1000")
        
        # Ejecutar búsqueda
        results = get_database().fetch_all(" ".join(query_parts), tuple(params) if params else None)
        
        if results:
            df = pd.DataFrame(results)
            
            # Configure grid with enhanced options
            gb = GridOptionsBuilder.from_dataframe(df)
            gb.configure_pagination(paginationAutoPageSize=True)
            gb.configure_side_bar()
            gb.configure_selection('single')
            gb.configure_column('created_at', type=['dateColumnFilter', 'customDateTimeFormat'], custom_format_string='yyyy-MM-dd HH:mm:ss')
            gb.configure_column('updated_at', type=['dateColumnFilter', 'customDateTimeFormat'], custom_format_string='yyyy-MM-dd HH:mm:ss')
            
            grid_response = AgGrid(
                df,
                gridOptions=gb.build(),
                data_return_mode='AS_INPUT', 
                update_mode='MODEL_CHANGED',
                fit_columns_on_grid_load=True,
                enable_enterprise_modules=True,
                height=400,
                width='100%',
                reload_data=False,
                allow_unsafe_jscode=True
            )
            
            # Show details if a row is selected
            selected = grid_response['selected_rows']
            if selected:
                st.subheader("Business Details")
                
                # Format dates for better visualization
                formatted_data = selected[0].copy()
                for key in ['created_at', 'updated_at', 'nsl_published_date', 'nsl_effective_date', 'remediated_date']:
                    if key in formatted_data and formatted_data[key]:
                        formatted_data[key] = pd.to_datetime(formatted_data[key]).strftime('%Y-%m-%d %H:%M:%S')
                
                st.json(formatted_data)
        else:
            st.info("No results found")

def show_data_management():
    """Display data management page."""
    st.title("💾 Data Management")
    
    tab1, tab2 = st.tabs(["Import Data", "Export Data"])
    
    with tab1:
        st.subheader("Import Data")
        st.write("Upload a CSV file with a 'business_name' column containing the names of businesses to search.")
        
        uploaded_file = st.file_uploader("Select a CSV file", type="csv")
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file)
                st.write("Data preview:")
                st.write(df.head())
                
                # Basic column validation
                required_columns = ['business_name']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    st.error(f"The CSV file must contain the following columns: {', '.join(missing_columns)}")
                else:
                    if st.button("Start Scraping"):
                        with st.spinner("Starting scraping process..."):
                            try:
                                # Save CSV file to temporary location
                                temp_input_file = os.path.join('temp', 'uploads', 'current_input.csv')
                                os.makedirs(os.path.dirname(temp_input_file), exist_ok=True)
                                df.to_csv(temp_input_file, index=False)
                                
                                # Start scraping process
                                from scrapy.crawler import CrawlerProcess
                                from scrapy.utils.project import get_project_settings
                                from scraper.spiders.business_spider import BusinessSpider
                                from multiprocessing import Process
                                
                                settings = get_project_settings()
                                
                                def run_spider_process():
                                    process = CrawlerProcess(settings)
                                    process.crawl(BusinessSpider)
                                    process.start()
                                
                                # Run spider in a separate process
                                spider_process = Process(target=run_spider_process)
                                spider_process.start()
                                
                                st.success(f"Scraping process started for {len(df)} businesses")
                                st.info("You can monitor the progress in the metrics tab")
                                
                            except Exception as e:
                                st.error(f"Error starting scraping: {str(e)}")
            except Exception as e:
                st.error(f"Error reading CSV file: {str(e)}")
    
    with tab2:
        st.subheader("Export Data")
        
        # Export options
        col1, col2 = st.columns(2)
        with col1:
            export_type = st.selectbox("Export Format", ["CSV", "Excel", "JSON"])
        with col2:
            verified_only = st.checkbox("Export only verified businesses", value=False)
        
        if st.button("Export Data"):
            with st.spinner("Exporting data..."):
                try:
                    db = get_database()
                    
                    # Build query based on filters
                    query = "SELECT * FROM businesses"
                    if verified_only:
                        query += " WHERE verified = true"
                    query += " ORDER BY created_at DESC"
                    
                    # Get data
                    results = db.fetch_all(query)
                    
                    if not results:
                        st.warning("No data available for export")
                        return
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(results)
                    
                    # Generate filename with timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    # Export according to selected format
                    if export_type == "CSV":
                        output = df.to_csv(index=False)
                        file_name = f"businesses_{timestamp}.csv"
                        mime_type = "text/csv"
                    elif export_type == "Excel":
                        output = df.to_excel(index=False)
                        file_name = f"businesses_{timestamp}.xlsx"
                        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    else:  # JSON
                        output = df.to_json(orient="records")
                        file_name = f"businesses_{timestamp}.json"
                        mime_type = "application/json"
                    
                    # Offer download
                    st.download_button(
                        label="Download File",
                        data=output,
                        file_name=file_name,
                        mime=mime_type
                    )
                    
                    st.success(f"Data exported successfully: {len(results)} records")
                    
                except Exception as e:
                    st.error(f"Error during export: {str(e)}")

def show_configuration():
    """Display configuration page."""
    st.title("⚙️ Settings")
    
    # Database configuration
    st.subheader("Database Configuration")
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Host", value=os.getenv("DB_HOST", "localhost"), disabled=True)
        st.text_input("Port", value=os.getenv("DB_PORT", "5432"), disabled=True)
    with col2:
        st.text_input("Database", value=os.getenv("DB_NAME", "business_scraper"), disabled=True)
        st.text_input("User", value=os.getenv("DB_USER", "postgres"), disabled=True)
    
    # Cache configuration
    st.subheader("Cache Configuration")
    st.text_input("Cache Type", value=os.getenv("CACHE_TYPE", "memory"), disabled=True)
    st.text_input("Cache TTL", value=os.getenv("CACHE_TTL", "3600"), disabled=True)
    
    # Scraper configuration
    st.subheader("Scraper Configuration")
    col1, col2 = st.columns(2)
    with col1:
        st.number_input("Threads", value=int(os.getenv("SCRAPER_THREADS", "4")))
        st.number_input("Request Timeout", value=int(os.getenv("REQUEST_TIMEOUT", "30")))
    with col2:
        st.number_input("Max Retries", value=int(os.getenv("MAX_RETRIES", "3")))
        st.text_input("User Agent", value=os.getenv("USER_AGENT", ""))

def show_metrics():
    """Display metrics page."""
    st.title("📈 Metrics")
    
    metrics = get_metrics()
    report = metrics.get_report()
    
    # Performance metrics
    st.subheader("Performance")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("CPU Usage", f"{report.performance['cpu_percent']}%")
    with col2:
        st.metric("Memory Usage", f"{report.performance['memory_mb']:.2f} MB")
    with col3:
        st.metric("DB Connections", report.database['connections'])
    
    # Cache metrics
    st.subheader("Cache")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Cache Hits", report.cache['hits'])
    with col2:
        st.metric("Cache Misses", report.cache['misses'])
    
    # Error metrics
    st.subheader("Errors by Type")
    error_df = pd.DataFrame(
        list(report.errors.items()),
        columns=['Error Type', 'Count']
    )
    if not error_df.empty:
        fig = px.bar(error_df, x='Error Type', y='Count')
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()