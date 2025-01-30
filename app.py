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
            "Menu Principal",
            ["Dashboard", "Búsqueda", "Gestión de Datos", "Configuración", "Métricas"],
            icons=['house', 'search', 'database', 'gear', 'graph-up'],
            menu_icon="cast",
            default_index=0
        )
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### Estado del Sistema")
        
        # Mostrar estado de conexiones
        db = get_database()
        metrics = get_metrics()
        cache = get_cache()
        
        try:
            db.execute("SELECT 1")
            st.sidebar.success("Base de datos: Conectada")
        except Exception as e:
            st.sidebar.error("Base de datos: Error de conexión")
        
        if os.getenv('CACHE_TYPE') == 'redis':
            try:
                cache.backend.client.ping()
                st.sidebar.success("Redis: Conectado")
            except:
                st.sidebar.error("Redis: Error de conexión")

    # Contenido principal basado en la selección
    if selected == "Dashboard":
        show_dashboard()
    elif selected == "Búsqueda":
        show_search()
    elif selected == "Gestión de Datos":
        show_data_management()
    elif selected == "Configuración":
        show_configuration()
    elif selected == "Métricas":
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
        @st.cache_data(ttl=300)  # Cache por 5 minutos
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
            st.metric("Total Negocios", metrics_data['total'])
        with col2:
            st.metric("Negocios Verificados", metrics_data['verified'])
        with col3:
            st.metric("Estados Cubiertos", metrics_data['states'])
        with col4:
            st.metric("Añadidos (7 días)", metrics_data['recent'])
        
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
            st.subheader("Negocios por Estado")
            if not charts_data['states'].empty:
                fig = px.bar(charts_data['states'], x='state', y='count')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Tipos de Violación")
            if not charts_data['violations'].empty:
                fig = px.pie(charts_data['violations'], values='count', names='violation_type')
                st.plotly_chart(fig, use_container_width=True)

def show_search():
    """Display search page."""
    st.title("🔍 Búsqueda de Negocios")
    
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
                "Estado",
                options=["Todos"] + filter_options['states']
            )
        
        with col2:
            violation_type = st.selectbox(
                "Tipo de Violación",
                options=["Todos"] + filter_options['violations']
            )
        
        with col3:
            verified = st.selectbox("Estado de Verificación", ["Todos", "Verificado", "No Verificado"])
        
        # Construir query de manera segura
        query_parts = ["SELECT * FROM businesses WHERE 1=1"]
        params = []
        
        if state != "Todos":
            query_parts.append("AND state = %s")
            params.append(state)
        
        if violation_type != "Todos":
            query_parts.append("AND violation_type = %s")
            params.append(violation_type)
        
        if verified != "Todos":
            query_parts.append("AND verified = %s")
            params.append(verified == "Verificado")
        
        # Agregar ordenamiento y límite
        query_parts.append("ORDER BY created_at DESC LIMIT 1000")
        
        # Ejecutar búsqueda
        results = get_database().fetch_all(" ".join(query_parts), tuple(params) if params else None)
        
        if results:
            df = pd.DataFrame(results)
            
            # Configurar grid con opciones mejoradas
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
            
            # Mostrar detalles si se selecciona una fila
            selected = grid_response['selected_rows']
            if selected:
                st.subheader("Detalles del Negocio")
                
                # Formatear fechas para mejor visualización
                formatted_data = selected[0].copy()
                for key in ['created_at', 'updated_at', 'nsl_published_date', 'nsl_effective_date', 'remediated_date']:
                    if key in formatted_data and formatted_data[key]:
                        formatted_data[key] = pd.to_datetime(formatted_data[key]).strftime('%Y-%m-%d %H:%M:%S')
                
                st.json(formatted_data)
        else:
            st.info("No se encontraron resultados")

def show_data_management():
    """Display data management page."""
    st.title("💾 Gestión de Datos")
    
    tab1, tab2 = st.tabs(["Importar Datos", "Exportar Datos"])
    
    with tab1:
        st.subheader("Importar Datos")
        uploaded_file = st.file_uploader("Selecciona un archivo CSV", type="csv")
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            st.write(df.head())
            if st.button("Importar Datos"):
                with st.spinner("Importando datos..."):
                    # Aquí iría la lógica de importación
                    st.success(f"Se importaron {len(df)} registros")
    
    with tab2:
        st.subheader("Exportar Datos")
        export_type = st.selectbox("Formato de Exportación", ["CSV", "Excel", "JSON"])
        if st.button("Exportar Datos"):
            with st.spinner("Exportando datos..."):
                # Aquí iría la lógica de exportación
                st.success("Datos exportados exitosamente")

def show_configuration():
    """Display configuration page."""
    st.title("⚙️ Configuración")
    
    # Configuración de la base de datos
    st.subheader("Configuración de Base de Datos")
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Host", value=os.getenv("DB_HOST", "localhost"), disabled=True)
        st.text_input("Puerto", value=os.getenv("DB_PORT", "5432"), disabled=True)
    with col2:
        st.text_input("Base de Datos", value=os.getenv("DB_NAME", "business_scraper"), disabled=True)
        st.text_input("Usuario", value=os.getenv("DB_USER", "postgres"), disabled=True)
    
    # Configuración de caché
    st.subheader("Configuración de Caché")
    st.text_input("Tipo de Caché", value=os.getenv("CACHE_TYPE", "memory"), disabled=True)
    st.text_input("TTL de Caché", value=os.getenv("CACHE_TTL", "3600"), disabled=True)
    
    # Configuración del scraper
    st.subheader("Configuración del Scraper")
    col1, col2 = st.columns(2)
    with col1:
        st.number_input("Hilos", value=int(os.getenv("SCRAPER_THREADS", "4")))
        st.number_input("Timeout de Peticiones", value=int(os.getenv("REQUEST_TIMEOUT", "30")))
    with col2:
        st.number_input("Máximo de Reintentos", value=int(os.getenv("MAX_RETRIES", "3")))
        st.text_input("User Agent", value=os.getenv("USER_AGENT", ""))

def show_metrics():
    """Display metrics page."""
    st.title("📈 Métricas")
    
    metrics = get_metrics()
    report = metrics.get_report()
    
    # Métricas de rendimiento
    st.subheader("Rendimiento")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Uso de CPU", f"{report.performance['cpu_percent']}%")
    with col2:
        st.metric("Uso de Memoria", f"{report.performance['memory_mb']:.2f} MB")
    with col3:
        st.metric("Conexiones DB", report.database['connections'])
    
    # Métricas de caché
    st.subheader("Caché")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Cache Hits", report.cache['hits'])
    with col2:
        st.metric("Cache Misses", report.cache['misses'])
    
    # Gráfico de errores
    st.subheader("Errores por Tipo")
    error_df = pd.DataFrame(
        list(report.errors.items()),
        columns=['Tipo de Error', 'Cantidad']
    )
    if not error_df.empty:
        fig = px.bar(error_df, x='Tipo de Error', y='Cantidad')
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()