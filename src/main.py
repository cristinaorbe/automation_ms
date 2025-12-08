# src/main.py (El orquestador principal - VERSIÓN MENSUALIZADA)

import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from contacts import (
    get_total_new_leads, 
    get_leads_by_country, 
    get_leads_by_traffic_source,
    get_leads_ambassadors, # Aseguramos que esta se use
    get_month_ranges_2025 # <-- IMPORTACIÓN CLAVE PARA LA ITERACIÓN
)
from deals import ( 
    get_engagements_per_pipeline,
    get_engagements_breakdown_by_property
)

# --- FUNCIÓN DE AYUDA PARA OBTENER EL CONTEO/VALOR ---
def get_count(data, key):
    """
    Función de ayuda para obtener el conteo/valor. 
    Asegura que se devuelva 0 si el valor es None o MANUAL_SKIP.
    """
    value = data.get(key, 0)
    
    if value is None or value == "MANUAL_SKIP":
        return 0
    
    try:
        # Intentamos convertir a int o float
        return float(value) if isinstance(value, (float, str)) and '.' in str(value) else int(value)
    except (ValueError, TypeError):
        # Devuelve 0 si no es un número válido (evitando problemas de CSV)
        return 0


# --- FUNCIÓN FINAL DE EXPORTACIÓN (MODIFICADA para formato mensualizado) ---
def write_final_report(monthly_data):
    """
    Crea el reporte CSV con toda la información consolidada en formato mensualizado.
    monthly_data: {'Ene-25': [(label, value), ...], 'Feb-25': [...]}
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"reporte_mensualizado_{today_str}.csv"
    
    print(f"\nEscribiendo reporte final en: {filename}")
    
    if not monthly_data:
        print("No hay datos para generar el reporte.")
        return

    # Extraer los encabezados de mes y las etiquetas de las métricas
    month_labels = list(monthly_data.keys())
    # Usamos el primer mes para obtener el orden de las métricas
    metric_labels_and_values = monthly_data[month_labels[0]]
    
    # 1. Construir las filas de encabezado (Fila 1, Fila 3, Meses)
    report_rows = []
    
    # Fila 1: Título principal
    report_rows.append(["MARKETING & SALES DASHBOARD (US$, Global)"] + [""] * len(month_labels))
    # Fila 2: Vacía para espaciar
    report_rows.append([""] * (len(month_labels) + 1))
    # Fila 3: Moneda y Encabezados de mes
    report_rows.append(["In US$, unless otherwise stated"] + month_labels)
    
    # 2. Construir las filas de Métricas
    for metric_label, _ in metric_labels_and_values:
        row = [metric_label]
        is_title_row = metric_label.startswith("---")
        
        # Iterar sobre cada mes para obtener el valor correspondiente
        for month_label in month_labels:
            # Obtener el valor de la métrica específica para este mes
            month_metrics = dict(monthly_data[month_label])
            value = month_metrics.get(metric_label, '')
            
            # Formateo si es una suma de valor para Deals
            if metric_label == "Value Partner (SUMA DE VALOR)" and isinstance(value, (int, float)):
                row.append(f"{value:.2f}")
            elif is_title_row:
                 row.append("")
            else:
                row.append(value)
                
        report_rows.append(row)

    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(report_rows)
            
        print(f"¡Éxito! Reporte guardado en {filename}")

    except Exception as e:
        print(f"Error al escribir el archivo CSV: {e}")

# --- FUNCIÓN PRINCIPAL DE EJECUCIÓN (MODIFICADA para la iteración mensual) ---
def main():
    print("Iniciando el script de automatización...")
    
    # A. CONFIGURACIÓN GENERAL 
    root_dir = Path(__file__).parent.parent
    env_path = root_dir / ".env"
    load_dotenv(dotenv_path=env_path)
    HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
    
    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("No se encontró HUBSPOT_ACCESS_TOKEN.")
    
    print("Token de HubSpot cargado con éxito.")
    
    # B. MAPAS DE DATOS (COMPLETOS)
    COUNTRIES_TO_CHECK = ["Spain", "Ireland", "Indonesia", "Australia", "Unknown", "Wealth"]
    LEAD_SOURCES_MAP = {
        "Meta - Paid Social": "PAID_SOCIAL", "Google - Paid Search": "PAID_SEARCH", "Organic Search": "ORGANIC_SEARCH", 
        "Organic Social": "ORGANIC_SOCIAL", "Direct Traffic": "DIRECT_TRAFFIC", "Email marketing": "EMAIL_MARKETING",
        "Referrals": "REFERRALS", "AI Referrals": "AI_REFERRALS", "Family & friends": "FAMILY_AND_FRIENDS", 
        "PR/Events/Organic (raw)": "PR_EVENTS_ORGANIC", "Partnerships": "PARTNERSHIPS", "App": "APP", 
        "Ambassadors": "AMBASSADORS", "Outbound": "OUTBOUND_SALES", "B2C referrals": "B2C_REFERRALS",
        "C2C referrals (manual)": "MANUAL_SKIP", "Marketing Influencers (manual)": "MANUAL_SKIP", 
    }
    
    PIPELINE_MAP = {"[SP] Sales": "default", "[SP] Value Partners & Wealth": "188587965"}
    PIPELINE_ID_LIST = list(PIPELINE_MAP.values()) 
    
    DEAL_TYPE_PROP_NAME = "dealtype"
    DEAL_TYPE_MAP = {
        "New": "newbusiness", "New - Multi": "New - Multi", "Repeat": "existingbusiness", "Repeat - Multi": "Repeat - Multi"
    }

    DEAL_SOURCE_PROP_NAME = "deal_source"
    DEAL_SOURCE_MAP_RAW = {
        "Family & Friends (raw)": "Direct Traffic", "Partnership (raw)": "B2C Referrals", "Ambassador": "Ambassador",
        "Paid": "Paid", "Organic": "Organic", "Outbound Sales": "Outbound Sales", "App": "App", 
        "Events": "Events", "Influencers and MKT Ambassadors": "Influencers and MKT Ambassadors",
        "C2C Referrals (raw)": "C2C Referrals" 
    }
    
    TRAFFIC_SOURCE_PROP_NAME = "hs_analytics_source" 
    TRAFFIC_SOURCE_MAP = {
        "Meta": "PAID_SOCIAL", "Google": "PAID_SEARCH",
    }

    # C. ITERACIÓN POR MES Y EJECUCIÓN DE LLAMADAS A LA API 
    monthly_report_data = {}
    month_ranges = get_month_ranges_2025() 
    
    if not month_ranges:
        print("No hay meses completos de 2025 para reportar. Terminando ejecución.")
        return

    print(f"\nEjecutando llamadas a la API para {len(month_ranges)} meses...")

    for month_label, (start_date_ms, end_date_ms) in month_ranges.items():
        print(f"-> Procesando **{month_label}** (Fechas: {datetime.fromtimestamp(start_date_ms/1000).strftime('%Y-%m-%d')} a {datetime.fromtimestamp(end_date_ms/1000).strftime('%Y-%m-%d')})")
        
        # 1. CONTACTS (Todas las funciones usan start_date_ms y end_date_ms)
        total_leads = get_total_new_leads(HUBSPOT_ACCESS_TOKEN, start_date_ms, end_date_ms)
        country_leads = get_leads_by_country(HUBSPOT_ACCESS_TOKEN, start_date_ms, end_date_ms, COUNTRIES_TO_CHECK)
        lead_sources_raw = get_leads_by_traffic_source(HUBSPOT_ACCESS_TOKEN, start_date_ms, end_date_ms, LEAD_SOURCES_MAP)
        ambassador_leads = get_leads_ambassadors(HUBSPOT_ACCESS_TOKEN, start_date_ms, end_date_ms, "promotional_code")

        # 2. DEALS (Todas las funciones usan start_date_ms y end_date_ms)
        pipeline_totals = get_engagements_per_pipeline(HUBSPOT_ACCESS_TOKEN, start_date_ms, end_date_ms, PIPELINE_MAP)
        
        # Obtenemos los valores individuales de la lista
        sales_count = get_count(pipeline_totals, "[SP] Sales")
        value_partner_sum = get_count(pipeline_totals, "[SP] Value Partners & Wealth")
        
        # Cálculos de Engagements
        total_engagements_for_report = sales_count + value_partner_sum # CONTEO + SUMA DE VALOR VP
        total_engagements_count = sales_count 
        
        deal_source_raw = get_engagements_breakdown_by_property(HUBSPOT_ACCESS_TOKEN, PIPELINE_ID_LIST, start_date_ms, end_date_ms, DEAL_SOURCE_PROP_NAME, DEAL_SOURCE_MAP_RAW)
        deal_type_data = get_engagements_breakdown_by_property(HUBSPOT_ACCESS_TOKEN, PIPELINE_ID_LIST, start_date_ms, end_date_ms, DEAL_TYPE_PROP_NAME, DEAL_TYPE_MAP)
        traffic_source_deals = get_engagements_breakdown_by_property(HUBSPOT_ACCESS_TOKEN, PIPELINE_ID_LIST, start_date_ms, end_date_ms, TRAFFIC_SOURCE_PROP_NAME, TRAFFIC_SOURCE_MAP)

        # D. PROCESAMIENTO Y AGRUPACIÓN DE RESULTADOS
        paid_online_mkt_leads = get_count(lead_sources_raw, "Meta - Paid Social") + get_count(lead_sources_raw, "Google - Paid Search")
        pr_events_organic_leads = get_count(lead_sources_raw, "PR/Events/Organic (raw)")
        c2c_referrals_leads = "Manual" 
        marketing_influencers_leads = "Manual" 
        
        meta_deals = get_count(traffic_source_deals, "Meta")
        google_deals = get_count(traffic_source_deals, "Google")
        paid_online_mkt_deals = meta_deals + google_deals 
        pr_events_organic_deals = get_count(deal_source_raw, "Organic") + get_count(deal_source_raw, "Events")
        
        # E. CONSTRUCCIÓN DEL REPORTE ORDENADO PARA EL MES ACTUAL
        final_report_data_month = [
            # --- 1. SECCIÓN LEADS ---
            ("--- 2LEADS - SPLIT PER CHANNEL ---", ""),
            ("# of new leads", total_leads),
            ("Target", "Manual"), 
            ("Paid online Marketing", paid_online_mkt_leads),
            ("Meta - Paid Social", get_count(lead_sources_raw, "Meta - Paid Social")),
            ("Google - Paid Search", get_count(lead_sources_raw, "Google - Paid Search")),
            ("C2C referrals", c2c_referrals_leads), 
            ("Family & friends", get_count(lead_sources_raw, "Family & friends")),
            ("PR /Events / Organic", pr_events_organic_leads),
            ("Marketing Influencers", marketing_influencers_leads), 
            ("Partnerships", get_count(lead_sources_raw, "Partnerships")),
            ("App", get_count(lead_sources_raw, "App")),
            ("Ambassadors", ambassador_leads),
            ("Outbound", get_count(lead_sources_raw, "Outbound")),
            ("B2C referrals", get_count(lead_sources_raw, "B2C referrals")),
            ("Indonesia", get_count(country_leads, "Indonesia")),
            ("Ireland", get_count(country_leads, "Ireland")),
            ("Wealth", get_count(country_leads, "Wealth")), 
            
            # --- 2. SECCIÓN ENGAGEMENTS ---
            ("", ""), 
            ("--- 5ENGAGEMENTS - SPLIT PER CHANNEL ---", ""),
            ("# of new Engagements (Sales CONTEO)", total_engagements_for_report), 
            ("Deal Source (Closers + PC)", total_engagements_count), 
            ("Value Partner (SUMA DE VALOR)", value_partner_sum), 
            ("Paid Online Marketing", paid_online_mkt_deals), 
            ("Meta", meta_deals), 
            ("Google", google_deals), 
            ("C2C", get_count(deal_source_raw, "C2C Referrals (raw)")), 
            ("Family and Friends", get_count(deal_source_raw, "Family & Friends (raw)")),
            ("PR / Events / Organic", pr_events_organic_deals), 
            ("Marketing Influencers", get_count(deal_source_raw, "Influencers and MKT Ambassadors")), 
            ("Partnerships", get_count(deal_source_raw, "Partnership (raw)")),
            ("App", get_count(deal_source_raw, "App")),
            ("Ambassador", get_count(deal_source_raw, "Ambassador")),
            ("Outbound Sales", get_count(deal_source_raw, "Outbound Sales")),
            
            # --- 3. SECCIÓN DEAL TYPE ---
            ("", ""), 
            ("--- 6Deal Type ---", ""),
            ("New", get_count(deal_type_data, "New")),
            ("New Multi", get_count(deal_type_data, "New - Multi")),
            ("Repeat", get_count(deal_type_data, "Repeat")),
            ("Repeat Multi", get_count(deal_type_data, "Repeat - Multi")),
        ]

        # F. Almacenar los datos del mes en el diccionario principal
        monthly_report_data[month_label] = final_report_data_month

    # G. EXPORTACIÓN FINAL 
    write_final_report(monthly_report_data)

if __name__ == "__main__":
    main()