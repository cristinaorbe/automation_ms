# src/main.py (El orquestador principal - CORREGIDO)

import os
import pprint
import csv
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from contacts import (
    get_total_new_leads, 
    get_leads_by_country, 
    get_leads_by_traffic_source,
    get_last_month_dates # <--- Importamos si es necesario, pero ya está en deals.py
)
from deals import ( 
    get_engagements_per_pipeline,
    get_engagements_breakdown_by_property
)

# --- FUNCIÓN FINAL DE EXPORTACIÓN ---
# ... (write_final_report se mantiene igual) ...
def write_final_report(data_to_write):
    """
    Crea el reporte CSV con toda la información consolidada en el orden exacto.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"reporte_mensual_{today_str}.csv"
    
    print(f"\nEscribiendo reporte final en: {filename}")
    
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Encabezados iniciales (solo una vez)
            writer.writerow(["MÉTRICA", "VALOR"])
            
            # Escribimos los datos de la lista ordenada
            for label, value in data_to_write:
                writer.writerow([label, value])
            
        print(f"¡Éxito! Reporte guardado en {filename}")
        print(f"Ruta completa: {Path.cwd() / filename}")

    except Exception as e:
        print(f"Error al escribir el archivo CSV: {e}")

# --- FUNCIÓN PRINCIPAL DE EJECUCIÓN ---
def main():
    print("Iniciando el script de automatización...")
    
    # --- A. CONFIGURACIÓN GENERAL ---
    root_dir = Path(__file__).parent.parent
    env_path = root_dir / ".env"
    load_dotenv(dotenv_path=env_path)
    HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
    
    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("No se encontró HUBSPOT_ACCESS_TOKEN.")
    
    print("Token de HubSpot cargado con éxito.")
    
    # --- B. MAPAS DE DATOS (COMPLETOS) ---
    # 1. Leads por País (Incluimos "Wealth" en la lista)
    COUNTRIES_TO_CHECK = ["Spain", "Ireland", "Indonesia", "Australia", "Unknown", "Wealth"]
    
    # 2. Leads por Fuente (Usamos SÓLO los valores ATÓMICOS de la API, y marcamos los manuales)
    LEAD_SOURCES_MAP = {
        "Meta - Paid Social": "PAID_SOCIAL", 
        "Google - Paid Search": "PAID_SEARCH", 
        "Organic Search": "ORGANIC_SEARCH", 
        "Organic Social": "ORGANIC_SOCIAL", 
        "Direct Traffic": "DIRECT_TRAFFIC", 
        "Email marketing": "EMAIL_MARKETING",
        "Referrals": "REFERRALS", 
        "AI Referrals": "AI_REFERRALS", 
        "Family & friends": "FAMILY_AND_FRIENDS", 
        "PR/Events/Organic (raw)": "PR_EVENTS_ORGANIC", # Usamos el valor atómico
        "Partnerships": "PARTNERSHIPS", 
        "App": "APP", 
        "Ambassadors": "AMBASSADORS", 
        "Outbound": "OUTBOUND_SALES", # Valor atómico
        "B2C referrals": "B2C_REFERRALS",
        # Estos se marcarán para saltar la consulta y se manejan en el reporte.
        "C2C referrals (manual)": "MANUAL_SKIP", 
        "Marketing Influencers (manual)": "MANUAL_SKIP", 
    }
    
    # ... (Mapeo de Deals se mantiene igual) ...
    # 3. Engagements (Deals) Configuración
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
        "Meta": "PAID_SOCIAL", 
        "Google": "PAID_SEARCH",
    }
    
    # --- C. EJECUCIÓN DE LLAMADAS A LA API ---
    
    # CONTACTS
    total_leads = get_total_new_leads(HUBSPOT_ACCESS_TOKEN)
    country_leads = get_leads_by_country(HUBSPOT_ACCESS_TOKEN, COUNTRIES_TO_CHECK)
    lead_sources_raw = get_leads_by_traffic_source(HUBSPOT_ACCESS_TOKEN, LEAD_SOURCES_MAP)
    
    # DEALS
    pipeline_totals = get_engagements_per_pipeline(HUBSPOT_ACCESS_TOKEN, PIPELINE_MAP)
    total_engagements = sum(v for v in pipeline_totals.values() if v is not None)
    deal_source_raw = get_engagements_breakdown_by_property(HUBSPOT_ACCESS_TOKEN, PIPELINE_ID_LIST, DEAL_SOURCE_PROP_NAME, DEAL_SOURCE_MAP_RAW)
    deal_type_data = get_engagements_breakdown_by_property(HUBSPOT_ACCESS_TOKEN, PIPELINE_ID_LIST, DEAL_TYPE_PROP_NAME, DEAL_TYPE_MAP)
    traffic_source_deals = get_engagements_breakdown_by_property(HUBSPOT_ACCESS_TOKEN, PIPELINE_ID_LIST, TRAFFIC_SOURCE_PROP_NAME, TRAFFIC_SOURCE_MAP)

    # --- D. PROCESAMIENTO Y AGRUPACIÓN DE RESULTADOS ---
    print("\nProcesando y agrupando resultados finales...")

    def get_count(data, key):
        # Si el valor es "MANUAL_SKIP" o None, devuelve 0
        value = data.get(key, 0)
        return 0 if value == "MANUAL_SKIP" or value is None else value

    # 1. AGRUPACIÓN DE FUENTES DE LEADS (CONTACTOS)
    
    # Corregido: Sumamos Meta y Google que son valores atómicos consultados
    paid_online_mkt_leads = get_count(lead_sources_raw, "Meta - Paid Social") + get_count(lead_sources_raw, "Google - Paid Search")
    
    # PR / Events / Organic: usamos el valor atómico consultado
    pr_events_organic_leads = get_count(lead_sources_raw, "PR/Events/Organic (raw)")
    
    # C2C: es manual, no se calcula
    c2c_referrals_leads = "Manual" 
    
    # Marketing Influencers: es manual, no se calcula
    marketing_influencers_leads = "Manual" 

    # 2. AGRUPACIÓN DE FUENTES DE ENGAGEMENTS (DEALS)
    meta_deals = get_count(traffic_source_deals, "Meta")
    google_deals = get_count(traffic_source_deals, "Google")
    paid_online_mkt_deals = meta_deals + google_deals 
    
    pr_events_organic_deals = get_count(deal_source_raw, "Organic") + get_count(deal_source_raw, "Events")
    
    # --- E. CONSTRUCCIÓN DEL REPORTE ORDENADO ---
    final_report_data = [
        # --- 1. SECCIÓN LEADS ---
        ("--- 2LEADS - SPLIT PER CHANNEL ---", ""),
        ("# of new leads", total_leads),
        ("Target", "Manual"), # Se deja Manual según tu solicitud
        ("Paid online Marketing", paid_online_mkt_leads),
        ("Meta - Paid Social", get_count(lead_sources_raw, "Meta - Paid Social")),
        ("Google - Paid Search", get_count(lead_sources_raw, "Google - Paid Search")),
        ("C2C referrals", c2c_referrals_leads), # Valor Manual
        ("Family & friends", get_count(lead_sources_raw, "Family & friends")),
        ("PR /Events / Organic", pr_events_organic_leads),
        ("Marketing Influencers", marketing_influencers_leads), # Valor Manual
        ("Partnerships", get_count(lead_sources_raw, "Partnerships")),
        ("App", get_count(lead_sources_raw, "App")),
        ("Ambassadors", get_count(lead_sources_raw, "Ambassadors")),
        ("Outbound", get_count(lead_sources_raw, "Outbound")),
        ("B2C referrals", get_count(lead_sources_raw, "B2C referrals")),
        ("Indonesia", get_count(country_leads, "Indonesia")),
        ("Ireland", get_count(country_leads, "Ireland")),
        ("Wealth", get_count(country_leads, "Wealth")), 
        
        # --- 2. SECCIÓN ENGAGEMENTS ---
        ("", ""), 
        ("--- 5ENGAGEMENTS - SPLIT PER CHANNEL ---", ""),
        ("# of new Engagements", total_engagements),
        ("Deal Source (Closers + PC + Wealth)", total_engagements), 
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
        ("B2B Referrals", get_count(deal_source_raw, "Partnership (raw)")), 
        
        # --- 3. SECCIÓN DEAL TYPE ---
        ("", ""), 
        ("--- 6Deal Type ---", ""),
        ("New", get_count(deal_type_data, "New")),
        ("New Multi", get_count(deal_type_data, "New - Multi")),
        ("Repeat", get_count(deal_type_data, "Repeat")),
        ("Repeat Multi", get_count(deal_type_data, "Repeat - Multi")),
    ]

    # --- F. EXPORTACIÓN FINAL ---
    write_final_report(final_report_data)

if __name__ == "__main__":
    main()