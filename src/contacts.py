# src/contacts.py (MODIFICADO para aceptar fechas como argumentos y generar rangos)

import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv 
from pathlib import Path 
import pprint

# URL base para buscar contactos en la API v3 de HubSpot
API_ENDPOINT = "https://api.hubspot.com/crm/v3/objects/contacts/search"

# --- FUNCIÓN DE AYUDA PARA FECHAS (NUEVA: RANGOS DE 2025) ---
def get_month_ranges_2025():
    """
    Calcula los rangos de fechas (timestamp en ms) para cada mes de 2025, 
    desde enero hasta el inicio del mes actual.
    """
    today = datetime.now()
    month_ranges = {}
    
    current_date = datetime(2025, 1, 1, 0, 0, 0)
    # El final del rango es el inicio del mes actual (para excluir datos incompletos)
    end_of_range = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    while current_date < end_of_range:
        start_date = current_date
        end_date = start_date + relativedelta(months=1)
        
        # Etiqueta para el reporte (Ene-25, Feb-25, etc.)
        month_label = start_date.strftime("%b-%y").capitalize().replace('Dec', 'Dic').replace('Aug', 'Ago').replace('Apr', 'Abr').replace('May', 'May').replace('Jun', 'Jun').replace('Jul', 'Jul').replace('Sep', 'Sep').replace('Oct', 'Oct').replace('Nov', 'Nov')

        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        
        month_ranges[month_label] = (start_timestamp, end_timestamp)
        
        current_date = end_date
        
    return month_ranges

# NOTA: get_last_month_dates() se elimina/ignora ya que no es necesaria en este nuevo flujo.

# --- EL "MOTOR" DE BÚSQUEDA (MODIFICADO para aceptar y usar fechas) ---
def _search_contacts(access_token, start_date_ms, end_date_ms, additional_filters=[]):
    """
    Función interna que busca contactos. Acepta y usa las fechas dadas.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # 1. Filtros Base: Rango de fechas provisto por los argumentos
    base_filters = [
        {"propertyName": "createdate", "operator": "GTE", "value": start_date_ms},
        {"propertyName": "createdate", "operator": "LT", "value": end_date_ms}
    ]
    
    # 2. Combinamos los filtros
    all_filters = base_filters + additional_filters
    
    payload = {
        "filterGroups": [{"filters": all_filters}],
        "limit": 1
    }

    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        return data.get("total", 0)

    except requests.exceptions.HTTPError as err:
        print(f"\n--- ERROR DE API ---")
        if err.response.status_code == 400:
            print(f"❌ ERROR 400: SOLICITUD INVÁLIDA (Revisar nombres de propiedades/filtros).")
            import json
            print(json.dumps(all_filters, indent=2)) 
        print(f"Error: {err}")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return None

# --- FUNCIONES PÚBLICAS (MODIFICADAS para aceptar fechas) ---

def get_total_new_leads(access_token, start_date_ms, end_date_ms):
    """Devuelve el total de leads (filtrado SÓLO por fecha)."""
    return _search_contacts(access_token, start_date_ms, end_date_ms, additional_filters=[])

def get_leads_by_country(access_token, start_date_ms, end_date_ms, countries):
    """Devuelve un desglose de contactos por país (MODIFICADO para aceptar fechas)."""
    
    results = {}
    property_name = "investment_destination_country__multiple_checkboxes_" 
    
    for country in countries:
        
        country_filter = None
        
        if country in ["Spain", "Ireland", "Indonesia", "Australia"]:
            country_filter = {
                "propertyName": property_name,
                "operator": "CONTAINS_TOKEN", 
                "value": country
            }
        elif country == "Wealth":
            country_filter = {
                "propertyName": property_name,
                "operator": "CONTAINS_TOKEN", 
                "value": "WEALTH"
            }
        elif country == "Unknown":
            # Filtro para valor vacío en multi-select (Probable causa de 400, usar con cautela)
            country_filter = {
                "propertyName": property_name,
                "operator": "NOT_HAS_PROPERTY", 
            }
        else:
             continue
        
        if country_filter:
            count = _search_contacts(access_token, start_date_ms, end_date_ms, additional_filters=[country_filter])
            results[country] = count
            
    return results


def get_leads_by_traffic_source(access_token, start_date_ms, end_date_ms, sources_map):
    """Devuelve el desglose para fuentes individuales o agregadas (MODIFICADO para aceptar fechas)."""
    
    property_name = "original_traffic_source_2_0" 
    results = {}
    
    for label, internal_value in sources_map.items():
        if internal_value == "MANUAL_SKIP":
             results[label] = "MANUAL_SKIP"
             continue
        
        source_filter = {
            "propertyName": property_name,
            "operator": "EQ",
            "value": internal_value
        }
        
        count = _search_contacts(access_token, start_date_ms, end_date_ms, additional_filters=[source_filter])
        results[label] = count
        
    return results

def get_leads_ambassadors(access_token, start_date_ms, end_date_ms, internal_name_code):
    """Obtiene el conteo de Ambassadors (MODIFICADO para aceptar fechas)."""

    filter_code = {
        "propertyName": internal_name_code, 
        "operator": "CONTAINS", 
        "value": "BLV10"
    }
    
    count = _search_contacts(access_token, start_date_ms, end_date_ms, [filter_code])
    return count

# --- BLOQUE DE PRUEBA (MODIFICADO para iterar sobre TODOS los meses) ---
if __name__ == "__main__":
    print("--- Prueba de contacts.py: Adquisición Mensual Completa ---")
    
    # 1. Cargar .env para obtener el token
    root_dir = Path(__file__).parent.parent
    load_dotenv(dotenv_path=root_dir / ".env")
    HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
    AMBASSADOR_PROPERTY = "promotional_code" 
    COUNTRIES = ["Spain", "Ireland", "Indonesia", "Australia", "Wealth", "Unknown"]
    TRAFFIC_SOURCES_MAP = {
        "Search": "ORGANIC_SEARCH", "Social": "SOCIAL_MEDIA", "Referral": "REFERRALS", 
        "Email": "EMAIL_MARKETING", "Paid Search": "PAID_SEARCH", "Direct": "DIRECT_TRAFFIC"
    }

    if not HUBSPOT_ACCESS_TOKEN:
        print("❌ ERROR: No se encontró HUBSPOT_ACCESS_TOKEN. Configure el archivo .env.")
    else:
        # 2. Obtener rangos de fecha para todos los meses completos de 2025
        ranges = get_month_ranges_2025()
        
        if not ranges:
             print("⚠️ No hay rangos de meses para 2025 generados todavía (verifique la fecha actual del sistema).")
             exit()

        print(f"✅ Rangos generados para {len(ranges)} meses: {list(ranges.keys())}")
        print("\n--- Ejecutando todas las consultas para CADA MES ---")
        
        # 3. Iterar sobre CADA MES y adquirir todos los datos solicitados
        for month_label, (start_ms, end_ms) in ranges.items():
            print(f"\nProcessing **{month_label}**...")

            start_dt = datetime.fromtimestamp(start_ms / 1000).strftime('%Y-%m-%d')
            end_dt = datetime.fromtimestamp(end_ms / 1000).strftime('%Y-%m-%d')
            
            # 3.1. Leads Totales
            total_leads = get_total_new_leads(HUBSPOT_ACCESS_TOKEN, start_ms, end_ms)
            print(f"  - 📈 Leads Totales: {total_leads}")

            # 3.2. Leads por Fuente de Tráfico
            leads_by_source = get_leads_by_traffic_source(HUBSPOT_ACCESS_TOKEN, start_ms, end_ms, TRAFFIC_SOURCES_MAP)
            print(f"  - 🚦 Fuentes (Traffic Source): {leads_by_source}")

            # 3.3. Leads por País
            leads_by_country = get_leads_by_country(HUBSPOT_ACCESS_TOKEN, start_ms, end_ms, COUNTRIES)
            print(f"  - 🌍 Países: {leads_by_country}")

            # 3.4. Ambassadors
            ambassadors = get_leads_ambassadors(HUBSPOT_ACCESS_TOKEN, start_ms, end_ms, AMBASSADOR_PROPERTY)
            print(f"  - 👑 Ambassadors (BLV10): {ambassadors}")

        print("\n--- ✅ PRUEBA DE CONTACTS COMPLETA ---")