# src/contacts.py

import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

# URL base para buscar contactos en la API v3 de HubSpot
API_ENDPOINT = "https://api.hubapi.com/crm/v3/objects/contacts/search"

# --- FUNCIÓN DE AYUDA PARA FECHAS ---
def get_last_month_dates():
    """Calcula las fechas de inicio y fin del mes pasado."""
    today = datetime.now()
    first_day_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day_last_month = first_day_current_month - relativedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    start_timestamp = int(first_day_last_month.timestamp() * 1000)
    end_timestamp = int(first_day_current_month.timestamp() * 1000)
    return start_timestamp, end_timestamp

# --- EL "MOTOR" DE BÚSQUEDA (PERFECTO, NO TOCAR) ---
def _search_contacts(access_token, additional_filters=[]):
    """
    Función interna que busca contactos.
    El filtro base es SÓLO la fecha de creación.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    start_date_ms, end_date_ms = get_last_month_dates()
    
    # 1. Filtros Base: SÓLO mes pasado.
    base_filters = [
        {"propertyName": "createdate", "operator": "GTE", "value": start_date_ms},
        {"propertyName": "createdate", "operator": "LT", "value": end_date_ms}
    ]
    
    # 2. Combinamos los filtros
    all_filters = base_filters + additional_filters
    
    payload = {
        "filterGroups": [{"filters": all_filters}],
        "limit": 1 # Solo queremos el conteo total
    }

    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status() 
        data = response.json()
        return data.get("total", 0)

    except requests.exceptions.HTTPError as err:
        print(f"\n--- ERROR DE API ---")
        print(f"Error: {err}")
        try:
            error_data = err.response.json()
            print(f"Detalles de HubSpot: {error_data.get('message', 'No message')}")
        except:
            print(f"Detalles (no JSON): {err.response.text}")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        return None

# --- FUNCIONES PÚBLICAS ---

def get_total_new_leads(access_token):
    """
    (PERFECTO, NO TOCAR)
    Petición 1: Devuelve el total de leads (filtrado SÓLO por fecha).
    """
    print("Obteniendo total de nuevos leads...")
    return _search_contacts(access_token, additional_filters=[])

def get_leads_by_country(access_token, countries):
    """
    Petición 2: Devuelve un desglose de contactos por país.
    """
    print("Obteniendo leads por país (usando 'CONTAINS_TOKEN')...")
    
    # ¡CORRECTO! Usamos el "Internal Name" que encontraste
    property_name = "investment_destination_country__multiple_checkboxes_" 
    
    results = {}
    for country in countries:
        print(f"  - Buscando país que 'contiene': {country}")
        
        # ¡CORRECTO! Usamos 'CONTAINS_TOKEN' para "multiple checkboxes"
        country_filter = {
            "propertyName": property_name,
            "operator": "CONTAINS_TOKEN", 
            "value": country
        }
        
        count = _search_contacts(access_token, additional_filters=[country_filter])
        results[country] = count
        
    return results

def get_leads_by_traffic_source(access_token, sources_map):
    """
    (PERFECTO, NO TOCAR)
    Petición 3: Devuelve un desglose por fuente de tráfico.
    """
    print("\nObteniendo leads por fuente de tráfico...")
    
    property_name = "hs_analytics_source" 
    
    results = {}
    for label, internal_value in sources_map.items():
        print(f"  - Buscando para: {label} (Valor API: {internal_value})")
        
        source_filter = {
            "propertyName": property_name,
            "operator": "EQ",
            "value": internal_value
        }
        
        count = _search_contacts(access_token, additional_filters=[source_filter])
        results[label] = count
        
    return results