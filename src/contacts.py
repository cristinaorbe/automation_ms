# src/contacts.py (CORREGIDO)

import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv 
from pathlib import Path 
import pprint

# URL base para buscar contactos en la API v3 de HubSpot
API_ENDPOINT = "https://api.hubspot.com/crm/v3/objects/contacts/search"

# --- FUNCIÓN DE AYUDA PARA FECHAS ---
def get_last_month_dates():
    """Calcula las fechas de inicio y fin del mes anterior. Rango de fechas."""
    today = datetime.now()
    first_day_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day_last_month = first_day_current_month - relativedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    start_timestamp = int(first_day_last_month.timestamp() * 1000)
    end_timestamp = int(first_day_current_month.timestamp() * 1000)
    return start_timestamp, end_timestamp

# --- EL "MOTOR" DE BÚSQUEDA ---
def _search_contacts(access_token, additional_filters=[]):
    """
    Función interna que busca contactos.
    El filtro base es SÓLO la fecha de creación.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",          #a quien le hacemos la solicitud
        "Content-Type": "application/json"                  #formato JSON
    }
    start_date_ms, end_date_ms = get_last_month_dates()     #FECHAS para cuando queremos sacar los datos
    
    # 1. Filtros Base: SÓLO mes pasado.
    base_filters = [
        {"propertyName": "createdate", "operator": "GTE", "value": start_date_ms},
        {"propertyName": "createdate", "operator": "LT", "value": end_date_ms}
    ]
    
    # 2. Combinamos los filtros
    all_filters = base_filters + additional_filters
    
    payload = {
        "filterGroups": [{"filters": all_filters}],
        "limit": 1                                           # Solo queremos el conteo total
    }

    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status()                          # Lanza un error 
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

# --- FUNCIONES PÚBLICAS (Base) ---

def get_total_new_leads(access_token):
    """Devuelve el total de leads (filtrado SÓLO por fecha)."""
    print("Obteniendo total de nuevos leads...")
    return _search_contacts(access_token, additional_filters=[])

def get_leads_by_country(access_token, countries):
    """Devuelve un desglose de contactos por país (multi-checkbox) y el valor 'Unknown'."""
    print("Obteniendo leads por país...")
    
    results = {}
    
    for country in countries:
        print(f"   - Buscando país: {country}")

        if country in ["Spain", "Ireland", "Indonesia", "Australia"]:
            # Usamos la propiedad multi-checkbox
            property_name = "investment_destination_country__multiple_checkboxes_" 
            country_filter = {
                "propertyName": property_name,
                "operator": "CONTAINS_TOKEN", 
                "value": country
            }
        elif country == "Wealth":
            # Asumimos que "Wealth" se correlaciona con el valor de país "WEALTH" o "Unknown" 
            # en la propiedad 'investment_destination_country__multiple_checkboxes_'
            property_name = "investment_destination_country__multiple_checkboxes_" 
            country_filter = {
                "propertyName": property_name,
                "operator": "CONTAINS_TOKEN", 
                "value": "WEALTH"
            }
        elif country == "Unknown":
            # Esto es un valor de la propiedad 'investment_destination_country__multiple_checkboxes_'
            property_name = "investment_destination_country__multiple_checkboxes_" 
            country_filter = {
                "propertyName": property_name,
                "operator": "CONTAINS_TOKEN", 
                "value": "Unknown"
            }
        else:
             # Si no está mapeado, se asume 0 o se usa la lógica Unknown/default.
             # Para simplificar, si no es conocido, lo ignoramos o lo marcamos como Unknown.
             continue
        
        count = _search_contacts(access_token, additional_filters=[country_filter])
        results[country] = count
            
    return results


def get_leads_by_traffic_source(access_token, sources_map):
    """Devuelve el desglose para fuentes individuales o agregadas.
       Solo busca valores ATÓMICOS de la API.
    """
    print("\nObteniendo leads por fuente de tráfico (Individual/Atómico)...")
    
    property_name = "original_traffic_source_2_0" 
    
    results = {}
    for label, internal_value in sources_map.items():
        # Ignoramos si la etiqueta es 'manual'
        if internal_value == "MANUAL_SKIP":
             results[label] = "MANUAL_SKIP"
             continue

        print(f"   - Buscando para: {label} (Valor API: {internal_value})")
        
        source_filter = {
            "propertyName": property_name,
            "operator": "EQ",
            "value": internal_value
        }
        
        count = _search_contacts(access_token, additional_filters=[source_filter])
        results[label] = count
        
    return results

# --- FUNCIÓN AMBASSADORS (CORREGIDA) ---
def get_leads_ambassadors(access_token, internal_name_code):
    """
    Obtiene el conteo de Ambassadors. Usa el filtro Promotional Code.
    """
    print("\nObteniendo leads de Ambassadors (Promotional Code)...")

    filter_code = {
        "propertyName": internal_name_code, 
        "operator": "CONTAINS", 
        "value": "BLV10"
    }
    
    count = _search_contacts(access_token, [filter_code])
    return count

# --- BLOQUE DE PRUEBA (SOLO PARA DEPURACIÓN - Se mantiene por completitud) ---
if __name__ == "__main__":
    # ... (El bloque de prueba sigue igual, solo se actualiza TEST_MAP_SOURCES)
    # ...
    # MAPA DE FUENTES CON VALORES FINALES
    TEST_MAP_SOURCES = {
        "Paid Search": "PAID_SEARCH",
        "Paid Social": "PAID_SOCIAL", 
        "C2C Referrals": "C2C_REFERRALS",
        "Family & Friends": "FAMILY_&_FRIENDS",
        "PR/Events/Organic": "PR_EVENTS_ORGANIC",
        "Partnerships": "PARTNERSHIPS",
        "Referrals": "REFERRALS", 
        "AI Referrals": "AI_REFERRALS", 
    }
    # ...