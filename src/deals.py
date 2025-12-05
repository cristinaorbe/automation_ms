# src/deals.py (Módulo API de Deals - LÓGICA MIXTA CONTEO/SUMA)

import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os 
from contacts import get_last_month_dates # <--- Reutilización del código

# Constantes de la API
DEALS_API_ENDPOINT = "https://api.hubspot.com/crm/v3/objects/deals/search"

# Constante para la propiedad que se SUMA en Value Partner
DEAL_VALUE_PROPERTY_VP = "properties_to_convert__from_projects_" 


# --- EL MOTOR DE BÚSQUEDA DE CONTEO (Para Sales y Desgloses) ---
def _search_deals(access_token, pipeline_id_list, additional_filters=[]):
    """
    Función interna que busca Deals y devuelve el CONTEO total.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    start_date_ms, end_date_ms = get_last_month_dates()
    
    base_filters = [
        {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"}, 
        {"propertyName": "closedate", "operator": "GTE", "value": start_date_ms},
        {"propertyName": "closedate", "operator": "LT", "value": end_date_ms},
        {"propertyName": "pipeline", "operator": "IN", "values": pipeline_id_list}
    ]
    
    all_filters = base_filters + additional_filters
    
    payload = {
        "filterGroups": [{"filters": all_filters}],
        "limit": 1 # Solo queremos el conteo total
    }

    try:
        response = requests.post(DEALS_API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status() 
        data = response.json()
        return data.get("total", 0)

    except requests.exceptions.HTTPError as err:
        print(f"\n--- ERROR DE API (Deals Conteo) ---")
        print(f"Error: {err}")
        try:
            error_data = err.response.json()
            print(f"Detalles de HubSpot: {error_data.get('message', 'No message')}")
        except:
            print(f"Detalles (no JSON): {err.response.text}")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado al buscar Deals: {e}")
        return None


# --- EL MOTOR DE BÚSQUEDA DE SUMA (Para Value Partner) ---
def _calculate_total_value(access_token, pipeline_id_list, additional_filters=[]):
    """
    Busca Deals paginando y suma el valor de DEAL_VALUE_PROPERTY_VP. 
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    start_date_ms, end_date_ms = get_last_month_dates()
    
    total_value = 0
    after = None # Para paginación
    
    base_filters = [
        {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"}, 
        {"propertyName": "closedate", "operator": "GTE", "value": start_date_ms},
        {"propertyName": "closedate", "operator": "LT", "value": end_date_ms},
        {"propertyName": "pipeline", "operator": "IN", "values": pipeline_id_list}
    ]
    
    all_filters = base_filters + additional_filters
    
    while True:
        payload = {
            "filterGroups": [{"filters": all_filters}],
            "properties": [DEAL_VALUE_PROPERTY_VP], # Solicitamos la propiedad de valor
            "limit": 100, 
            "after": after 
        }

        try:
            response = requests.post(DEALS_API_ENDPOINT, headers=headers, json=payload)
            response.raise_for_status() 
            data = response.json()

            for deal in data.get("results", []):
                value_str = deal.get("properties", {}).get(DEAL_VALUE_PROPERTY_VP, "0")
                try:
                    total_value += float(value_str)
                except ValueError:
                    pass 

            paging = data.get("paging")
            if paging and "next" in paging:
                after = paging["next"]["after"]
            else:
                break 

        except requests.exceptions.HTTPError as err:
            print(f"\n--- ERROR DE API (Deals Suma) ---")
            print(f"Error: {err}")
            try:
                error_data = err.response.json()
                print(f"Detalles de HubSpot: {error_data.get('message', 'No message')}")
            except:
                print(f"Detalles (no JSON): {err.response.text}")
            return None
        except Exception as e:
            print(f"Ocurrió un error inesperado al sumar Deals: {e}")
            return None
            
    return total_value 


# -------------------------------------------------------------------
# --- FUNCIONES PÚBLICAS PARA SER IMPORTADAS POR MAIN.PY ---
# -------------------------------------------------------------------

def get_engagements_per_pipeline(access_token, pipeline_map):
    """
    Obtiene el CONTEO para Sales y la SUMA DE VALOR para Value Partner.
    """
    print("\nObteniendo Engagements (Deals) por Pipeline (Métrica Específica)...")
    results = {}
    
    VALUE_PARTNER_ID = "188587965" # ID del pipeline de Value Partner
    
    for label, pipeline_id in pipeline_map.items():
        # ...
        if pipeline_id == VALUE_PARTNER_ID:
            # Métrica: SUMA DE VALOR
            value = _calculate_total_value(access_token, [pipeline_id], additional_filters=[])
        else:
            # Métrica: CONTEO (Para Sales y cualquier otro pipeline por defecto)
            value = _search_deals(access_token, [pipeline_id], additional_filters=[])
        
        results[label] = value
        
    return results # {'[SP] Sales': CONTEO, '[SP] Value Partners & Wealth': SUMA_DE_VALOR}

def get_engagements_breakdown_by_property(access_token, pipeline_id_list, property_name, property_map):
    """
    Obtiene el desglose de CONTEO de deals por una propiedad específica.
    Usamos CONTEO para el desglose estándar.
    """
    print(f"\nObteniendo desglose de CONTEO de Engagements por propiedad: {property_name}...")
    
    results = {}
    
    for label, internal_value in property_map.items():
        print(f"  - Buscando por: {label} (Valor API: {internal_value})")
        
        property_filter = {
            "propertyName": property_name,
            "operator": "EQ",
            "value": internal_value
        }
        
        # Usamos la función de CONTEO
        count = _search_deals(access_token, pipeline_id_list, additional_filters=[property_filter])
        results[label] = count
        
    return results