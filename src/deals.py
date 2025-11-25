import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os # Necesario para get_last_month_dates si no se importa de contacts
from contacts import get_last_month_dates # rehutilizamos la función de fechas de contacts.py

DEALS_API_ENDPOINT = "https://api.hubspot.com/crm/v3/objects/deals/search"

# --- EL "MOTOR" DE BÚSQUEDA BASE PARA DEALS (CORREGIDO) ---
def _search_deals(access_token, pipeline_id_list, additional_filters=[]):
    """
    Función interna que busca Deals. 
    Aplica filtros de pipeline, CERRADO/GANADO y fecha de CIERRE.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    start_date_ms, end_date_ms = get_last_month_dates()
    
    # 1. Filtros Base: Pipeline, Cerrado-Ganado y Fecha de CIERRE.
    base_filters = [
        {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"},    #Necesitamos que sea verdadera. 
        
        {"propertyName": "closedate", "operator": "GTE", "value": start_date_ms},
        {"propertyName": "closedate", "operator": "LT", "value": end_date_ms},      #Necesitamos info de todo el mes pasado. 
        
        {"propertyName": "pipeline", "operator": "IN", "values": pipeline_id_list}  #Solo contemplamos determinados pipelines (ValuePartner & Default). Los utilizamos en main.py y los tenemos definidos ahí. 
    ]
    
    # 2. Combinamos todos los filtros
    all_filters = base_filters + additional_filters
    
    payload = {
        "filterGroups": [{"filters": all_filters}],
        "properties": ["dealname"], 
        "limit": 1 # Solo queremos el conteo total
    }

#para los errores, como hemos hecho en contacts: 
    try:
        response = requests.post(DEALS_API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status() 
        data = response.json()
        return data.get("total", 0)

    except requests.exceptions.HTTPError as err:
        print(f"\n--- ERROR DE API (Deals) ---")
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
    

def get_engagements_per_pipeline(access_token, pipeline_map):
    """
    Obtiene el conteo total de deals para cada pipeline especificado en el mapa.
    """
    print("\nObteniendo Engagements (Deals) por Pipeline...")
    results = {}
        
    for label, pipeline_id in pipeline_map.items():
        print(f"  - Buscando Pipeline: {label} (ID: {pipeline_id})")
        
        # Para el conteo por pipeline, solo usamos el ID de ese pipeline para el filtro
        count = _search_deals(access_token, [pipeline_id], additional_filters=[])
        results[label] = count
        
    return results

def get_engagements_breakdown_by_property(access_token, pipeline_id_list, property_name, property_map):
    """
    Obtiene el desglose de deals por una propiedad específica (ej. deal_source o dealtype).
    """
    print(f"\nObteniendo desglose de Engagements por propiedad: {property_name}...")
    
    results = {}
    
    for label, internal_value in property_map.items():
        print(f"  - Buscando por: {label} (Valor API: {internal_value})")
        
        # Filtro para la propiedad y el valor actual
        property_filter = {
            "propertyName": property_name,
            "operator": "EQ", # Asumimos operador de igualdad (EQ)
            "value": internal_value
        }
        
        # Usamos todos los pipelines definidos en 'pipeline_id_list'
        count = _search_deals(access_token, pipeline_id_list, additional_filters=[property_filter])
        results[label] = count
        
    return results