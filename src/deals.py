# src/deals.py (MODIFICADO para aceptar fechas como argumentos y probar la iteración mensual)

import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os 
from pathlib import Path
from dotenv import load_dotenv

# Importamos solo lo necesario para la prueba, incluida la función de rangos
# Asumimos que contacts.py está en la misma carpeta src o disponible en PATH
from contacts import get_month_ranges_2025 # <-- USAMOS LA FUNCIÓN DE RANGOS CREADA ANTES

# Constantes de la API
DEALS_API_ENDPOINT = "https://api.hubspot.com/crm/v3/objects/deals/search"

# Constante para la propiedad que se SUMA en Value Partner
DEAL_VALUE_PROPERTY_VP = "properties_to_convert__from_projects_" 
VALUE_PARTNER_ID = "188587965" # ID del pipeline de Value Partner

# --- EL MOTOR DE BÚSQUEDA DE CONTEO (MODIFICADO para aceptar fechas) ---
def _search_deals(access_token, pipeline_id_list, start_date_ms, end_date_ms, additional_filters=[]):
    """
    Función interna que busca Deals y devuelve el CONTEO total.
    Acepta y usa las fechas dadas.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Filtros Base: Cerrado ganado, Rango de Fechas de cierre y Pipeline
    base_filters = [
        {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"}, 
        {"propertyName": "closedate", "operator": "GTE", "value": start_date_ms}, # Usa fecha de inicio
        {"propertyName": "closedate", "operator": "LT", "value": end_date_ms},    # Usa fecha de fin
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
        if err.response.status_code == 400:
            print(f"❌ ERROR 400: SOLICITUD INVÁLIDA (Revisar nombres de propiedades/filtros).")
            import json
            print(json.dumps(all_filters, indent=2)) 
        print(f"Error: {err}")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado al buscar Deals: {e}")
        return None


# --- EL MOTOR DE BÚSQUEDA DE SUMA (MODIFICADO para aceptar fechas) ---
def _calculate_total_value(access_token, pipeline_id_list, start_date_ms, end_date_ms, additional_filters=[]):
    """
    Busca Deals paginando y suma el valor de DEAL_VALUE_PROPERTY_VP. 
    Acepta y usa las fechas dadas.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    total_value = 0.0
    after = None # Para paginación
    
    # Filtros Base: Cerrado ganado, Rango de Fechas de cierre y Pipeline
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
            if err.response.status_code == 400:
                print(f"❌ ERROR 400: SOLICITUD INVÁLIDA (Revisar nombres de propiedades/filtros).")
                import json
                print(json.dumps(all_filters, indent=2))
            print(f"Error: {err}")
            return None
        except Exception as e:
            print(f"Ocurrió un error inesperado al sumar Deals: {e}")
            return None
            
    return total_value 


# -------------------------------------------------------------------
# --- FUNCIONES PÚBLICAS (MODIFICADAS para aceptar fechas) ---
# -------------------------------------------------------------------

def get_engagements_per_pipeline(access_token, start_date_ms, end_date_ms, pipeline_map):
    """
    Obtiene el CONTEO para Sales y la SUMA DE VALOR para Value Partner.
    Acepta y pasa las fechas.
    """
    print("\nObteniendo Engagements (Deals) por Pipeline (Métrica Específica)...")
    results = {}
    
    for label, pipeline_id in pipeline_map.items():
        
        if pipeline_id == VALUE_PARTNER_ID:
            # Métrica: SUMA DE VALOR
            value = _calculate_total_value(access_token, [pipeline_id], start_date_ms, end_date_ms, additional_filters=[])
        else:
            # Métrica: CONTEO (Para Sales y cualquier otro pipeline por defecto)
            value = _search_deals(access_token, [pipeline_id], start_date_ms, end_date_ms, additional_filters=[])
        
        results[label] = value
        
    return results # {'[SP] Sales': CONTEO, '[SP] Value Partners & Wealth': SUMA_DE_VALOR}

def get_engagements_breakdown_by_property(access_token, pipeline_id_list, start_date_ms, end_date_ms, property_name, property_map):
    """
    Obtiene el desglose de CONTEO de deals por una propiedad específica.
    Acepta y pasa las fechas.
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
        count = _search_deals(access_token, pipeline_id_list, start_date_ms, end_date_ms, additional_filters=[property_filter])
        results[label] = count
        
    return results

# --- BLOQUE DE PRUEBA (MODIFICADO para iterar sobre TODOS los meses) ---
if __name__ == "__main__":
    print("--- Prueba de deals.py: Adquisición Mensual Completa ---")
    
    # 1. Cargar .env para obtener el token
    root_dir = Path(__file__).parent.parent
    load_dotenv(dotenv_path=root_dir / ".env")
    HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")

    # 2. Configuración de prueba
    SALES_PIPELINE_ID = "default"
    PIPELINE_MAP = {"[SP] Sales": SALES_PIPELINE_ID, "[SP] Value Partners & Wealth": VALUE_PARTNER_ID}
    PIPELINE_ID_LIST = list(PIPELINE_MAP.values())

    if not HUBSPOT_ACCESS_TOKEN:
        print("❌ ERROR: No se encontró HUBSPOT_ACCESS_TOKEN. Configure el archivo .env.")
    else:
        # 3. Obtener rangos de fecha para todos los meses completos de 2025
        try:
            ranges = get_month_ranges_2025()
        except ImportError:
            print("❌ ERROR: Asegúrese que 'get_month_ranges_2025' está en contacts.py y guardado.")
            exit()
            
        if not ranges:
             print("⚠️ No hay rangos de meses para 2025 generados todavía (verifique la fecha actual).")
             exit()

        print(f"✅ Rangos generados para {len(ranges)} meses: {list(ranges.keys())}")
        print("\n--- Ejecutando todas las consultas de Deals para CADA MES ---")
        
        # 4. Iterar sobre CADA MES y adquirir todos los datos solicitados
        for month_label, (start_ms, end_ms) in ranges.items():
            print(f"\nProcessing **{month_label}**...")

            # 4.1. Engagements por Pipeline (Ventas y VP)
            pipeline_totals = get_engagements_per_pipeline(HUBSPOT_ACCESS_TOKEN, start_ms, end_ms, PIPELINE_MAP)
            
            # 4.2. Desglose de Fuente (Ejemplo de una llamada de desglose)
            DEAL_SOURCE_PROP_NAME = "deal_source"
            DEAL_SOURCE_MAP_RAW = {"Organic": "Organic", "Outbound Sales": "Outbound Sales"} # Mapeo simplificado para prueba
            deal_source_breakdown = get_engagements_breakdown_by_property(
                HUBSPOT_ACCESS_TOKEN, PIPELINE_ID_LIST, start_ms, end_ms, DEAL_SOURCE_PROP_NAME, DEAL_SOURCE_MAP_RAW
            )

            print(f"  - 💰 Totales Pipeline: {pipeline_totals}")
            print(f"  - 📈 Desglose Fuente: {deal_source_breakdown}")
            
        print("\n--- ✅ PRUEBA DE DEALS COMPLETA ---")