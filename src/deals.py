# src/deals.py

import requests
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from pathlib import Path

# URL de la API para DEALS
API_ENDPOINT = "https://api.hubapi.com/crm/v3/objects/deals/search"

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

# --- MOTOR DE BÚSQUEDA GENÉRICO PARA DEALS ---
def _search_deals(access_token, filters_list):
    """
    Función interna genérica para buscar deals con los filtros que le pasemos.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "filterGroups": [{"filters": filters_list}],
        "limit": 1 # Solo queremos el conteo total
    }

    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status() 
        data = response.json()
        return data.get("total", 0)

    except requests.exceptions.HTTPError as err:
        print(f"\n--- ERROR DE API (DEALS) ---")
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

# --- FUNCIÓN PÚBLICA PARA ENGAGEMENTS ---
def get_new_engagements(access_token):
    """
    Obtiene el total de 'Engagements' (Deals ganados el mes pasado).
    """
    print("\nObteniendo total de 'Engagements' (Deals)...")
    
    start_date_ms, end_date_ms = get_last_month_dates()
    
    engagement_filters = [
        {
            "propertyName": "closedate", # Asumimos que este es correcto
            "operator": "GTE",
            "value": start_date_ms
        },
        {
            "propertyName": "closedate",
            "operator": "LT",
            "value": end_date_ms
        },
        {
            # ¡CORRECTO! Usamos el "Internal Name" que encontraste
            "propertyName": "hs_is_closed_won",
            "operator": "EQ",
            "value": True # Usamos el booleano True
        }
    ]
    
    return _search_deals(access_token, engagement_filters)


# --- BLOQUE DE PRUEBA (SOLO PARA DEPURACIÓN) ---
if __name__ == "__main__":
    """
    Esto solo se ejecuta cuando corremos 'python src/deals.py' directamente
    """
    print("--- MODO DE PRUEBA: 'deals.py' ---")
    
    # 1. Cargar el .env
    root_dir = Path(__file__).parent.parent
    env_path = root_dir / ".env"
    load_dotenv(dotenv_path=env_path)
    
    # 2. Obtener el Token
    HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
    
    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("No se encontró HUBSPOT_ACCESS_TOKEN para la prueba.")
        
    print("Token cargado. Llamando a la función 'get_new_engagements'...")
    
    # 3. Llamar a la función que queremos probar
    total_engagements = get_new_engagements(HUBSPOT_ACCESS_TOKEN)
    
    # 4. Imprimir el resultado
    if total_engagements is not None:
        print("\n--- ¡Prueba Exitosa! ---")
        print(f"Total Engagements: {total_engagements}")
    else:
        print("\n--- La prueba falló. Revisa el error de API de arriba. ---")