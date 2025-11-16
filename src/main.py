# src/main.py

import os
import pprint
import csv
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from contacts import (
    get_total_new_leads, 
    get_leads_by_country, 
    get_leads_by_traffic_source
)
# Importamos la función de nuestro archivo deals.py
from deals import get_new_engagements

# La función de CSV ahora acepta el nuevo dato
def write_results_to_csv(total_engagements, total_leads, countries_data, sources_data_grouped):
    """
    Crea un archivo CSV con los resultados agrupados.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"reporte_hubspot_{today_str}.csv"
    
    print(f"\nEscribiendo resultados en el archivo: {filename}")
    
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # --- Sección Métricas Principales ---
            writer.writerow(["Metrica", "Valor"])
            # Añadimos la fila de Engagements
            writer.writerow(["Total Engagements (Mes Pasado)", total_engagements])
            writer.writerow(["Total Nuevos Leads (Mes Pasado)", total_leads])
            writer.writerow([]) # Fila vacía como separador
            
            # --- Sección Países ---
            writer.writerow(["Pais", "Nuevos Leads (Multi-Checkbox)"])
            for country, count in countries_data.items():
                writer.writerow([country, count])
            writer.writerow([]) # Fila vacía
            
            # --- Sección Fuentes de Tráfico (Agrupada) ---
            writer.writerow(["Fuente de Trafico Agrupada", "Nuevos Leads"])
            for group_name, count in sources_data_grouped.items():
                writer.writerow([group_name, count])
            writer.writerow([])

        print(f"¡Éxito! Reporte guardado en {filename}")
        print(f"Ruta completa: {Path.cwd() / filename}")

    except Exception as e:
        print(f"Error al escribir el archivo CSV: {e}")


def main():
    print("Iniciando el script de automatización...")
    
    # --- Carga de .env ---
    root_dir = Path(__file__).parent.parent
    env_path = root_dir / ".env"
    load_dotenv(dotenv_path=env_path)
    
    HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("No se encontró HUBSPOT_ACCESS_TOKEN.")
    
    print("Token de HubSpot cargado con éxito.")

    # --- 0. OBTENER ENGAGEMENTS ---
    total_engagements = get_new_engagements(HUBSPOT_ACCESS_TOKEN)
    if total_engagements is not None:
        print(f"\n--- Total de Engagements (Mes Pasado, Deals) ---")
        print(f"Total: {total_engagements}")
        print("--------------------------------------------------")

    # --- 1. OBTENER TOTAL DE NUEVOS LEADS ---
    total_leads = get_total_new_leads(HUBSPOT_ACCESS_TOKEN)
    if total_leads is not None:
        print(f"\n--- Total de Nuevos Leads (Mes Pasado, Global) ---")
        print(f"Total: {total_leads}")
        print("--------------------------------------------------")

    # --- 2. OBTENER LEADS POR PAÍS ---
    countries_to_check = ["Spain", "Ireland", "Indonesia", "Australia", "Unknown"]
    country_leads = get_leads_by_country(HUBSPOT_ACCESS_TOKEN, countries_to_check)
    if country_leads:
        print(f"\n--- Desglose por País (Mes Pasado) ---")
        pprint.pprint(country_leads)
        print("--------------------------------------")

    # --- 3. OBTENER LEADS POR FUENTE DE TRÁFICO (DATOS CRUDOS) ---
    sources_map = {
        "Paid Social": "PAID_SOCIAL",
        "Paid Search": "PAID_SEARCH",
        "Organic Search": "ORGANIC_SEARCH",
        "Organic Social": "ORGANIC_SOCIAL",
        "Direct Traffic": "DIRECT_TRAFFIC", 
        "Email marketing": "EMAIL_MARKETING",
        "Referrals": "REFERRALS",
        "AI Referrals": "AI Referrals", 
        "Offline Sources": "OFFLINE",
        "Other campaigns": "OTHER_CAMPAIGNS",
    }
    source_leads_raw = get_leads_by_traffic_source(HUBSPOT_ACCESS_TOKEN, sources_map)
    
    if not source_leads_raw:
        print("No se pudieron obtener los datos de fuentes de tráfico. Saliendo.")
        return

    # --- 4. PROCESAR Y AGRUPAR FUENTES ---
    print("\nAgrupando resultados de fuentes de tráfico...")

    def get_count(key):
        return source_leads_raw.get(key, 0) or 0

    grouped_sources = {
        "Paid online Marketing": get_count("Paid Social") + get_count("Paid Search"),
        "  - Paid Social (META)": get_count("Paid Social"),
        "  - Paid Search (GOOGLE)": get_count("Paid Search"),
        "PR / Events / Organic": get_count("Organic Search") + get_count("Organic Social") + get_count("Direct Traffic") + get_count("Email marketing"),
        "C2C Referrals": get_count("Referrals") + get_count("AI Referrals"),
        "Offline": get_count("Offline Sources"),
        "Other campaigns": get_count("Other campaigns")
    }
    
    print(f"\n--- Desglose por Fuente de Tráfico (Agrupado) ---")
    pprint.pprint(grouped_sources)
    print("--------------------------------------------------")


    # --- 5. ESCRIBIR TODO A CSV ---
    # Pasamos el nuevo dato 'total_engagements'
    if total_engagements is not None and total_leads is not None and country_leads and grouped_sources:
        write_results_to_csv(total_engagements, total_leads, country_leads, grouped_sources)
    else:
        print("No se generó el CSV porque faltaron datos de la API.")

if __name__ == "__main__":
    main()