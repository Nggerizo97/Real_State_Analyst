import re
import numpy as np
import pandas as pd
import streamlit as st
from src.utils.formatters import fmt_cop

def render_tab_finance(df_master):
    st.markdown("## Simulación Financiera del Cliente")
    st.markdown(
        '<div class="disclaimer">'
        'Ingresa las capacidades financieras del cliente comprador. '
        'El simulador calculará el endeudamiento máximo permitido (tasa de esfuerzo bancaria del 30%) '
        'y filtrará los inmuebles que se ajusten a su presupuesto real.'
        '</div>',
        unsafe_allow_html=True
    )
    
    # 1. Inputs Financieros
    with st.expander("▸ Perfil Financiero del Comprador", expanded=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            salario = st.number_input(
                "Ingresos mensuales netos (COP)", 
                min_value=1_000_000, 
                max_value=100_000_000, 
                value=4_500_000, 
                step=500_000,
                format="%d",
                key="in_salario"
            )
        with f2:
            ahorros = st.number_input(
                "Ahorros iniciales (Cuota inicial) (COP)", 
                min_value=0, 
                max_value=2_000_000_000, 
                value=50_000_000, 
                step=5_000_000,
                format="%d",
                key="in_ahorros"
            )
        with f3:
            patrimonio = st.number_input(
                "Otros activos o patrimonio disponible (COP)", 
                min_value=0, 
                max_value=2_000_000_000, 
                value=15_000_000, 
                step=2_000_000,
                format="%d",
                key="in_patrimonio"
            )
            
        f_cond1, f_cond2, f_cond3 = st.columns(3)
        with f_cond1:
            tasa_anual = st.slider(
                "Tasa de Interés del Crédito (E.A. %)", 
                min_value=6.0, 
                max_value=25.0, 
                value=12.0, 
                step=0.5,
                key="in_tasa"
            )
        with f_cond2:
            plazo_anos = st.selectbox(
                "Plazo de Pago (Años)", 
                options=[10, 15, 20, 30], 
                index=2,
                key="in_plazo"
            )
        with f_cond3:
            tasa_esfuerzo = st.slider(
                "Límite de cuota sobre salario (%)",
                min_value=20,
                max_value=45,
                value=30,
                step=5,
                help="Bancos colombianos permiten máximo el 30% del salario para cuota hipotecaria VIS y 40% para No-VIS.",
                key="in_esfuerzo"
            )

        # Validación de Aptitud para Crédito
        apto_credito = True
        motivos_no_apto = []
        
        if salario < 1_400_000:
            apto_credito = False
            motivos_no_apto.append("Ingresos mensuales menores al salario mínimo legal vigente (COP 1.4M).")
        if (ahorros + patrimonio) < 10_000_000:
            apto_credito = False
            motivos_no_apto.append("Aporte propio disponible (ahorros + patrimonio) inferior a COP 10M, insuficiente para cubrir una cuota inicial mínima.")

        # Cálculos de endeudamiento según estándar financiero Colombiano
        # Convertir tasa E.A. a mensual vencida
        tasa_mensual = (1 + tasa_anual / 100) ** (1 / 12) - 1
        plazo_meses = plazo_anos * 12
        
        # Cuota máxima mensual permitida por la tasa de esfuerzo
        cuota_maxima = salario * (tasa_esfuerzo / 100)
        
        # Monto del préstamo máximo que se puede amortizar con esa cuota
        if apto_credito:
            if tasa_mensual > 0:
                prestamo_maximo = cuota_maxima * ((1 - (1 + tasa_mensual) ** (-plazo_meses)) / tasa_mensual)
            else:
                prestamo_maximo = cuota_maxima * plazo_meses
            presupuesto_total = ahorros + patrimonio + prestamo_maximo
        else:
            prestamo_maximo = 0.0
            presupuesto_total = float(ahorros + patrimonio)
        
        # Visualización de resultados financieros
        st.markdown('<hr style="border-color:rgba(255,255,255,.05);margin:.8rem 0">', unsafe_allow_html=True)
        r1, r2, r3, r4 = st.columns(4)
        
        if apto_credito:
            r1.metric("Cuota Mensual Máx.", fmt_cop(cuota_maxima), help="Basado en el porcentaje de ingresos permitido.")
            r2.metric("Crédito Hipotecario Viable", fmt_cop(prestamo_maximo), help="Monto máximo que el banco prestaría.")
            r3.metric("Aporte Propio (Cuota Inicial)", fmt_cop(ahorros + patrimonio), help="Ahorros + Otros Activos.")
            r4.metric("Presupuesto de Compra Máximo", fmt_cop(presupuesto_total), help="Patrimonio propio + capacidad máxima de crédito.")
        else:
            r1.metric("Cuota Mensual Máx.", "N/A", help="No califica para financiación.")
            r2.metric("Crédito Hipotecario Viable", "$0", help="No apto para crédito bancario.")
            r3.metric("Aporte Propio (Cuota Inicial)", fmt_cop(ahorros + patrimonio), help="Ahorros + Otros Activos.")
            r4.metric("Presupuesto de Compra Máximo", fmt_cop(presupuesto_total), help="Presupuesto limitado únicamente a fondos propios.")

        if not apto_credito:
            st.markdown(
                f'<div style="background:rgba(217, 83, 79, 0.08);border:1px solid rgba(217, 83, 79, 0.4);'
                f'padding:1.2rem;border-radius:4px;margin-top:1.2rem">'
                f'  <div style="font-weight:bold;color:#f2dede;font-size:.92rem;margin-bottom:.5rem;display:flex;align-items:center">'
                f'    ⚠️ PERFIL FINANCIERO CON RESTRICCIONES (CLIENTE NO APTO PARA CRÉDITO DE VIVIENDA)'
                f'  </div>'
                f'  <p style="font-size:.78rem;color:rgba(255,255,255,.7);line-height:1.6;margin:0">'
                f'    El cliente no cumple con las condiciones básicas para acceder a una financiación hipotecaria o adquirir un inmueble en Colombia por las siguientes razones:<br>'
                f'    <ul style="margin-top:.4rem;margin-bottom:.4rem;padding-left:1.2rem">'
                f'      {"".join([f"<li>{m}</li>" for m in motivos_no_apto])}'
                f'    </ul>'
                f'    <strong>Acción comercial recomendada:</strong> Sugerir al cliente un plan de ahorro programado para la cuota inicial o consolidar ingresos familiares con un codeudor para incrementar los ingresos demostrables ante las entidades bancarias.'
                f'  </p>'
                f'</div>',
                unsafe_allow_html=True
            )

    # 2. Buscador e Inmuebles Viables
    st.markdown("### Inmuebles que se ajustan al Presupuesto")
    
    if not apto_credito:
        st.markdown(
            '<div style="background:rgba(255,255,255,.02);border:1px dashed rgba(255,255,255,.1);'
            'padding:2.5rem;text-align:center;color:var(--muted);font-size:.85rem;border-radius:4px;margin-top:1rem">'
            '❌ <strong>Búsqueda deshabilitada</strong>: El cliente no es apto para crédito ni adquisición de vivienda.<br>'
            '<span style="font-size:.75rem; color:var(--muted)">Ajusta los parámetros financieros a la izquierda (Ingresos mensuales o Cuota inicial) para desbloquear el buscador.</span>'
            '</div>',
            unsafe_allow_html=True
        )
    else:
        with st.expander("▸ Ajustar Filtros de Propiedad", expanded=True):
            r1c1, r1c2, r1c3 = st.columns(3)
            with r1c1:
                deps_disp = sorted(list(df_master["departamento"].dropna().unique()))
                default_deps = [d for d in ["Bogotá D.C.", "Antioquia"] if d in deps_disp] or [deps_disp[0]]
                departamento_sel = st.multiselect(
                    "Filtrar por Departamento", 
                    options=deps_disp, 
                    default=default_deps,
                    key="filter_departamento"
                )
            with r1c2:
                # Filtrar ciudades por departamentos seleccionados
                if departamento_sel:
                    df_temp_mun = df_master[df_master["departamento"].isin(departamento_sel)]
                else:
                    df_temp_mun = df_master
                mun_disp = sorted(list(df_temp_mun["municipio"].dropna().unique()))
                municipio_sel = st.multiselect(
                    "Ciudad / Municipio", 
                    options=mun_disp, 
                    default=[],
                    help="Si se deja vacío, se simularán todas las ciudades del departamento.",
                    key="filter_municipio"
                )
            with r1c3:
                tipos_disp = list(df_master["tipo_inmueble"].unique())
                tipo_sel = st.multiselect(
                    "Tipo de Inmueble", 
                    options=tipos_disp, 
                    default=tipos_disp,
                    format_func=lambda x: str(x).title(),
                    key="filter_type"
                )
                
            r2c1, r2c2, r2c3 = st.columns([1, 1, 2])
            with r2c1:
                habs_min = st.selectbox(
                    "Habitaciones Mínimas", 
                    options=[1, 2, 3, 4], 
                    index=1,
                    key="filter_rooms"
                )
            with r2c2:
                order_by = st.selectbox(
                    "Ordenar resultados por",
                    options=[
                        "Mejor Inversión (Score / Rentabilidad)",
                        "Menor Precio",
                        "Mayor Precio",
                        "Mayor Área"
                    ],
                    index=0,
                    key="filter_order"
                )
            with r2c3:
                # Rango de precio interactivo en millones
                max_slider_val = max(50, int(presupuesto_total / 1e6))
                min_slider_val = min(30, max_slider_val - 10)
                default_slider_range = (min(80, max_slider_val - 20), max_slider_val)
                
                price_range_m = st.slider(
                    "Rango de Precio de Compra (Millones COP)",
                    min_value=int(min_slider_val),
                    max_value=int(max_slider_val),
                    value=(int(default_slider_range[0]), int(default_slider_range[1])),
                    step=5,
                    key="filter_price_range_m"
                )
                
        # Filtrar dataframe master
        df_filtered = df_master.copy()
        
        # Filtro de precio por slider en millones COP
        df_filtered = df_filtered[
            (df_filtered["precio_num"] >= price_range_m[0] * 1e6) & 
            (df_filtered["precio_num"] <= price_range_m[1] * 1e6)
        ]
        
        # Aplicar filtros geográficos y tipología
        if departamento_sel:
            df_filtered = df_filtered[df_filtered["departamento"].isin(departamento_sel)]
        if municipio_sel:
            df_filtered = df_filtered[df_filtered["municipio"].isin(municipio_sel)]
        if tipo_sel:
            df_filtered = df_filtered[df_filtered["tipo_inmueble"].isin(tipo_sel)]
        if habs_min:
            df_filtered = df_filtered[df_filtered["habitaciones"] >= habs_min]
            
        # Ordenar resultados
        if order_by == "Mejor Inversión (Score / Rentabilidad)":
            sort_cols = []
            sort_ascending = []
            if "score_inversion" in df_filtered.columns:
                sort_cols.append("score_inversion")
                sort_ascending.append(False)
            if "rentabilidad_potencial" in df_filtered.columns:
                sort_cols.append("rentabilidad_potencial")
                sort_ascending.append(False)
            if sort_cols:
                df_filtered = df_filtered.sort_values(sort_cols, ascending=sort_ascending)
            else:
                df_filtered = df_filtered.sort_values("precio_num", ascending=True)
        elif order_by == "Menor Precio":
            df_filtered = df_filtered.sort_values("precio_num", ascending=True)
        elif order_by == "Mayor Precio":
            df_filtered = df_filtered.sort_values("precio_num", ascending=False)
        elif order_by == "Mayor Área":
            df_filtered = df_filtered.sort_values("area_m2", ascending=False)
        
        # Mostrar resultados
        st.markdown(
            f'<div class="section-label">{len(df_filtered)} Inmuebles Viables Encontrados</div>',
            unsafe_allow_html=True
        )
        
        if df_filtered.empty:
            st.warning(
                "⚠️ No se encontraron inmuebles en ese rango de precios para las ubicaciones seleccionadas. "
                "Prueba a aumentar los ingresos del cliente, ampliar el ahorro o seleccionar más regiones."
            )
        else:
            # Dividir resultados en tabla y tarjetas interactivas
            st.write("A continuación se presentan los inmuebles ordenados según el criterio seleccionado dentro del rango de presupuesto.")
            
            # Grid de tarjetas de inmuebles (Happy Path con enlace directo)
            top_properties = df_filtered.head(12)
            cols_cards = st.columns(3)
            
            for idx, (_, row) in enumerate(top_properties.iterrows()):
                col_target = cols_cards[idx % 3]
                
                titulo = str(row.get("titulo", "Propiedad Inmobiliaria"))
                titulo_short = titulo[:38] + "..." if len(titulo) > 38 else titulo
                precio_val = row["precio_num"]
                precio_str = fmt_cop(precio_val)
                ciudad_str = str(row.get("municipio", "Municipio"))
                barrio_str = str(row.get("sector", "Sector"))
                url_target = str(row.get("url", "#"))
                fuente_portal = str(row.get("fuente", "Portal")).replace("_", " ").title()
                
                area = row.get("area_m2", 0)
                habs = row.get("habitaciones", 0)
                banos = row.get("banos", 0)
                
                rent_val = row.get("rentabilidad_potencial")
                score_val = row.get("score_inversion")
                
                rent_txt = f"{rent_val * 100:.1f}% E.A." if pd.notna(rent_val) and rent_val > 0 else "N/D"
                score_txt = f"{int(score_val)} pts" if pd.notna(score_val) and score_val > 0 else "N/D"
                
                col_target.markdown(
                    f'<div style="background:var(--surface2);border:1px solid var(--border);'
                    f'padding:1.1rem;border-radius:4px;margin-bottom:1rem;min-height:245px;'
                    f'display:flex;flex-direction:column;justify-content:space-between">'
                    f'  <div>'
                    f'    <div style="font-size:.62rem;color:var(--gold);text-transform:uppercase;letter-spacing:1px;font-weight:bold;margin-bottom:.3rem">{ciudad_str}</div>'
                    f'    <div style="font-size:.82rem;font-weight:bold;color:white;margin-bottom:.5rem;line-height:1.3">{titulo_short}</div>'
                    f'    <div style="font-size:1.15rem;font-weight:800;color:var(--gold);margin-bottom:.6rem">{precio_str}</div>'
                    f'    <div style="font-size:.72rem;color:var(--muted);margin-bottom:.8rem">'
                    f'      📐 {area:.0f}m² &nbsp;&nbsp; 🛏️ {habs:.0f} Hab &nbsp;&nbsp; 🚿 {banos:.0f} Baños'
                    f'    </div>'
                    f'    <div style="font-size:.7rem;color:white;background:rgba(255,255,255,.03);padding:.35rem .5rem;border-radius:2px;margin-bottom:.5rem">'
                    f'      📈 Rentabilidad: <span style="color:var(--gold);font-weight:bold">{rent_txt}</span> &nbsp;&nbsp;&nbsp;&nbsp;'
                    f'      🤖 IA Score: <span style="color:#2a9b6a;font-weight:bold">{score_txt}</span>'
                    f'    </div>'
                    f'  </div>'
                    f'  <div style="border-top:1px solid rgba(255,255,255,.05);padding-top:.7rem;display:flex;justify-content:space-between;align-items:center">'
                    f'    <span style="font-size:.65rem;color:var(--muted)">Soporte: <span style="color:var(--gold)">✔ Incluido</span></span>'
                    f'    <a href="{url_target}" target="_blank" style="font-size:.72rem;color:white;background:var(--gold);'
                    f'    padding:.35rem .8rem;border-radius:2px;text-decoration:none;font-weight:bold;transition:all 0.2s">Ver en {fuente_portal} ↗</a>'
                    f'  </div>'
                    f'</div>',
                    unsafe_allow_html=True
                )
                
            st.markdown("<br>", unsafe_allow_html=True)
            # Mostrar tabla detallada completa abajo
            with st.expander("▸ Ver listado completo en formato tabla", expanded=False):
                df_table = df_filtered[["titulo", "municipio", "precio_num", "area_m2", "habitaciones", "fuente"]].copy()
                df_table.columns = ["Título", "Ciudad / Municipio", "Precio (COP)", "Área (m²)", "Hab.", "Portal Fuente"]
                
                st.dataframe(
                    df_table.style.format({
                        "Precio (COP)": lambda x: f"${int(x):,}".replace(",", "."),
                        "Área (m²)": "{:.0f}",
                        "Hab.": "{:.0f}"
                    }),
                    width="stretch",
                    hide_index=True
                )
