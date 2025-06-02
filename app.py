import streamlit as st
import pandas as pd
import numpy as np
import base64
from io import BytesIO
import logging
import re
import traceback

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de la página de Streamlit
st.set_page_config(page_title="Procesador de Datos AFIP para ONVIO - BETA", layout="wide")

# Nuevo y Ampliado: Diccionario de mapeo de regímenes de ONVIO basados en tu tabla
# 'keywords_afip': Lista de cadenas de texto (palabras clave o frases) que se buscarán en las columnas de PERCEPCIONES de AFIP.
#                   Incluye códigos numéricos si esos códigos de AFIP corresponden directamente a este régimen ONVIO.
ONVIO_REGIMES_MAPPING = {
    'RG_140_TARJ': {'onvio_code': '140', 'onvio_article': '', 'onvio_description': 'RG. 140 - TARJ DE CREDITO', 'keywords_afip': ['140', 'TARJ DE CREDITO', 'LIQUIDACION TARJETAS']},
    'R155_10_IB_CABA': {'onvio_code': '155', 'onvio_article': '', 'onvio_description': 'R155/10 Perc.IB CABA', 'keywords_afip': ['155', 'R155/10', 'IB CABA', 'INGRESOS BRUTOS CABA']},
    'RETENCION_SUSS_LIMP_INM': {'onvio_code': '1556', 'onvio_article': '', 'onvio_description': 'Retención SUSS (Limp Inm)', 'keywords_afip': ['1556', 'SUSS', 'LIMPIEZA INMUEBLES', 'LIMPIEZA']},
    'R1574_2000_RET_IB_CABA': {'onvio_code': '1574', 'onvio_article': '', 'onvio_description': 'R 1574/2000 Ret IB CABA', 'keywords_afip': ['1574', 'R 1574/2000', 'IB CABA', 'RETENCION INGRESOS BRUTOS CABA']},
    'RG_1575_13A_RET_IVA_FC_M': {'onvio_code': '1575', 'onvio_article': '13A', 'onvio_description': 'RG 1575 Ret. IVA FC M', 'keywords_afip': ['1575', '13A', 'RET. IVA FC M', 'RG 1575', 'FACTURA M']},
    'RG_1575_13B_RET_GCIAS_FC_M': {'onvio_code': '1575', 'onvio_article': '13B', 'onvio_description': 'RG 1575 Ret. Gcias FC M', 'keywords_afip': ['1575', '13B', 'RET. GCIAS FC M', 'GANANCIAS FACTURA M']},
    'RETENCION_SUSS_I_S': {'onvio_code': '1769', 'onvio_article': '', 'onvio_description': 'Retención SUSS (I y S)', 'keywords_afip': ['1769', 'SUSS', 'SEGURIDAD SOCIAL', 'INDEMNIZACION']},
    'RETENCION_SUSS': {'onvio_code': '1784', 'onvio_article': '', 'onvio_description': 'Retención SUSS', 'keywords_afip': ['1784', 'SUSS', 'OBRAS SOCIALES']},
    'RETENCION_IVA_RG_18_A': {'onvio_code': '18', 'onvio_article': '1', 'onvio_description': 'RETENCION IVA RG 18 (A)', 'keywords_afip': ['18', '1', 'RETENCION IVA RG 18 A', 'IVA A']},
    'RETENCION_IVA_RG_18_B': {'onvio_code': '18', 'onvio_article': '2', 'onvio_description': 'RETENCION IVA RG 18 (B)', 'keywords_afip': ['18', '2', 'RETENCION IVA RG 18 B', 'IVA B']},
    'RETENCION_IVA_RG_18_C': {'onvio_code': '18', 'onvio_article': '3', 'onvio_description': 'RETENCION IVA RG 18 (C)', 'keywords_afip': ['18', '3', 'RETENCION IVA RG 18 C', 'IVA C']},
    'RET_IIBB_STA_CRUZ_DIRECTO': {'onvio_code': '192D', 'onvio_article': '', 'onvio_description': 'RET IIBB STA CRUZ DIRECTO', 'keywords_afip': ['192D', 'IIBB STA CRUZ', 'INGRESOS BRUTOS SANTA CRUZ DIRECTO']},
    'RG_212_SUJ_NO_CATEGOR': {'onvio_code': '212', 'onvio_article': '', 'onvio_description': 'RG. 212 - SUJ. NO CATEGOR', 'keywords_afip': ['212', 'NO CATEGORIZADO', 'PERCEPCION NO CATEGORIZADO']},
    'PERCEP_IVA_RG_2408': {'onvio_code': '2408', 'onvio_article': '', 'onvio_description': 'PERCEP IVA RG 2408', 'keywords_afip': ['2408', 'PERCEPCION IVA RG 2408']},
    'PERCEP_IVA_RG_2408_10_5': {'onvio_code': '2408', 'onvio_article': '2', 'onvio_description': 'PERCEP IVA RG 2408 10,5%', 'keywords_afip': ['2408', '2', 'PERCEPCION IVA RG 2408 10,5', 'IVA 10.5']},
    'RG_2616_GAN_SERVICIOS': {'onvio_code': '2616', 'onvio_article': '1', 'onvio_description': 'RG 2616 GAN - Servicios', 'keywords_afip': ['2616', '1', 'GANANCIAS SERVICIOS', 'RETENCION GANANCIAS SERVICIOS']},
    'RG_2616_GAN_BS_MUEBLES': {'onvio_code': '2616', 'onvio_article': '2', 'onvio_description': 'RG 2616 GAN - Bs Muebles', 'keywords_afip': ['2616', '2', 'GANANCIAS BIENES MUEBLES', 'RETENCION GANANCIAS BIENES']},
    'RG_2616_IVA_SERVICIOS': {'onvio_code': '2616', 'onvio_article': '4', 'onvio_description': 'RG 2616 IVA - Servicios', 'keywords_afip': ['2616', '4', 'IVA SERVICIOS', 'RETENCION IVA SERVICIOS']},
    'RG_2616_IVA_BS_MUEBLES': {'onvio_code': '2616', 'onvio_article': '5', 'onvio_description': 'RG 2616 IVA - Bs Muebles', 'keywords_afip': ['2616', '5', 'IVA BIENES MUEBLES', 'RETENCION IVA BIENES']},
    'RET_SUSS_INGENIERIA': {'onvio_code': '2682', 'onvio_article': '10', 'onvio_description': 'RET SUSS INGENIERIA', 'keywords_afip': ['2682', '10', 'SUSS INGENIERIA', 'RETENCION SUSS']},
    'RG_2784_PROF_LIBERALES_I': {'onvio_code': '2784', 'onvio_article': '1', 'onvio_description': 'RG.2784 PROF LIBERALES I.', 'keywords_afip': ['2784', '1', 'PROF LIBERALES INSC.', 'RETENCION PROFESIONALES INSC']},
    'RG_2784_PROF_LIBERALES_NI': {'onvio_code': '2784', 'onvio_article': '2', 'onvio_description': 'RG.2784 PROF LIBERALES NI', 'keywords_afip': ['2784', '2', 'PROF LIBERALES NO INSC.', 'RETENCION PROFESIONALES NO INSC']},
    'RG_2784_LOCAC_OBRA_SERV': {'onvio_code': '2784', 'onvio_article': '3', 'onvio_description': 'RG.2784 LOCAC. OBRA/SERV.', 'keywords_afip': ['2784', '3', 'LOCACION OBRAS SERVICIOS', 'RETENCION LOCACION OBRAS']},
    'RG_2784_LOC_OBRA_SERV_NI': {'onvio_code': '2784', 'onvio_article': '4', 'onvio_description': 'RG.2784 LOC. OBRA/SERV.NI', 'keywords_afip': ['2784', '4', 'LOCACION OBRAS SERVICIOS NO INSCRIPTO']},
    'RG_2784_HONORAR_DIREC_SOC': {'onvio_code': '2784', 'onvio_article': '5', 'onvio_description': 'RG.2784 HONORAR DIREC SOC', 'keywords_afip': ['2784', '5', 'HONORARIOS DIRECTORES SOCIEDADES', 'RETENCION HONORARIOS']},
    'RG_2784_ALQUILERES': {'onvio_code': '2784', 'onvio_article': '6', 'onvio_description': 'RG.2784 ALQUILERES', 'keywords_afip': ['2784', '6', 'ALQUILERES', 'RETENCION ALQUILERES']},
    'RG_2784_INTERESES': {'onvio_code': '2784', 'onvio_article': '7', 'onvio_description': 'RG.2784 - INTERESES', 'keywords_afip': ['2784', '7', 'INTERESES', 'RETENCION INTERESES']},
    'RETEN_GANANCIAS_2793_OPC': {'onvio_code': '2793', 'onvio_article': '1', 'onvio_description': 'RETEN. GANANCIAS 2793 OPC', 'keywords_afip': ['2793', '1', 'GANANCIAS OPC', 'RETENCION GANANCIAS']},
    'RET_IVA_RG_2854_BIENES': {'onvio_code': '2854', 'onvio_article': '8A', 'onvio_description': 'RET IVA RG 2854 (Bienes)', 'keywords_afip': ['2854', '8A', 'RET IVA 2854 BIENES', 'IVA BIENES']},
    'RET_IVA_RG_2854_SERVICIOS': {'onvio_code': '2854', 'onvio_article': '8B', 'onvio_description': 'RET IVA RG 2854 (Servic.)', 'keywords_afip': ['2854', '8B', 'RET IVA 2854 SERVICIOS', 'IVA SERVICIOS']},
    'RET_IVA_RG_2854_10_5': {'onvio_code': '2854', 'onvio_article': '8C', 'onvio_description': 'RET IVA RG 2854 (10,5%)', 'keywords_afip': ['2854', '8C', 'RET IVA 2854 10,5%', 'IVA 10.5']},
    'RET_IVA_RG_2854_ART9': {'onvio_code': '2854', 'onvio_article': '9', 'onvio_description': 'RET IVA RG 2854 art.9)', 'keywords_afip': ['2854', '9', 'RET IVA 2854 ART 9']},
    'RET_IVA_RG_2854_ART9_BS': {'onvio_code': '2854', 'onvio_article': '9B', 'onvio_description': 'RET IVA RG 2854 art.9) Bs', 'keywords_afip': ['2854', '9B', 'RET IVA 2854 ART 9 BIENES']},
    'RET_IVA_RG_2854_ART9_SS': {'onvio_code': '2854', 'onvio_article': '9C', 'onvio_description': 'RET IVA RG 2854 art.9) Ss', 'keywords_afip': ['2854', '9C', 'RET IVA 2854 ART 9 SERVICIOS']},
    'RETENCION_IVA_RG_3125_A': {'onvio_code': '3125', 'onvio_article': '1', 'onvio_description': 'RETENCION IVA RG.3125 (A)', 'keywords_afip': ['3125', '1', 'RETENCION IVA 3125 A', 'IVA 3125 A']},
    'RETENCION_IVA_RG_3125_B': {'onvio_code': '3125', 'onvio_article': '2', 'onvio_description': 'RETENCION IVA RG.3125 (B)', 'keywords_afip': ['3125', '2', 'RETENCION IVA 3125 B', 'IVA 3125 B']},
    'RETENCION_IVA_RG_3125_C': {'onvio_code': '3125', 'onvio_article': '3', 'onvio_description': 'RETENCION IVA RG.3125 (C)', 'keywords_afip': ['3125', '3', 'RETENCION IVA 3125 C', 'IVA 3125 C']},
    'RG_3164_RET_IVA_NO_INSC': {'onvio_code': '3164', 'onvio_article': 'NI', 'onvio_description': 'RG. 3164 RET IVA No Insc.', 'keywords_afip': ['3164', 'NI', 'IVA NO INSCRIPTO']},
    'RG_3164_RET_IVA_INSC': {'onvio_code': '3164', 'onvio_article': 'RI', 'onvio_description': 'RG. 3164 RET IVA Insc.', 'keywords_afip': ['3164', 'RI', 'IVA INSCRIPTO']},
    'RETENCION_IVA_RG_3273': {'onvio_code': '3273', 'onvio_article': '', 'onvio_description': 'RETENCION IVA RG.3273', 'keywords_afip': ['3273', 'RETENCION IVA RG 3273', 'LIQUIDACION TARJETAS']},
    'RETENC_GANANCIAS_RG_3311': {'onvio_code': '3311', 'onvio_article': '', 'onvio_description': 'RETENC. GANANCIAS RG.3311', 'keywords_afip': ['3311', 'RETENCION GANANCIAS RG 3311', 'LIQUIDACION TARJETAS', 'GANANCIAS']},
    'PERCEPCION_IVA_RG_3337_GEN': {'onvio_code': '3337', 'onvio_article': '', 'onvio_description': 'PERCEPCION IVA RG.3337', 'keywords_afip': ['3337', 'PERCEPCION IVA RG 3337', 'IVA GENERAL']}, # General para 3337 si no especifica articulo
    'PERCEP_RG_3337_ART1': {'onvio_code': '3337', 'onvio_article': '1', 'onvio_description': 'PERCEP RG 3337 ART 1', 'keywords_afip': ['3337', '1', 'PERCEP RG 3337 ART 1', 'PERCEPCION IVA RG 3337 ART 1']},
    'PERCEP_IVA_RG_3337_21': {'onvio_code': '3337', 'onvio_article': '21', 'onvio_description': 'PERCEPCION IVA RG.3337', 'keywords_afip': ['3337', '21', 'PERCEPCION IVA RG 3337', 'IVA 21%']},
    'PERCEP_IVA_10_5': {'onvio_code': '3337', 'onvio_article': '22', 'onvio_description': 'PERCEP IVA (tasa 10.5%)', 'keywords_afip': ['3337', '22', 'PERCEP IVA 10.5%', 'IVA 10.5']},
    'PERCEPCION_IVA_RG_3431_GEN': {'onvio_code': '3431', 'onvio_article': '', 'onvio_description': 'PERCEPCION IVA RG. 3431', 'keywords_afip': ['3431', 'PERCEPCION IVA RG 3431']}, # General para 3431
    'PERC_IMP_CARNES_BOBINOS_A': {'onvio_code': '3431', 'onvio_article': 'A', 'onvio_description': 'Perc. imp. carnes bobinos', 'keywords_afip': ['3431', 'A', 'CARNES BOBINOS', 'IVA CARNES A']},
    'PERC_IMP_MUEBLES_NO_BU_B1': {'onvio_code': '3431', 'onvio_article': 'B1', 'onvio_description': 'Perc.imp.Muebles No B.Uso', 'keywords_afip': ['3431', 'B1', 'MUEBLES NO BUEN USO']},
    'PERC_IMP_MUEBLES_BU_B2': {'onvio_code': '3431', 'onvio_article': 'B2', 'onvio_description': 'Perc.imp.Muebles B.Uso', 'keywords_afip': ['3431', 'B2', 'MUEBLES BUEN USO']},
    'PERC_IMP_C_MBLES_FTAS_LEG_B3': {'onvio_code': '3431', 'onvio_article': 'B3', 'onvio_description': 'Perc.imp.c.Mbles,ftas,leg', 'keywords_afip': ['3431', 'B3', 'COMBUSTIBLES FERTILIZANTES LEGUMBRES']},
    'PERCEPCION_IMPORTAC_3543_GEN': {'onvio_code': '3543', 'onvio_article': '', 'onvio_description': 'PERCEPCION IMPORTAC 3543', 'keywords_afip': ['3543', 'PERCEPCION IMPORTACION']}, # General para 3543
    'PERC_IMP_BNES_CON_CVDI_1': {'onvio_code': '3543', 'onvio_article': '1', 'onvio_description': 'Perc.Imp.bienes con CVDI', 'keywords_afip': ['3543', '1', 'BIENES CON CVDI']},
    'PERC_IMP_BNES_IMP_C_CVDI_2': {'onvio_code': '3543', 'onvio_article': '2', 'onvio_description': 'Perc.Imp.bnes imp. c/CVDI', 'keywords_afip': ['3543', '2', 'BIENES IMPORTADOS CON CVDI']},
    'PERC_IMP_BNES_IMP_S_CVDI_3': {'onvio_code': '3543', 'onvio_article': '3', 'onvio_description': 'Perc.Imp.bnes imp. s/CVDI', 'keywords_afip': ['3543', '3', 'BIENES IMPORTADOS SIN CVDI']},
    'PERC_IMP_BIENES_S_CVDI_4': {'onvio_code': '3543', 'onvio_article': '4', 'onvio_description': 'Perc. Imp. bienes s/CVDI', 'keywords_afip': ['3543', '4', 'BIENES SIN CVDI']},
    'PERC_IMP_BIENES_PARA_VTA_4_1': {'onvio_code': '3543', 'onvio_article': '4.1', 'onvio_description': 'Perc.Imp. bienes para vta', 'keywords_afip': ['3543', '4.1', 'BIENES PARA VENTA']},
    'PERC_IMP_BNES_P_USO_IMP_4_2': {'onvio_code': '3543', 'onvio_article': '4.2', 'onvio_description': 'Perc.Imp.bnes p/uso impor', 'keywords_afip': ['3543', '4.2', 'BIENES USO IMPORTADO']},
    'PERC_IMP_DEF_BIENES_5': {'onvio_code': '3543', 'onvio_article': '5', 'onvio_description': 'Perc. Imp. def. bienes', 'keywords_afip': ['3543', '5', 'BIENES DEFINITIVOS']},
    'RET_IVA_21_INSCRIP_RFPEM_24A': {'onvio_code': '3692', 'onvio_article': '24A', 'onvio_description': 'RET IVA 21% INSCRIP RFPEM', 'keywords_afip': ['3692', '24A', 'RET IVA 21% INSCRIPTO']},
    'RET_IVA_21_NO_INSC_RFPEM_24B': {'onvio_code': '3692', 'onvio_article': '24B', 'onvio_description': 'RET IVA 21% NO INSC RFPEM', 'keywords_afip': ['3692', '24B', 'RET IVA 21% NO INSCRIPTO']},
    'RET_IVA_10_5_INSCRIP_RFPEM_24C': {'onvio_code': '3692', 'onvio_article': '24C', 'onvio_description': 'RET IVA 10,5% INSCR RFPEM', 'keywords_afip': ['3692', '24C', 'RET IVA 10.5% INSCRIPTO']},
    'RET_IVA_10_5_NO_INSC_RFPEM_24D': {'onvio_code': '3692', 'onvio_article': '24D', 'onvio_description': 'RET IVA 10,5% NO IN RFPEM', 'keywords_afip': ['3692', '24D', 'RET IVA 10.5% NO INSCRIPTO']},
    'RET_IVA_27_INSCRIP_RFPEM_24E': {'onvio_code': '3692', 'onvio_article': '24E', 'onvio_description': 'RET IVA 27% INSCRIP RFPEM', 'keywords_afip': ['3692', '24E', 'RET IVA 27% INSCRIPTO']},
    'RET_IVA_27_NO_INSC_RFPEM_24F': {'onvio_code': '3692', 'onvio_article': '24F', 'onvio_description': 'RET IVA 27% NO INSC RFPEM', 'keywords_afip': ['3692', '24F', 'RET IVA 27% NO INSCRIPTO']},
    'RET_IG_RFPEM_REGALIAS_38A': {'onvio_code': '3692', 'onvio_article': '38A', 'onvio_description': 'RET IG RFPEM REGALIAS', 'keywords_afip': ['3692', '38A', 'RETENCION REGALIAS']},
    'RET_IG_NIR_BS_MUEBLES_38B1': {'onvio_code': '3692', 'onvio_article': '38B1', 'onvio_description': 'RET IG NIR - BS MUEBLES..', 'keywords_afip': ['3692', '38B1', 'RETENCION IG NIR BIENES MUEBLES']},
    'RET_IG_NIR_RESTO_OPERAC_38B2': {'onvio_code': '3692', 'onvio_article': '38B2', 'onvio_description': 'RET IG NIR - RESTO OPERAC', 'keywords_afip': ['3692', '38B2', 'RETENCION IG NIR RESTO OPERACIONES']},
    'REINTEGRO_IVA_DTO_1043_16': {'onvio_code': '3971', 'onvio_article': '', 'onvio_description': 'Reintegro IVA Dto.1043/16', 'keywords_afip': ['3971', 'REINTEGRO IVA', 'DTO 1043/16']},
    'RETENCION_SUSS_SER_EVEN': {'onvio_code': '3983', 'onvio_article': '', 'onvio_description': 'Retención SUSS (Ser Even)', 'keywords_afip': ['3983', 'SUSS SERVICIOS EVENTUALES', 'RETENCION SUSS']},
    'RG_830_INTERESES_A_INSC_A1': {'onvio_code': '830', 'onvio_article': 'A1', 'onvio_description': 'RG.830 - INTERESES a Insc', 'keywords_afip': ['830', 'A1', 'INTERESES INSCRIPTO']},
    'RG_830_INTERESES_NO_INSC_A2': {'onvio_code': '830', 'onvio_article': 'A2', 'onvio_description': 'RG.830 INTERESES No Insc', 'keywords_afip': ['830', 'A2', 'INTERESES NO INSCRIPTO']},
    'RG_830_ALQUILERES_INSCRIP_B1': {'onvio_code': '830', 'onvio_article': 'B1', 'onvio_description': 'RG.830 ALQUILERES Inscrip', 'keywords_afip': ['830', 'B1', 'ALQUILERES INSCRIPTO']},
    'RG_830_ALQUILERES_NO_INSC_B2': {'onvio_code': '830', 'onvio_article': 'B2', 'onvio_description': 'RG.830 ALQUILERES No Insc', 'keywords_afip': ['830', 'B2', 'ALQUILERES NO INSCRIPTO']},
    'ENAJEN_BIENES_MBLES_INSCRIP_F1': {'onvio_code': '830', 'onvio_article': 'F1', 'onvio_description': 'ENAJEN.BIENES MBLES Inscr', 'keywords_afip': ['830', 'F1', 'ENAJENACION BIENES MUEBLES INSCRIPTO']},
    'ENAJEN_BIENES_MBLES_NO_INSC_F2': {'onvio_code': '830', 'onvio_article': 'F2', 'onvio_description': 'ENAJ.BIENES MBL No Inscr', 'keywords_afip': ['830', 'F2', 'ENAJENACION BIENES MUEBLES NO INSCRIPTO']},
    'RG_830_LOC_OBR_SERV_INSCRIP_I1': {'onvio_code': '830', 'onvio_article': 'I1', 'onvio_description': 'RG.830 LOC. OBR/SERV.Insc', 'keywords_afip': ['830', 'I1', 'LOCACION OBRAS SERVICIOS INSCRIPTO']},
    'RG_830_LOC_OBR_SER_NO_INSC_I2': {'onvio_code': '830', 'onvio_article': 'I2', 'onvio_description': 'RG.830 LOC.OBR/SER.No Ins', 'keywords_afip': ['830', 'I2', 'LOCACION OBRAS SERVICIOS NO INSCRIPTO']},
    'RG_830_PROF_LIBER_INSCRIP_K1': {'onvio_code': '830', 'onvio_article': 'K1', 'onvio_description': 'RG.830 PROF LIBERAL Insc.', 'keywords_afip': ['830', 'K1', 'PROFESIONES LIBERALES INSCRIPTO']},
    'RG_830_PROF_LIBER_NO_INSC_K2': {'onvio_code': '830', 'onvio_article': 'K2', 'onvio_description': 'RG.830 PROF LIBER No Insc', 'keywords_afip': ['830', 'K2', 'PROFESIONES LIBERALES NO INSCRIPTO']},
    'RG_830_HONORAR_DIREC_SOC_K3': {'onvio_code': '830', 'onvio_article': 'K3', 'onvio_description': 'RG.830 HONORAR DIREC SOC', 'keywords_afip': ['830', 'K3', 'HONORARIOS DIRECTORES SOCIEDADES']},
    'RG_830_DESP_ADUANA_INSC_K4': {'onvio_code': '830', 'onvio_article': 'K4', 'onvio_description': 'RG.830 DESP ADUANA Insc', 'keywords_afip': ['830', 'K4', 'DESPACHANTES ADUANEROS INSCRIPTO']},
    'RG_830_DESP_ADUANA_NO_INSC_K5': {'onvio_code': '830', 'onvio_article': 'K5', 'onvio_description': 'RG.830 DESP ADUAN No Insc', 'keywords_afip': ['830', 'K5', 'DESPACHANTES ADUANEROS NO INSCRIPTO']},
    'RG_830_TRANS_CARGA_INSC_L1': {'onvio_code': '830', 'onvio_article': 'L1', 'onvio_description': 'RG.830 TRANS CARGA Insc', 'keywords_afip': ['830', 'L1', 'TRANSPORTE CARGA INSCRIPTO']},
    'RG_830_TRANS_CARG_NO_INSC_L2': {'onvio_code': '830', 'onvio_article': 'L2', 'onvio_description': 'RG.830 TRANS CARG No Insc', 'keywords_afip': ['830', 'L2', 'TRANSPORTE CARGA NO INSCRIPTO']},
    'RG_830_LIC_USO_SOFT_INSC_N1': {'onvio_code': '830', 'onvio_article': 'N1', 'onvio_description': 'RG.830 LIC USO SOFT. Insc', 'keywords_afip': ['830', 'N1', 'LICENCIA USO SOFTWARE INSCRIPTO']},
    'RG_830_LIC_USO_SOFT_NI_N2': {'onvio_code': '830', 'onvio_article': 'N2', 'onvio_description': 'RG.830 LIC USO SOFT. NI', 'keywords_afip': ['830', 'N2', 'LICENCIA USO SOFTWARE NO INSCRIPTO']},
    'RET_IIBB_PROV_STA_CRUZ_CM_CON1': {'onvio_code': 'CON1', 'onvio_article': '', 'onvio_description': 'RET IIBB PROV STA CRUZ CM', 'keywords_afip': ['CON1', 'IIBB STA CRUZ CM', 'RETENCION IIBB SANTA CRUZ']},
    'REGIMEN_PUENTE_CPUE8': {'onvio_code': 'CPUE', 'onvio_article': '8', 'onvio_description': 'Régimen Puente', 'keywords_afip': ['CPUE', '8', 'REGIMEN PUENTE']},
    'PERCEP_DM_672_D672': {'onvio_code': 'D672', 'onvio_article': '', 'onvio_description': 'PERCEP. DM 672', 'keywords_afip': ['D672', 'PERCEPCION DM 672']},
    'PERCEPCION_DN38_IB_DN38': {'onvio_code': 'DN38', 'onvio_article': '', 'onvio_description': 'PERCEPCION DN38 (I.B.)', 'keywords_afip': ['DN38', 'PERCEPCION DN38 IB', 'IIBB DN38']},
    'PERCEPCION_DN38_CM_DN38_1': {'onvio_code': 'DN38', 'onvio_article': '1', 'onvio_description': 'PERCEPCION DN38 (C.M.)', 'keywords_afip': ['DN38', '1', 'PERCEPCION DN38 CM']},
    'RETENCION_DN43_BS_AS_DN43': {'onvio_code': 'DN43', 'onvio_article': '', 'onvio_description': 'RETENCION DN43 (BS. AS.)', 'keywords_afip': ['DN43', 'RETENCION DN43', 'RETENCION INGRESOS BRUTOS BS AS']},
    'DNB1_PERC_IB_BS_AS_RI': {'onvio_code': 'DNB1', 'onvio_article': '', 'onvio_description': 'DNB1 Perc. IB Bs As R.I.', 'keywords_afip': ['DNB1', 'PERC IB BS AS RI', 'INGRESOS BRUTOS RI']},
    'DNB1_PERC_IB_BS_AS_RM_2': {'onvio_code': 'DNB1', 'onvio_article': '2', 'onvio_description': 'DNB1 Perc. IB Bs As R.M.', 'keywords_afip': ['DNB1', '2', 'PERC IB BS AS RM', 'INGRESOS BRUTOS RM']},
    'RET_ING_BRUTOS_BS_AS_410R': {'onvio_code': 'DNB1', 'onvio_article': '410R', 'onvio_description': 'Ret. Ing. Brutos Bs. As.', 'keywords_afip': ['DNB1', '410R', 'RETENCION INGRESOS BRUTOS BS AS']},
    'RETENCION_DNB6': {'onvio_code': 'DNB6', 'onvio_article': '', 'onvio_description': 'RETENCION DNB6', 'keywords_afip': ['DNB6', 'RETENCION DNB6', 'LIQUIDACION TARJETAS']},
    'PERCEPCION_IIBB_BS_AS_IBBA': {'onvio_code': 'IBBA', 'onvio_article': '', 'onvio_description': 'Percepcion IIBB BS. AS.', 'keywords_afip': ['IBBA', 'PERCEPCION IIBB BS AS', 'INGRESOS BRUTOS BUENOS AIRES']},
    'PERCEPCION_IIBB_CABA_IBCF': {'onvio_code': 'IBCF', 'onvio_article': '', 'onvio_description': 'Percepcion IIBB CABA', 'keywords_afip': ['IBCF', 'PERCEPCION IIBB CABA', 'INGRESOS BRUTOS CABA']},
    'PERCEPCION_IIBB_CHUBUT_IBCH': {'onvio_code': 'IBCH', 'onvio_article': '', 'onvio_description': 'Percepcion IIBB CHUBUT', 'keywords_afip': ['IBCH', 'PERCEPCION IIBB CHUBUT', 'INGRESOS BRUTOS CHUBUT']},
    'PERCEPCION_IIBB_STA_CRUZ_IBSC': {'onvio_code': 'IBSC', 'onvio_article': '', 'onvio_description': 'Percepcion IIBB STA CRUZ', 'keywords_afip': ['IBSC', 'PERCEPCION IIBB SANTA CRUZ', 'INGRESOS BRUTOS SANTA CRUZ']},
    'PERCEP_IMP_S_INTER_L25063_PINT': {'onvio_code': 'PINT', 'onvio_article': '', 'onvio_description': 'PERCEP.IMP S/INTER L25063', 'keywords_afip': ['PINT', 'INTERESES L25063', 'LIQUIDACION TARJETAS']},
    'PERCEPC_GANANC_TARJ_CRED_PTC': {'onvio_code': 'PTC', 'onvio_article': '', 'onvio_description': 'PERCEPC GANANC. TARJ.CRED', 'keywords_afip': ['PTC', 'PERCEPCION GANANCIAS TARJETA CREDITO', 'LIQUIDACION TARJETAS', 'GANANCIAS TARJETA']},
    'PUENTE_PUEN8': {'onvio_code': 'PUEN', 'onvio_article': '8', 'onvio_description': 'PUENTE', 'keywords_afip': ['PUEN', '8', 'PUENTE']},
    'RET_GAN_PERMISO_EMBARQU_RGPE': {'onvio_code': 'RGPE', 'onvio_article': '', 'onvio_description': 'Ret. Gan. Permiso Embarqu', 'keywords_afip': ['RGPE', 'RETENCION GANANCIAS PERMISO EMBARQUE']},

    # Códigos AFIP Directos (si aparecen como el campo 'Régimen' en el archivo de percepciones de AFIP)
    '493': {'onvio_code': '3337', 'onvio_article': '1', 'onvio_description': 'PERCEP RG 3337 ART 1', 'keywords_afip': ['493']}, # Mapeo directo de código AFIP
    '767': {'onvio_code': '3337', 'onvio_article': '1', 'onvio_description': 'PERCEP RG 3337 ART 1', 'keywords_afip': ['767']}, # Mapeo directo de código AFIP
    # Aquí puedes añadir más si AFIP tiene un código numérico directo que corresponde a un ONVIO_CODE específico
}


def download_excel(df, filename="plantilla_completada.xlsx"):
    """Genera un link para descargar el DataFrame como Excel"""
    output = BytesIO()
    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        excel_data = output.getvalue()
        b64 = base64.b64encode(excel_data).decode()
        return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}">**Descargar plantilla completada 📥**</a>'
    except Exception as e:
        logging.error(f"Error al generar el archivo Excel para descarga: {e}")
        return f'<p style="color:red;">Error al generar el archivo para descarga: {e}</p>'

def normalizar_numero(valor):
    """Normaliza un valor a una cadena de dígitos, útil para CUITs y números de comprobante."""
    if pd.isna(valor):
        return ""
    valor_str = str(valor).strip()
    numeros = re.findall(r'\d+', valor_str)
    if not numeros:
        return "" # Devolver vacío si no hay dígitos
    return "".join(numeros)

def extraer_tipo_y_letra_comprobante(tipo_comprobante_texto):
    """Extrae el tipo de comprobante y la letra del texto AFIP."""
    tipo = "FC" # Valor por defecto
    letra = ""
    
    if pd.isna(tipo_comprobante_texto):
        return tipo, letra
    
    tipo_comprobante_str = str(tipo_comprobante_texto).upper()
    
    # Detectar la letra (más robusto)
    # Preferir patrones como "FACTURA A" o "NCA" para mayor certeza
    if "FACTURA A" in tipo_comprobante_str or "NCA" in tipo_comprobante_str or "NDA" in tipo_comprobante_str:
        letra = "A"
    elif "FACTURA B" in tipo_comprobante_str or "NCB" in tipo_comprobante_str or "NDB" in tipo_comprobante_str:
        letra = "B"
    elif "FACTURA C" in tipo_comprobante_str or "NCC" in tipo_comprobante_str or "NDC" in tipo_comprobante_str:
        letra = "C"
    # Fallback si solo está la letra al final o con espacios
    elif tipo_comprobante_str.endswith(" A") or " A " in tipo_comprobante_str:
        letra = "A"
    elif tipo_comprobante_str.endswith(" B") or " B " in tipo_comprobante_str:
        letra = "B"
    elif tipo_comprobante_str.endswith(" C") or " C " in tipo_comprobante_str:
        letra = "C"
    
    # Detectar el tipo de comprobante
    if "FACTURA" in tipo_comprobante_str:
        tipo = "FC"
    elif "NOTA DE CREDITO" in tipo_comprobante_str or "NC" in tipo_comprobante_str:
        tipo = "NC"
    elif "NOTA DE DEBITO" in tipo_comprobante_str or "ND" in tipo_comprobante_str:
        tipo = "ND"
    elif "RECIBO" in tipo_comprobante_str or "RC" in tipo_comprobante_str:
        tipo = "RC"
    elif "TICKET" in tipo_comprobante_str or "TK" in tipo_comprobante_str:
        tipo = "TK"
    elif "COMPROBANTE" in tipo_comprobante_str: # Genérico si no se detecta nada más específico
        tipo = "OTRO"
    
    return tipo, letra

def determinar_situacion_iva(cuit, tipo_comprobante_texto):
    """Determina la situación de IVA basado en el CUIT y tipo de comprobante."""
    if pd.notna(cuit) and pd.notna(tipo_comprobante_texto):
        tipo_comprobante_str = str(tipo_comprobante_texto).upper()
        if "FACTURA A" in tipo_comprobante_str or " A " in tipo_comprobante_str:
            return "RI" # Responsable Inscripto
        elif "FACTURA B" in tipo_comprobante_str or " B " in tipo_comprobante_str:
            return "CF" # Consumidor Final (o Monotributista / Exento a RI)
        elif "FACTURA C" in tipo_comprobante_str or " C " in tipo_comprobante_str:
            return "NRI" # No Responsable Inscripto (Monotributista o Exento)
    return "RI" # Valor por defecto si no se puede determinar o si el CUIT es nulo

def mapear_codigo_regimen(codigo_afip, descripcion_afip, impuesto_afip, desc_impuesto_afip):
    """Mapea códigos de régimen de AFIP a códigos de ONVIO usando el diccionario ONVIO_REGIMES_MAPPING."""
    
    texto_combinado_upper = f"{str(codigo_afip).upper()} {str(descripcion_afip).upper()} {str(impuesto_afip).upper()} {str(desc_impuesto_afip).upper()}"
    
    best_match_data = None
    max_score = -1 # Usamos -1 para asegurarnos de que cualquier coincidencia, incluso con score 0, sea capturada
    
    # Prioridad 1: Coincidencia de código AFIP numérico directo
    codigo_afip_num_str = str(codigo_afip).split('|')[0].strip() if pd.notna(codigo_afip) else ""
    if codigo_afip_num_str.isdigit():
        for onvio_key, onvio_data in ONVIO_REGIMES_MAPPING.items():
            # Buscar el código AFIP numérico exacto dentro de las palabras clave
            if codigo_afip_num_str in onvio_data.get('keywords_afip', []):
                logging.info(f"Mapeo por código AFIP numérico directo: {codigo_afip_num_str} -> ONVIO Code: {onvio_data['onvio_code']} / Article: {onvio_data['onvio_article']}")
                return {'codigo': onvio_data['onvio_code'], 'articulo': onvio_data['onvio_article'], 'descripcion': onvio_data['onvio_description']}

    # Prioridad 2: Mapeo por palabras clave (mejor puntuación)
    for onvio_key, onvio_data in ONVIO_REGIMES_MAPPING.items():
        current_score = 0
        keywords_in_onvio = onvio_data.get('keywords_afip', [])
        
        for keyword in keywords_in_onvio:
            if keyword.upper() in texto_combinado_upper:
                # Una keyword más larga y específica da más puntos
                current_score += len(keyword.split()) * 10 # Puntos por palabra en la keyword
                current_score += 1 # Punto base por coincidencia
        
        if current_score > max_score:
            max_score = current_score
            best_match_data = onvio_data
    
    if best_match_data and max_score > 0: # Solo si hubo al menos una coincidencia de palabra clave
        logging.info(f"Mapeo por palabras clave (score: {max_score}): '{texto_combinado_upper}' -> ONVIO Code: {best_match_data['onvio_code']} / Article: {best_match_data['onvio_article']}")
        return {'codigo': best_match_data['onvio_code'], 'articulo': best_match_data['onvio_article'], 'descripcion': best_match_data['onvio_description']}
    
    # Prioridad 3: Inferencia de tipo genérico (IVA, IIBB, GAN)
    if "IVA" in texto_combinado_upper or "VALOR AGREGADO" in texto_combinado_upper:
        logging.warning(f"No se encontró mapeo específico. Inferencia genérica: IVA para '{texto_combinado_upper}'.")
        return {'codigo': '3337', 'articulo': '1', 'descripcion': 'PERCEP RG 3337 ART 1'} # Default IVA
    if "IIBB" in texto_combinado_upper or "INGRESOS BRUTOS" in texto_combinado_upper:
        logging.warning(f"No se encontró mapeo específico. Inferencia genérica: IIBB para '{texto_combinado_upper}'.")
        return {'codigo': 'IIBB', 'articulo': '', 'descripcion': 'Percepción IIBB Genérica'} # Default IIBB
    if "GANANCIA" in texto_combinado_upper or "GANANCIAS" in texto_combinado_upper:
        logging.warning(f"No se encontró mapeo específico. Inferencia genérica: Ganancias para '{texto_combinado_upper}'.")
        return {'codigo': 'GAN', 'articulo': '', 'descripcion': 'RETEN. GANANCIAS GEN'} # Default Ganancias

    # Si todo falla, devolver un valor por defecto general
    logging.warning(f"No se encontró mapeo para Régimen AFIP: '{texto_combinado_upper}'. Usando valor por defecto 'OTROS'.")
    return {'codigo': 'OTROS', 'articulo': '', 'descripcion': 'OTRAS PERCEPCIONES'}


def infer_column(df, possible_names, strict=False):
    """
    Intenta inferir el nombre de una columna de un DataFrame.
    Retorna el nombre de la columna inferida o None si no hay una única coincidencia clara.
    Si strict=True, solo busca coincidencia exacta.
    """
    df_cols = [col.strip() for col in df.columns]
    
    # Intentar coincidencia exacta primero (case-insensitive)
    for p_name in possible_names:
        for df_col in df_cols:
            if df_col.lower() == p_name.lower():
                return df_col # Retorna el nombre original de la columna en el DF

    if strict: # Si es estricto y no hay coincidencia exacta, retorna None
        return None

    # Si no es estricto, buscar coincidencias parciales o muy similares
    found_cols = []
    for p_name_option in possible_names:
        for df_col in df_cols:
            # Coincidencia con palabras clave (más tolerante)
            p_name_lower = p_name_option.lower()
            df_col_lower = df_col.lower()

            # Check if all words from possible_name are in df_col (more robust than just "in")
            if all(word in df_col_lower for word in p_name_lower.split()) and len(p_name_lower.split()) > 0:
                found_cols.append(df_col)
            # Or if the entire possible_name is a substring of df_col (or vice-versa)
            elif p_name_lower in df_col_lower or df_col_lower in p_name_lower:
                found_cols.append(df_col)
    
    found_cols = list(set(found_cols)) # Eliminar duplicados
    
    if len(found_cols) == 1:
        return found_cols[0]
    elif len(found_cols) > 1:
        # Si hay múltiples coincidencias, preferir la más corta o la que esté en la lista `possible_names`
        # Este es el punto donde la ambigüedad podría requerir intervención.
        logging.warning(f"Múltiples columnas posibles para {possible_names[0]}: {found_cols}. Se requerirá selección manual.")
        return None 
    return None

def process_and_fill_template(comprobantes_df, percepciones_df, template_df, column_map_comp, column_map_perc, column_map_template):
    """Procesa los datos de comprobantes y percepciones para completar la plantilla modelo."""
    try:
        # --- 1. Renombrar columnas de entrada a nombres estándar para el procesamiento interno ---
        # Usar .get() para manejar casos donde una columna opcional no fue mapeada (valor None)
        df_comp = comprobantes_df.rename(columns={
            column_map_comp.get('fecha_emision'): 'Fecha de Emisión',
            column_map_comp.get('tipo_comprobante'): 'Tipo de Comprobante (AFIP - Mis Comprobantes)',
            column_map_comp.get('punto_venta'): 'Punto de Venta',
            column_map_comp.get('numero_comprobante'): 'Número',
            column_map_comp.get('cuit_proveedor'): 'CUIT del Proveedor',
            column_map_comp.get('razon_social_proveedor'): 'Razón social del Provedor',
            column_map_comp.get('importe_neto'): 'Importe Neto',
            column_map_comp.get('iva_inscripto'): 'IVA Inscripto',
            column_map_comp.get('importe_exento'): 'Importe Exento',
            column_map_comp.get('impuestos_internos_no_gravado'): 'Impuestos Internos / No Gravado',
            column_map_comp.get('importe_total_comprobante'): 'Importe Total del Comprobante',
            column_map_comp.get('numero_cai'): 'Número de CAI',
            column_map_comp.get('cotizacion'): 'Cotización',
            column_map_comp.get('moneda'): 'Moneda',
            column_map_comp.get('codigo_concepto_articulo'): 'Código de Concepto / Artículo',
            column_map_comp.get('provincia_iibb'): 'Provincia IIBB',
        })
        # Asegurarse de que las columnas opcionales existan si se renombraron, si no, crearlas vacías con NaN
        for col in ['Número de CAI', 'Cotización', 'Moneda', 'Código de Concepto / Artículo', 'Provincia IIBB']:
            if col not in df_comp.columns:
                df_comp[col] = np.nan # Usar np.nan para valores ausentes

        df_perc = percepciones_df.rename(columns={
            column_map_perc.get('cuit_agente'): 'CUIT Agente Ret./Perc.',
            column_map_perc.get('numero_comprobante'): 'Número Comprobante',
            column_map_perc.get('impuesto'): 'Impuesto',
            column_map_perc.get('descripcion_impuesto'): 'Descripción Impuesto',
            column_map_perc.get('regimen'): 'Régimen',
            column_map_perc.get('descripcion_regimen'): 'Descripción Régimen',
            column_map_perc.get('importe_percepcion'): 'Importe Ret./Perc.',
        })

        # --- 2. Normalización y Limpieza de Datos ---
        # Convertir columnas numéricas a tipo numérico, forzando errores a 0
        numeric_cols_comp = ['Importe Neto', 'IVA Inscripto', 'Importe Exento', 'Impuestos Internos / No Gravado', 'Importe Total del Comprobante']
        for col in numeric_cols_comp:
            if col in df_comp.columns:
                df_comp[col] = pd.to_numeric(df_comp[col], errors='coerce').fillna(0)
        
        if 'Importe Ret./Perc.' in df_perc.columns:
            df_perc['Importe Ret./Perc.'] = pd.to_numeric(df_perc['Importe Ret./Perc.'], errors='coerce').fillna(0)
        
        # Procesar tipo y letra de comprobante y situación IVA
        df_comp['TIPO_COMPROBANTE_ESTANDAR'] = ""
        df_comp['LETRA_COMPROBANTE_ESTANDAR'] = ""
        df_comp['SITUACION_IVA_ESTANDAR'] = ""
        
        for idx in df_comp.index:
            tipo_comp_texto = df_comp.at[idx, 'Tipo de Comprobante (AFIP - Mis Comprobantes)'] if 'Tipo de Comprobante (AFIP - Mis Comprobantes)' in df_comp.columns else None
            tipo, letra = extraer_tipo_y_letra_comprobante(tipo_comp_texto)
            df_comp.at[idx, 'TIPO_COMPROBANTE_ESTANDAR'] = tipo
            df_comp.at[idx, 'LETRA_COMPROBANTE_ESTANDAR'] = letra
            
            cuit_prov = df_comp.at[idx, 'CUIT del Proveedor'] if 'CUIT del Proveedor' in df_comp.columns else None
            df_comp.at[idx, 'SITUACION_IVA_ESTANDAR'] = determinar_situacion_iva(cuit_prov, tipo_comp_texto)
        
        # Normalizar CUITs y números de comprobante para el cruce
        df_comp['CUIT_NORMALIZADO'] = df_comp['CUIT del Proveedor'].apply(normalizar_numero) if 'CUIT del Proveedor' in df_comp.columns else ""
        df_comp['NUMERO_COMPROBANTE_NORMALIZADO'] = df_comp['Número'].apply(normalizar_numero) if 'Número' in df_comp.columns else ""
        
        df_perc['CUIT_AGENTE_NORMALIZADO'] = df_perc['CUIT Agente Ret./Perc.'].apply(normalizar_numero) if 'CUIT Agente Ret./Perc.' in df_perc.columns else ""
        df_perc['NUMERO_COMPROBANTE_PERC_NORMALIZADO'] = df_perc['Número Comprobante'].apply(normalizar_numero) if 'Número Comprobante' in df_perc.columns else ""
        
        # Crear clave de unión para el cruce (CUIT del proveedor + Número de comprobante normalizado)
        df_comp['KEY'] = df_comp['CUIT_NORMALIZADO'] + '|' + df_comp['NUMERO_COMPROBANTE_NORMALIZADO']
        df_perc['KEY'] = df_perc['CUIT_AGENTE_NORMALIZADO'] + '|' + df_perc['NUMERO_COMPROBANTE_PERC_NORMALIZADO']
        
        # --- 3. Procesamiento y Cruce de Percepciones ---
        # Agrupar percepciones por la clave para sumar importes y consolidar descripciones
        percepciones_agrupadas = df_perc.groupby('KEY')['Importe Ret./Perc.'].sum().reset_index()
        percepciones_agrupadas.rename(columns={'Importe Ret./Perc.': 'SUMA_PERCEPCIONES'}, inplace=True)

        percepciones_info = df_perc.groupby('KEY').agg(
            impuesto_perc_consolidado=('Impuesto', lambda x: '|'.join(x.dropna().astype(str).unique()) if not x.dropna().empty else None),
            desc_impuesto_perc_consolidado=('Descripción Impuesto', lambda x: '|'.join(x.dropna().astype(str).unique()) if not x.dropna().empty else None),
            regimen_perc_consolidado=('Régimen', lambda x: '|'.join(x.dropna().astype(str).unique()) if not x.dropna().empty else None),
            desc_regimen_perc_consolidado=('Descripción Régimen', lambda x: '|'.join(x.dropna().astype(str).unique()) if not x.dropna().empty else None)
        ).reset_index()
        
        percepciones_completas = percepciones_agrupadas.merge(percepciones_info, on='KEY', how='left')
        
        resultado_proceso = df_comp.merge(percepciones_completas, on='KEY', how='left')
        
        # --- 4. Cálculo de Diferencias y Asignación de Percepciones ---
        # Asegurarse de que las columnas existan antes de usarlas en cálculos
        importe_neto = resultado_proceso['Importe Neto'].fillna(0) if 'Importe Neto' in resultado_proceso.columns else 0
        iva_inscripto = resultado_proceso['IVA Inscripto'].fillna(0) if 'IVA Inscripto' in resultado_proceso.columns else 0
        importe_exento = resultado_proceso['Importe Exento'].fillna(0) if 'Importe Exento' in resultado_proceso.columns else 0
        imp_int_no_grav = resultado_proceso['Impuestos Internos / No Gravado'].fillna(0) if 'Impuestos Internos / No Gravado' in resultado_proceso.columns else 0
        importe_total_comp = resultado_proceso['Importe Total del Comprobante'].fillna(0) if 'Importe Total del Comprobante' in resultado_proceso.columns else 0

        resultado_proceso['TOTAL_CALCULADO_BASE'] = importe_neto + iva_inscripto + importe_exento + imp_int_no_grav
        
        resultado_proceso['DIFERENCIA_PERCEPCION'] = importe_total_comp - resultado_proceso['TOTAL_CALCULADO_BASE']
        resultado_proceso['PERCEPCION_FINAL'] = resultado_proceso['SUMA_PERCEPCIONES'].fillna(0)
        resultado_proceso['ALERTA_DIFERENCIA_FINAL'] = ""

        for idx in resultado_proceso.index:
            # Si no se encontró percepción en el archivo de percepciones pero hay una diferencia positiva
            if resultado_proceso.at[idx, 'DIFERENCIA_PERCEPCION'] > 0.05 and resultado_proceso.at[idx, 'PERCEPCION_FINAL'] == 0:
                logging.info(f"Detectada diferencia en comprobante {resultado_proceso.at[idx, 'Número']} del CUIT {resultado_proceso.at[idx, 'CUIT del Proveedor']}. Asignando diferencia como percepción: {resultado_proceso.at[idx, 'DIFERENCIA_PERCEPCION']:.2f}")
                resultado_proceso.at[idx, 'PERCEPCION_FINAL'] = resultado_proceso.at[idx, 'DIFERENCIA_PERCEPCION']
            
            # Verificar si el total del comprobante cierra con la percepción final
            total_con_perc = resultado_proceso.at[idx, 'TOTAL_CALCULADO_BASE'] + resultado_proceso.at[idx, 'PERCEPCION_FINAL']
            if abs(importe_total_comp.at[idx] - total_con_perc) > 0.1: # Tolerancia de 0.1 para redondeo
                resultado_proceso.at[idx, 'ALERTA_DIFERENCIA_FINAL'] = f"Alerta: Diferencia final de {(importe_total_comp.at[idx] - total_con_perc):.2f}"
            
        # --- 5. Mapeo de Códigos de Régimen a formato ONVIO ---
        resultado_proceso['COD_REGIMEN_ONVIO'] = ""
        resultado_proceso['ART_REGIMEN_ONVIO'] = ""
        resultado_proceso['DESC_REGIMEN_ONVIO'] = ""
        
        for idx in resultado_proceso.index:
            if resultado_proceso.at[idx, 'PERCEPCION_FINAL'] > 0: # Solo si hay un importe de percepción final
                codigo_afip_val = resultado_proceso.at[idx, 'regimen_perc_consolidado']
                desc_regimen_afip_val = resultado_proceso.at[idx, 'desc_regimen_perc_consolidado']
                impuesto_afip_val = resultado_proceso.at[idx, 'impuesto_perc_consolidado']
                desc_impuesto_afip_val = resultado_proceso.at[idx, 'desc_impuesto_perc_consolidado']
                
                mapping = mapear_codigo_regimen(
                    codigo_afip_val, desc_regimen_afip_val, impuesto_afip_val, desc_impuesto_afip_val
                )
                
                resultado_proceso.at[idx, 'COD_REGIMEN_ONVIO'] = mapping['codigo']
                resultado_proceso.at[idx, 'ART_REGIMEN_ONVIO'] = mapping['articulo']
                resultado_proceso.at[idx, 'DESC_REGIMEN_ONVIO'] = mapping['descripcion']

        # --- 6. Preparar la Plantilla Final para ONVIO usando las columnas mapeadas ---
        template_filled = pd.DataFrame(columns=template_df.columns)
        
        # Mapeo de columnas internas estandarizadas a las de la plantilla del usuario
        internal_standard_cols_map_for_template = {
            'Fecha de Emisión': 'Fecha de Emisión',
            'Tipo de Comprobante': 'TIPO_COMPROBANTE_ESTANDAR',
            'Letra': 'LETRA_COMPROBANTE_ESTANDAR',
            'Punto de Venta': 'Punto de Venta',
            'Número': 'Número',
            'Número de CAI': 'Número de CAI',
            'Razón social del Provedor': 'Razón social del Provedor',
            'CUIT': 'CUIT del Proveedor', # Esta es la clave para tu problema original
            'Número de Documento del Cliente': 'CUIT del Proveedor', # ONVIO a veces usa esta para CUIT
            'Situación de IVA del Proveedor': 'SITUACION_IVA_ESTANDAR',
            'Cotización': 'Cotización',
            'Moneda': 'Moneda',
            'Importe Neto': 'Importe Neto',
            'IVA Inscripto': 'IVA Inscripto',
            'Importe Exento': 'Importe Exento',
            'Impuestos Internos / No Gravado': 'Impuestos Internos / No Gravado',
            'Importe Percepción': 'PERCEPCION_FINAL',
            'Importe Total del Comprobante': 'Importe Total del Comprobante',
            'Código de Concepto / Artículo': 'Código de Concepto / Artículo',
            'Provincia IIBB': 'Provincia IIBB',
            'Cód. Regimen Especial': 'COD_REGIMEN_ONVIO',
            'Art. Regimen Especial': 'ART_REGIMEN_ONVIO',
            'Desc. Regimen Especial': 'DESC_REGIMEN_ONVIO',
            'Alerta / Observación': 'ALERTA_DIFERENCIA_FINAL'
        }
        
        for _, row in resultado_proceso.iterrows():
            new_row_data = {}
            for template_col_name, internal_mapped_col_name in column_map_template.items():
                if internal_mapped_col_name in row and pd.notna(row[internal_mapped_col_name]):
                    new_row_data[template_col_name] = row[internal_mapped_col_name]
                else:
                    new_row_data[template_col_name] = None # Asegurar que los campos no mapeados o vacíos sean None
            
            # Usar pd.concat para agregar la fila al DataFrame (más eficiente para muchas filas)
            template_filled = pd.concat([template_filled, pd.DataFrame([new_row_data])], ignore_index=True)
        
        return template_filled, "Procesamiento completado correctamente"
    
    except KeyError as ke:
        error_msg = f"Error de datos: La columna esperada '{ke}' no se encontró después del mapeo. Esto podría deberse a un mapeo incorrecto o datos faltantes en tus archivos de origen."
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        return None, error_msg
    except Exception as e:
        error_msg = f"Error inesperado durante el procesamiento de datos: {e}. Por favor, revisa los archivos y las selecciones de columnas."
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        return None, error_msg

# --- Interfaz de usuario con Streamlit ---
st.title('🚀 Procesador de Datos AFIP para ONVIO 📊')

st.markdown("""
¡Bienvenido al **Procesador Inteligente de Datos AFIP para ONVIO**!
Esta herramienta consolida automáticamente tus comprobantes de compras y percepciones para generar una plantilla lista para importar, con **mínima intervención manual**.

### ¿Cómo funciona?
1.  **Sube los archivos Excel:** Comprobantes de Compras, Percepciones y tu Plantilla Modelo de ONVIO.
2.  **Detección inteligente:** La aplicación intentará identificar las columnas clave automáticamente.
3.  **Procesamiento automático:** Si todo es claro, la plantilla se generará de inmediato.
4.  **Confirmación opcional:** Solo si hay ambigüedad o dudas (o si la inferencia falla), te pediremos que confirmes algunas columnas.
""")

# --- Subida de archivos ---
st.subheader("1. Carga tus Archivos Excel")
col_files = st.columns(3)
with col_files[0]:
    comprobantes_file = st.file_uploader("📂 Archivo de Comprobantes de Compras (AFIP)", type=['xlsx', 'xls'], key="comp_uploader")
with col_files[1]:
    percepciones_file = st.file_uploader("📂 Archivo de Percepciones (AFIP)", type=['xlsx', 'xls'], key="perc_uploader")
with col_files[2]:
    template_file = st.file_uploader("📂 Tu Plantilla Modelo ONVIO", type=['xlsx', 'xls'], key="template_uploader")

df_comp, df_perc, df_template = None, None, None
can_proceed_to_process = False

# Leer archivos si se subieron
if comprobantes_file:
    try:
        df_comp = pd.read_excel(comprobantes_file)
    except Exception as e:
        st.error(f"Error al leer el archivo de comprobantes: {e}")
if percepciones_file:
    try:
        df_perc = pd.read_excel(percepciones_file)
    except Exception as e:
        st.error(f"Error al leer el archivo de percepciones: {e}")
if template_file:
    try:
        df_template = pd.read_excel(template_file)
    except Exception as e:
        st.error(f"Error al leer el archivo de la plantilla: {e}")

# Mapeo de columnas con inferencia automática
# Estos son los nombres "ideales" o "esperados" de las columnas
column_mappings_comp = {
    'fecha_emision': ['Fecha de Emisión', 'Fecha Emision', 'Fecha', 'F. Emision'],
    'tipo_comprobante': ['Tipo de Comprobante (AFIP - Mis Comprobantes)', 'Tipo Comprobante', 'Tipo', 'Tipo de Comprobante'],
    'punto_venta': ['Punto de Venta', 'Pto Vta', 'PV'],
    'numero_comprobante': ['Número', 'Numero Comprobante', 'Comprobante', 'Nro Comprobante', 'Nro. Comprobante'],
    'cuit_proveedor': ['CUIT del Proveedor', 'CUIT Proveedor', 'CUIT', 'Cuit del Proveedor'],
    'razon_social_proveedor': ['Razón social del Provedor', 'Razon Social Proveedor', 'Razon Social', 'Proveedor'],
    'importe_neto': ['Importe Neto', 'Neto Gravado', 'Neto'],
    'iva_inscripto': ['IVA Inscripto', 'IVA', 'IVA 21%', 'IVA 10.5%'],
    'importe_exento': ['Importe Exento', 'Exento'],
    'impuestos_internos_no_gravado': ['Impuestos Internos / No Gravado', 'Impuestos Internos', 'No Gravado'],
    'importe_total_comprobante': ['Importe Total del Comprobante', 'Total Comprobante', 'Importe Total'],
    'numero_cai': ['Número de CAI', 'CAI', 'Nro CAI'],
    'cotizacion': ['Cotización', 'Cotizacion'],
    'moneda': ['Moneda', 'Tipo Moneda'],
    'codigo_concepto_articulo': ['Código de Concepto / Artículo', 'Cod Concepto', 'Concepto'],
    'provincia_iibb': ['Provincia IIBB', 'Provincia'],
}

column_mappings_perc = {
    'cuit_agente': ['CUIT Agente Ret./Perc.', 'CUIT Agente', 'CUIT'],
    'numero_comprobante': ['Número Comprobante', 'Nro Comprobante', 'Comprobante'],
    'impuesto': ['Impuesto', 'Tipo Impuesto'],
    'descripcion_impuesto': ['Descripción Impuesto', 'Descripcion Impuesto', 'Impuesto Descripcion'],
    'regimen': ['Régimen', 'Regimen', 'Codigo Regimen'],
    'descripcion_regimen': ['Descripción Régimen', 'Descripcion Regimen', 'Regimen Descripcion'],
    'importe_percepcion': ['Importe Ret./Perc.', 'Importe Percepcion', 'Percepcion', 'Importe'],
}

# Columnas internas estandarizadas que el script genera (para mapear a la plantilla ONVIO)
internal_standard_cols_map_for_template = {
    'Fecha de Emisión': 'Fecha de Emisión',
    'Tipo de Comprobante': 'TIPO_COMPROBANTE_ESTANDAR',
    'Letra': 'LETRA_COMPROBANTE_ESTANDAR',
    'Punto de Venta': 'Punto de Venta',
    'Número': 'Número',
    'Número de CAI': 'Número de CAI',
    'Razón social del Provedor': 'Razón social del Provedor',
    'CUIT': 'CUIT del Proveedor', 
    'Número de Documento del Cliente': 'CUIT del Proveedor', # ONVIO a veces usa esta para CUIT
    'Situación de IVA del Proveedor': 'SITUACION_IVA_ESTANDAR',
    'Cotización': 'Cotización',
    'Moneda': 'Moneda',
    'Importe Neto': 'Importe Neto',
    'IVA Inscripto': 'IVA Inscripto',
    'Importe Exento': 'Importe Exento',
    'Impuestos Internos / No Gravado': 'Impuestos Internos / No Gravado',
    'Importe Percepción': 'PERCEPCION_FINAL',
    'Importe Total del Comprobante': 'Importe Total del Comprobante',
    'Código de Concepto / Artículo': 'Código de Concepto / Artículo',
    'Provincia IIBB': 'Provincia IIBB',
    'Cód. Regimen Especial': 'COD_REGIMEN_ONVIO',
    'Art. Regimen Especial': 'ART_REGIMEN_ONVIO',
    'Desc. Regimen Especial': 'DESC_REGIMEN_ONVIO',
    'Alerta / Observación': 'ALERTA_DIFERENCIA_FINAL'
}

# --- Lógica de inferencia y, si es necesario, confirmación manual ---
if df_comp is not None and df_perc is not None and df_template is not None:
    st.markdown("---")
    st.subheader("2. Detección de Columnas (Revisión Opcional)")
    st.info("La aplicación ha intentado detectar las columnas clave automáticamente. Si todo está **✅ Detectado automáticamente**, puedes ir directamente al Paso 3.")
    st.info("Si ves alguna **⚠️ Advertencia**, por favor, selecciona la columna correcta manualmente.")

    manual_selection_needed = False
    
    # Inferencia para Comprobantes
    st.markdown("#### Columnas del Archivo de Comprobantes:")
    final_map_comp = {}
    for key, possible_names in column_mappings_comp.items():
        inferred_col = infer_column(df_comp, possible_names, strict=False)
        if inferred_col:
            final_map_comp[key] = inferred_col
            st.markdown(f"✅ **{key.replace('_', ' ').title()}:** `{inferred_col}` (Detectado automáticamente)")
        else:
            manual_selection_needed = True
            st.warning(f"⚠️ **{key.replace('_', ' ').title()}:** No se pudo detectar con certeza o hay ambigüedad.")
            selected = st.selectbox(
                f"Por favor, selecciona la columna para '{key.replace('_', ' ').title()}' en Comprobantes:", 
                ['Seleccionar...'] + df_comp.columns.tolist(),
                key=f"manual_comp_{key}"
            )
            if selected != 'Seleccionar...':
                final_map_comp[key] = selected
            else:
                final_map_comp[key] = None # No se seleccionó

    # Inferencia para Percepciones
    st.markdown("#### Columnas del Archivo de Percepciones:")
    final_map_perc = {}
    for key, possible_names in column_mappings_perc.items():
        inferred_col = infer_column(df_perc, possible_names, strict=False)
        if inferred_col:
            final_map_perc[key] = inferred_col
            st.markdown(f"✅ **{key.replace('_', ' ').title()}:** `{inferred_col}` (Detectado automáticamente)")
        else:
            manual_selection_needed = True
            st.warning(f"⚠️ **{key.replace('_', ' ').title()}:** No se pudo detectar con certeza o hay ambigüedad.")
            selected = st.selectbox(
                f"Por favor, selecciona la columna para '{key.replace('_', ' ').title()}' en Percepciones:", 
                ['Seleccionar...'] + df_perc.columns.tolist(),
                key=f"manual_perc_{key}"
            )
            if selected != 'Seleccionar...':
                final_map_perc[key] = selected
            else:
                final_map_perc[key] = None # No se seleccionó
    
    # Mapeo para la Plantilla Modelo ONVIO (también con inferencia)
    st.markdown("#### Mapeo de Columnas de la Plantilla Modelo de ONVIO:")
    st.info("Aquí puedes ajustar qué dato se carga en cada columna de tu plantilla final. La app intentará pre-seleccionar los más comunes.")
    final_map_template = {}
    
    for template_col_name in df_template.columns:
        inferred_internal_key = None
        # Intentar inferir a qué columna interna estandarizada corresponde esta columna de la plantilla
        # Buscamos coincidencias con las claves del diccionario (ej. 'CUIT') y con los valores (ej. 'CUIT del Proveedor')
        
        # Preferencia por la clave interna (ej. 'CUIT')
        for internal_key in internal_standard_cols_map_for_template.keys():
            if template_col_name.lower() == internal_key.lower():
                inferred_internal_key = internal_key
                break
        
        # Si no se encontró por clave, intentar por el nombre estandarizado (ej. 'CUIT del Proveedor')
        if not inferred_internal_key:
            for internal_key, internal_col_name in internal_standard_cols_map_for_template.items():
                if internal_col_name and template_col_name.lower() == internal_col_name.lower():
                    inferred_internal_key = internal_key
                    break
        
        default_index = 0
        if inferred_internal_key:
            default_index = list(internal_standard_cols_map_for_template.keys()).index(inferred_internal_key) + 1 # +1 por la opción "No mapear"
            st.markdown(f"✅ **Columna '{template_col_name}':** Mapeada a `{inferred_internal_key}` (Detectado automáticamente)")
        else:
            manual_selection_needed = True # Si no se puede inferir la columna de la plantilla, también se requiere revisión
            st.warning(f"⚠️ **Columna '{template_col_name}':** No se pudo pre-seleccionar automáticamente.")

        options = ["No mapear esta columna"] + list(internal_standard_cols_map_for_template.keys())
        selected_option = st.selectbox(
            f"Columna '{template_col_name}' de la Plantilla Modelo: ¿Qué dato quieres que contenga?",
            options=options,
            index=default_index,
            key=f"template_map_{template_col_name}"
        )
        if selected_option != "No mapear esta columna":
            final_map_template[template_col_name] = internal_standard_cols_map_for_template[selected_option]
        else:
            final_map_template[template_col_name] = None
    
    # Verificar si faltan columnas esenciales después de la inferencia/selección manual
    # Consideramos "esencial" que el mapeo exista (no sea None)
    # Algunas columnas de comprobantes son opcionales para la inferencia, pero deben existir en el DF si se quieren usar.
    essential_comp_keys = [k for k in column_mappings_comp.keys() if k not in ['numero_cai', 'cotizacion', 'moneda', 'codigo_concepto_articulo', 'provincia_iibb']]
    missing_comp_cols = [k for k in essential_comp_keys if final_map_comp.get(k) is None]

    missing_perc_cols = [k for k in column_mappings_perc.keys() if final_map_perc.get(k) is None]

    
    if missing_comp_cols:
        st.error(f"❌ **Error:** Faltan mapear columnas esenciales de Comprobantes: {', '.join([k.replace('_', ' ').title() for k in missing_comp_cols])}. Por favor, selecciona la columna correcta en cada campo para poder procesar.")
        can_proceed_to_process = False
    elif missing_perc_cols:
        st.error(f"❌ **Error:** Faltan mapear columnas esenciales de Percepciones: {', '.join([k.replace('_', ' ').title() for k in missing_perc_cols])}. Por favor, selecciona la columna correcta en cada campo para poder procesar.")
        can_proceed_to_process = False
    else:
        can_proceed_to_process = True
    
    st.markdown("---")
    st.subheader("3. Procesar y Descargar")
    if can_proceed_to_process:
        if st.button('✨ Procesar Datos y Generar Plantilla Ahora', help="Haz clic para procesar los archivos"):
            with st.spinner('⏳ Procesando y validando datos... Esto puede tomar un momento...'):
                try:
                    # Limpiar mapeos de "None"
                    final_map_comp_cleaned = {k: v for k, v in final_map_comp.items() if v is not None}
                    final_map_perc_cleaned = {k: v for k, v in final_map_perc.items() if v is not None}
                    final_map_template_cleaned = {k: v for k, v in final_map_template.items() if v is not None}

                    resultado_df, mensaje = process_and_fill_template(
                        df_comp, df_perc, df_template, final_map_comp_cleaned, final_map_perc_cleaned, final_map_template_cleaned
                    )
                    
                    if resultado_df is not None:
                        st.success(f"🎉 {mensaje}")
                        
                        st.subheader("📊 Vista previa del resultado (Primeras 10 filas):")
                        st.dataframe(resultado_df.head(10))
                        
                        st.subheader("⬇️ Descarga tu plantilla completada:")
                        st.markdown(download_excel(resultado_df), unsafe_allow_html=True)
                        
                        st.subheader("📈 Resumen de Fiabilidad y Procesamiento:")
                        st.write(f"- Total de comprobantes procesados: **{len(df_comp)}**")
                        st.write(f"- Total de percepciones en el archivo de origen: **{len(df_perc)}**")
                        st.write(f"- Registros generados en la plantilla: **{len(resultado_df)}**")
                        
                        # Estadísticas de CUITs
                        if 'CUIT del Proveedor' in resultado_df.columns:
                            cuits_completados = resultado_df['CUIT del Proveedor'].apply(lambda x: pd.notna(x) and str(x).strip() != '').sum()
                            st.write(f"- CUITs de proveedor cargados en la plantilla: **{cuits_completados} de {len(resultado_df)}**")
                            if cuits_completados < len(resultado_df):
                                st.warning("⚠️ Algunos CUITs de proveedor no pudieron ser cargados o son inválidos. Revisa el archivo de origen.")
                        
                        # Estadísticas de Percepciones
                        if 'PERCEPCION_FINAL' in resultado_df.columns:
                            comprobantes_con_percepcion = resultado_df[resultado_df['PERCEPCION_FINAL'] > 0]
                            st.write(f"- Comprobantes con percepciones asignadas: **{len(comprobantes_con_percepcion)}**")
                            st.write(f"- Suma total de percepciones asignadas: **${comprobantes_con_percepcion['PERCEPCION_FINAL'].sum():,.2f}**")
                            
                            # Estadísticas de mapeo de regímenes
                            if 'COD_REGIMEN_ONVIO' in resultado_df.columns:
                                unmapped_regimes = resultado_df[resultado_df['COD_REGIMEN_ONVIO'] == 'OTROS'].shape[0]
                                if unmapped_regimes > 0:
                                    st.warning(f"❗ **Atención:** Se asignó el código 'OTROS' a **{unmapped_regimes}** percepciones. Esto significa que no se encontró un mapeo específico para estos regímenes en el diccionario interno. Es recomendable revisarlos.")
                                else:
                                    st.info("✅ Todos los regímenes de percepción se mapearon correctamente a un código ONVIO específico. ¡Excelente fiabilidad!")

                        # Estadísticas de Alertas
                        if 'ALERTA_DIFERENCIA_FINAL' in resultado_df.columns:
                            alertas_existentes = resultado_df[resultado_df['ALERTA_DIFERENCIA_FINAL'] != ""].shape[0]
                            if alertas_existentes > 0:
                                st.warning(f"🚨 Se detectaron **{alertas_existentes}** registros con 'Alertas de Diferencia Final'. Revisa la columna 'Alerta / Observación' en el Excel descargado. Estos registros podrían requerir una revisión manual.")
                            else:
                                st.info("✅ No se detectaron diferencias significativas en los totales de los comprobantes. ¡Excelente fiabilidad!")

                    else:
                        st.error(f"❌ Error en el procesamiento: {mensaje}")
                    
                except Exception as e:
                    st.error(f"Se produjo un error crítico al intentar procesar los datos: {str(e)}")
                    st.error("Por favor, verifica que las columnas mapeadas sean correctas y que los archivos estén bien formados.")
                    st.error(traceback.format_exc())
                    logging.error(f"Error crítico en la interfaz de usuario durante el procesamiento: {e}")
                    logging.error(traceback.format_exc())
    else:
        st.warning("☝️ Por favor, sube los tres archivos y/o revisa las columnas que requieren selección manual para poder procesar.")

# Pie de página
st.markdown("---")
st.markdown("Desarrollado con ❤️ usando Inteligencia Artificial para simplificar tu trabajo.")