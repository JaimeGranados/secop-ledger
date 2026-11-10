"""Which source columns we pull, and which we never pull.

Data minimization: personal data is not requested from the API, so it never reaches the disk.
A test checks that the two lists never overlap.
"""

# Columns we ask the API for (`$select`). Order = column order in the raw Parquet files.
SELECTED_COLUMNS: tuple[str, ...] = (
    # Contract identity
    "id_contrato",
    "referencia_del_contrato",
    "proceso_de_compra",
    "urlproceso",
    # Public entity
    "codigo_entidad",
    "nit_entidad",
    "nombre_entidad",
    "departamento",
    "ciudad",
    "orden",
    "sector",
    "rama",
    "entidad_centralizada",
    # Supplier (no personal ID: codigo_proveedor is SECOP's own code)
    "codigo_proveedor",
    "tipodocproveedor",
    "proveedor_adjudicado",
    "es_grupo",
    "es_pyme",
    # What and how
    "objeto_del_contrato",
    "descripcion_del_proceso",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "justificacion_modalidad_de",
    "codigo_de_categoria_principal",
    "condiciones_de_entrega",
    "origen_de_los_recursos",
    "destino_gasto",
    "espostconflicto",
    # Status and dates (tracked over time)
    "estado_contrato",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "duraci_n_del_contrato",
    "dias_adicionados",
    "el_contrato_puede_ser_prorrogado",
    "fecha_de_notificaci_n_de_prorrogaci_n",
    "liquidaci_n",
    "fecha_inicio_liquidacion",
    "fecha_fin_liquidacion",
    "ultima_actualizacion",
    # Money (tracked over time)
    "valor_del_contrato",
    "valor_de_pago_adelantado",
    "habilita_pago_adelantado",
    "valor_facturado",
    "valor_pendiente_de_pago",
    "valor_pagado",
    "valor_amortizado",
    "valor_pendiente_de",
    "valor_pendiente_de_ejecucion",
    "saldo_cdp",
    "saldo_vigencia",
    # Where the money comes from
    "presupuesto_general_de_la_nacion_pgn",
    "sistema_general_de_participaciones",
    "sistema_general_de_regal_as",
    "recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_",
    "recursos_de_credito",
    "recursos_propios",
)

# Personal data: never requested. Listed so the rule is explicit and tested.
PII_COLUMNS: frozenset[str] = frozenset(
    {
        "documento_proveedor",
        "nombre_representante_legal",
        "nacionalidad_representante_legal",
        "domicilio_representante_legal",
        "tipo_de_identificaci_n_representante_legal",
        "identificaci_n_representante_legal",
        "g_nero_representante_legal",
        "nombre_del_banco",
        "tipo_de_cuenta",
        "n_mero_de_cuenta",
        "nombre_ordenador_del_gasto",
        "tipo_de_documento_ordenador_del_gasto",
        "n_mero_de_documento_ordenador_del_gasto",
        "nombre_supervisor",
        "tipo_de_documento_supervisor",
        "n_mero_de_documento_supervisor",
        "nombre_ordenador_de_pago",
        "tipo_de_documento_ordenador_de_pago",
        "n_mero_de_documento_ordenador_de_pago",
        "direcci_n_de_ejecuci_n_del_contrato",
    }
)
