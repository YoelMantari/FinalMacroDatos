# DiccionarioDatos

**Nombre del Dataset:** Emisión de arbitrios municipales de la Municipalidad Distrital de Jesús María - [MDJM]

| Variable | Descripción | Tipo de dato | Tamaño | Recurso relacionado | Información Adicional |
| --- | --- | --- | --- | --- | --- |
| FECHA_CORTE | Día en el que se genero la dataset | Numérico | 8 |  | Formato: aaaammdd |
| PERIODO | Año que se emite la liquidicación correspondiente a los arbitrios municipales | Numérico | 8 |  | Ejemplo: 100000 |
| COD_CONTRIBUYENTE | Código único que identifica al contribuyente anonimizado | Texto | 500 |  |  |
| NOM_CONTRIBUYENTE | Nombre completo del contribuyente titular del predio anonimizado | Texto | 500 |  |  |
| COD_PREDIO | Código único que identifica a un predio anonimizado | Texto | 500 |  |  |
| PORCENTAJE_CONDOMINIO | Porcentaje de participación del contribuyente en un predio de propiedad compartida (condominio) | Numérico | 8 |  | Ejemplo: 100000 |
| MONTO_PARQUE_JARDIN | Monto anual que se cobra al contribuyente por el servicio de mantenimiento de parques y jardines | Numérico | 15 |  | Ejemplo: 12345.67 |
| MONTO_SERENAZGO | Monto anual que se cobra al contribuyente por el servicio de seguridad ciudadana (serenazgo) | Numérico | 15 |  | Ejemplo: 12345.68 |
| MONTO_RESIDUO_SOLIDO | Monto anual que se cobra al contribuyente por el servicio de recolección de residuos sólidos | Numérico | 15 |  | Ejemplo: 12345.69 |
| MONTO_BARRIDO_CALLE | Monto anual que se cobra al contribuyente por el servicio de limpieza y barrido de vías públicas | Numérico | 15 |  | Ejemplo: 12345.70 |
| DESCUENTO_TOPE_PARQUE_JARDIN | Descuento aplicado al monto de parques y jardines cuando supera el tope máximo permitido según normativa | Numérico | 15 |  | Ejemplo: 12345.71 |
| DESCUENTO_TOPE_SERENAZGO | Descuento aplicado al monto de serenazgo cuando supera el tope máximo permitido según normativa | Numérico | 15 |  | Ejemplo: 12345.72 |
| MONTO_TOPE_RESIDUO_SOLIDO | Descuento aplicado al monto de recolección de residuos sólidos cuando supera el tope máximo permitido según normativa | Numérico | 15 |  | Ejemplo: 12345.73 |
| DESCUENTO_TOPE_BARRIO_CALLE | Descuento aplicado al monto de barrido de calles cuando supera el tope máximo permitido según normativa | Numérico | 15 |  | Ejemplo: 12345.74 |
| DEPARTAMENTO | Departamento donde se realizó la emisión | Texto | 20 | CATALOGO DEL INEI |  |
| PROVINCIA | Provincia donde se realizó la emisión | Texto | 20 | CATALOGO DEL INEI |  |
| DISTRITO | Distrito donde se realizó la emisión | Texto | 20 | CATALOGO DEL INEI |  |
| UBIGEO | Código de Ubicación Geográfica donde se realizó la emisión | Alfanumérico | 6 | CATALOGO DEL INEI |  |
