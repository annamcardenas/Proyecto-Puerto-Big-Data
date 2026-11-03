# Memoria de la fase 1 · Diseño del centro de datos e ingesta (RA1)

> Rellenad esta plantilla en Markdown dentro del repositorio. Las imágenes van en esta misma carpeta
> (`![descripción](nombre.png)`). La entrega es el **tag de Git** `F1-entrega` + el ZIP en Moodle.

## 0. Datos de la pareja

| | Miembro 1 | Miembro 2 | Miembro 3 |
|---|---|---|---|
| Nombre | Jose | Ana | Hugo |
| Usuario de GitHub | joseGS10 | annamcardenas | Hugopdam |
| Partes de las que es responsable principal | | | |

**Horas dedicadas aproximadas:** ___  ·  **Commit final:** `______`

## 1. Resumen (máx. 10 líneas)
F1.E2 
**✏️ TAREA A1.** ¿Por qué el espacio «en disco» es aproximadamente el triple del lógico? ¿Qué propiedad de`docker/hadoop.env` lo explica? Contestad en la memoria.
Para garantizar la tolerancia a fallos y la alta disponibilidad. Propiedad DFS_REPLICATION=3. Hadoop guarda automáticamente 3 copias idénticas de cada bloque en distintos DataNodes para garantizar la tolerancia a fallo. En el fichero de configuración del clúster (docker/hadoop.env) está establecido que cada bloque se guarde 3 veces (réplica = 3).   
 
**✏️ TAREA A2.** La ingesta actual es **por lotes** (todo de golpe). Los datos AIS en realidad llegan de forma
continua. Describid en la memoria cómo haríais una ingesta **incremental** (idea: `scripts/hdfs/simular_llegada_ais.sh`)
y qué herramientas se usan en la industria (Kafka, NiFi, Flume, Spark Structured Streaming...).
Indicad para cada fuente del proyecto: frecuencia de llegada, volumen/día y mecanismo de ingesta propuesto. 

## 2. Desarrollo por criterio de evaluación

> Cada apartado se corrige con la fila correspondiente de la rúbrica. Sin evidencias, el apartado no se puntúa.
>
> **Normas de las evidencias (obligatorias):**
> - Cada apartado lleva **capturas de pantalla propias** (pantalla completa o ventana completa, no recortes sueltos)
>   en las que se vea el **reloj de Windows** y, cuando sea un cuaderno, la salida del **sello de evidencia**
>   (`sello(PAREJA)`, primera celda) o de un comando ejecutado en ese momento.
> - Guardad las capturas en esta carpeta con nombre descriptivo (`F2_D1_datanode_muerto.png`) e insertadlas
>   con `![descripción](F2_D1_datanode_muerto.png)`. Debajo de cada una, una línea explicando qué demuestra.
> - Además de las capturas: los comandos exactos, las salidas relevantes (copiadas como texto) y las tablas y gráficos generados.
> - Las evidencias deben coincidir con el historial de commits y con el cuaderno entregado.

### CE1.a · Diseño de la solución de almacenamiento

*Qué se espera:* Diagrama de arquitectura (zonas raw/bronze/silver/gold/cuarentena/control, servicios, flujos) y justificación de cada decisión. Enlazad `docs/ARQUITECTURA.md`.



### CE1.b · Ingesta de datos

*Qué se espera:* Tabla de fuentes (frecuencia, volumen/día, mecanismo). Evidencia de la ingesta por lotes (salida de `ingesta.sh`, `/puerto/control/ingestas.jsonl`). Propuesta de ingesta incremental. Lectura con esquema y tratamiento de registros corruptos (TAREA B1).

El script automatiza la carga de datos desde el sistema local hacia HDFS en tres pasos:
1. Crea la estructura de directorios en el lago de datos (/puerto/raw/, /puerto/silver/, /puerto/gold/ y /puerto/control/) utilizando hdfs dfs -mkdir -p.
2. Transfiere los ficheros crudos mediante el comando hdfs dfs -put -f. El parámetro -f (force) sobrescribe los ficheros existentes en caso de reejecución, garantizando que la operación sea idempotente (ejecutar el proceso múltiples veces produce exactamente el mismo estado en el lago sin duplicar datos).
3. Registra cada operación de subida añadiendo una línea de auditoría en formato JSONL dentro del fichero /puerto/control/ingestas.jsonl con la marca temporal, ruta de destino, tamaño y estado de la carga.
   
**Tabla de Fuentes**
1. ais (/puerto/raw/ais): Formato JSONL semi-estructurado. Frecuencia continua en tiempo real. Volumen diario de ~2,08 MB/día (124,9 MB en 60 días). Mecanismo propuesto: Streaming con particionado diario idempotente.
2. sensores (/puerto/raw/sensores): Formato CSV estructurado. Frecuencia de casi tiempo real (1 min para meteo/oleaje/aire y 5 min para grúas). Volumen diario de ~1,13 MB/día (67,6 MB en 60 días). Mecanismo propuesto: Micro-batch / Streaming hacia ficheros diarios.
3. contenedores (/puerto/raw/contenedores): Formato CSV estructurado. Frecuencia operacional diaria. Volumen diario de ~114,2 KB/día (40,7 MB en 365 días). Mecanismo propuesto: Ingesta Batch diaria al cierre de jornada desde el sistema de terminal (TOS). 
4. negocio (/puerto/raw/negocio): Formato mixto (CSV y JSONL). Frecuencia diaria (facturación, incidencias, redes sociales) y mensual (competencia, tipos de cambio). Volumen diario de ~16,5 KB/día (5,9 MB en 365 días). Mecanismo propuesto: Batch diario/mensual desde el ERP y APIs. 
5. escalas (/puerto/raw/escalas): Formato CSV estructurado. Frecuencia intradiaria/diaria. Volumen diario de ~1,54 KB/día (1,1 MB en 731 días, años 2024 y 2025). Mecanismo propuesto: Batch diario programado. 
6. maestros (/puerto/raw/maestros): Formato CSV de catálogos estáticos (muelles, navieras, buques). Baja frecuencia (bajo demanda). Volumen total de 33,6 KB (< 0,1 KB/día). Mecanismo propuesto: Batch con recarga completa (full snapshot) mediante -put -f.

**Propuesta de Ingesta Incremental para AIS:**
Dado que las antenas AIS reciben posiciones GPS de los buques cada pocos segundos, la carga por lotes diarios genera latencia. Se propone una arquitectura de ingesta en tiempo real basada en tres componentes:
1. Apache NiFi: Conectado al flujo de las estaciones receptoras AIS para capturar las tramas NMEA/JSON, validar su estructura inicial y gestionar colas con control de contrapresión (backpressure).
2. Apache Kafka: Actúa como bus de mensajería distribuido y desacoplado a través de un topic específico (puerto-ais-raw), absorbiendo picos de tráfico marítimo sin pérdida de eventos.
3. Spark Structured Streaming: Consume continuamente del topic de Kafka, aplica el esquema explícito separando posibles tramas corruptas y escribe de forma incremental en HDFS particionando por fecha (ais_AAAA-MM-DD.jsonl) mediante checkpointing para asegurar procesamiento exactly-once e idempotencia.  

### CE1.c · Formato de almacenamiento

*Qué se espera:* Tabla y gráficos de la comparativa de formatos con VUESTROS números; formato elegido por zona y por qué; particionado y problema de ficheros pequeños.



### CE1.d · Procesamiento

*Qué se espera:* Consultas Spark SQL (código + resultado) guardadas en /puerto/gold; zona bronze de todas las fuentes (TAREA D1).



### CE1.e · Presentación al cliente

*Qué se espera:* Informe ejecutivo de 1 página: ≤ 3 gráficos, 3 conclusiones en lenguaje no técnico y 1 recomendación.



## 3. Problemas encontrados y cómo los resolvimos

| Problema / mensaje de error | Causa | Solución | Fuente consultada |
|---|---|---|---|
| | | | |

## 4. Conclusiones y mejoras propuestas


## 5. Declaración de uso de herramientas

Indicad qué herramientas de IA o fuentes externas habéis usado, **para qué** y qué parte es vuestra.
Recordad: el profesor puede pedir a cualquier miembro que defienda oralmente cualquier parte. Lo que no se sepa explicar se considera no realizado (cláusula de autoría de la programación).

| Herramienta / fuente | Para qué | Partes afectadas |
|---|---|---|
| | | |
