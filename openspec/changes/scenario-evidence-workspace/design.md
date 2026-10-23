# Design

## Context

Ver `proposal.md` para la motivación y los dos deltas para la aceptación.
La consola actual usa HTML, CSS y JavaScript sin framework. `renderScenarioResult`
presenta comprobaciones, pero sus enlaces llaman a `selectRecord`/`viewOrder` y
cambian de vista. El detalle de ledger se limita inicialmente al JSON desplegable.

`DemoRun` ya registra mensajes con escenario/paso, órdenes con etiquetas y checks
con `references` y `observedAt`. Los snapshots, resultados y salidas están repartidos
en referencias con formas diferentes; los avisos DLQ no están registrados como
salidas leídas en todos los recorridos. La respuesta no es todavía un modelo uniforme
para la ficha. `ConsoleQueries`, `ConsoleKafka` y los recorridos de recuperación
proporcionan las lecturas necesarias. El historial es acotado y sólo vive en memoria.

Las specs actuales exigen mantener las otras vistas y ubican la evidencia técnica
tras un desplegable. El delta de `evaluation-console` modifica esa presentación:
las vistas pasan a Herramientas avanzadas y los datos esenciales quedan visibles
en el escenario; el payload crudo conserva su despliegue opcional.

## Goals / Non-Goals

**Goals:**
- Una ficha verificable que correlacione evidencia real sin interpretar prosa ni
  consultar todo el historial de Kafka o SQL en cada refresco.
- Reusar lecturas, runner, contratos y controles, con presentación progresiva y
  cambios de captura limitados a `demo`.
- Mantener separadas observación histórica, consulta actual y disponibilidad de
  evidencia, incluso con fallas o cancelaciones.

**Non-Goals:**
- Cambiar el procesamiento de ER, esquema SQL, publicación o política de errores.
- Agregar framework, gestor de estado, build frontend, tablas de demo, persistencia
  de runs, streaming, nuevos escenarios o un motor genérico de eventos.
- Reimplementar las verificaciones del servidor en el navegador o atribuir efectos
  a una instancia a partir de su asignación actual.

## Decisions

### 1. Escenario como lugar de trabajo; herramientas generales secundarias

Conservar selector lateral y usar el área principal para una sola ficha. En orden:
objetivo breve y esperado; Ejecutar y modo; avance/acción actual; conclusión y
comprobaciones; tabla de mensajes; órdenes con ledger; salidas e incidentes relevantes.
Los pasos detallados y JSON se despliegan cuando se necesitan. Automático sigue
siendo el valor inicial; paso a paso, cancelar y ejecutar todos siguen disponibles.

La navegación principal ofrece Escenarios y Herramientas avanzadas. Esta última
agrupa Mensajes, Órdenes y Sistema. Conservar `#messages`, `#orders`, `#system` y
`#scenarios/<runId>/<scenarioId>`: abrir un hash general selecciona la herramienta
correspondiente. Seleccionar escenario o volver desde herramientas no publica,
reinicia ni inicia automáticamente una ejecución. Un run no disponible muestra
esa condición y permite una ejecución nueva sólo por acción explícita.

Alternativa descartada: borrar herramientas o iniciar al seleccionar. Pierde
inspección útil y convierte navegación en acciones que pueden terminar procesos.

### 2. Ampliación aditiva de la respuesta existente

Agregar `evidence` a cada elemento de `scenarioResults` de `GET /demo/api/runs/{id}`.
No crear endpoints de negocio ni reemplazar `messages`, `orders`, `steps`, `checks`,
`references`, `restoration` o sus significados. Mantener los ocho IDs y las APIs
usadas por smokes. El nuevo bloque contiene tres colecciones:

| Colección | Datos de presentación y correlación |
| --- | --- |
| `messages` | Envíos de entrada en orden de publicación, id/posición completa, paso, key, payload original, condición transaccional, resumen del ER y observación opcional de resultado SQL. |
| `orderSnapshots` | Orden/etiqueta, paso y check de captura, fecha, estado/cantidades/contador y ledger completo ordenado. Conserva las observaciones sucesivas. |
| `outputs` | Registros observados de settlement o rechazo, posición propia, key, payload, tipo, posición de entrada referida y alcance/fecha de lectura. |

La ejecución y escenario son el contenedor de las tres colecciones; cada elemento
conserva paso, fuente y fecha. Los casos de recuperación y auditoría reutilizan
checks/referencias estructurados ya existentes, con renderers explícitos por sus
IDs estables. No recorrer recursivamente JSON arbitrario ni interpretar títulos
para decidir qué es un resultado, un snapshot o una falla.

`sqlObservations` conserva una lista de lecturas por mensaje para que una consulta
posterior no sobrescriba una observación anterior; `sqlObservationStatus` expone el
estado más reciente. Los estados son `NOT_QUERIED`, `NO_RESULT`, `RESULT_FOUND` y
`QUERY_FAILED`. No se usa un único null para esos hechos.
Un resultado conserva su snapshot, no el estado del ER ni la orden actual. El
resumen del ER expone `fixId`, estado y cantidades como datos separados; JSON
inválido conserva el original y deja esos campos sin dato.

Alternativa descartada: un agregado nuevo que consulte dependencias cada vez que
se pide un run. Hace que evidencia histórica cambie o desaparezca durante fallas
y suma costo de lectura al refresco de dos segundos.

### 3. Capturar donde ya se observa; conservar hechos parciales

Incorporar métodos pequeños de captura en `DemoRun` y llamadas explícitas en los
recorridos, sin escribir en tablas ni cambiar las barreras:

- `message` ya conoce la posición y el payload confirmado por el productor:
  registra esa evidencia inmediatamente, incluido el abort deliberado.
- `ScenarioSteps.result`/`assertResults` capturan lecturas SQL de la posición;
  después de observar offsets confirmados, completar las posiciones que aún
  carezcan de observación con `ConsoleQueries.result` en el mismo recorrido.
- `expectOrder` captura el snapshot de una sola lectura estado/ledger. Los
  recorridos de recuperación capturan también las órdenes auxiliares que hoy
  consultan sin pasar por ese helper. Conservar cada observación por paso/check;
  identificar explícitamente antes/después del reinicio y progresión de snapshots.
- Los lectores existentes de settlement registran el rango y los registros leídos
  al comprobar la salida. Para cuarentena, usar el lector read_committed existente
  de rechazos y correlacionar `rejectionId`/source de la posición propia; un marcador
  `notified` SQL no se etiqueta como un registro leído del broker.
- Capturar original y auditoría desde las lecturas existentes de cuarentena.
  Mantenerlos en las referencias del check con su posición y fecha; el mensaje de
  entrada conserva el original y el resultado apunta al ER efectivo del ledger.

Las lecturas adicionales se limitan a posiciones/órdenes de la corrida y a los
topics relevantes. Reutilizar los presupuestos y cursores existentes, sin escaneo
ilimitado ni lectura nueva dentro de una ventana crítica de barrera. Una lectura
auxiliar fallida queda como evidencia no disponible; no crea un éxito, reintento
de negocio o excepción que sustituya el diagnóstico de procesamiento. Las
verificaciones existentes siguen siendo la autoridad de PASSED/FAILED.

El GET de run sólo serializa una copia coherente de la memoria de `DemoRun`, sin
acceso a SQL/Kafka. Una observación nueva amplía la historia sin reescribir las
anteriores. Se conservan las observaciones previas al fallo/cancelación. La
retención sigue siendo la actual: hasta 50 runs por proceso, sin recuperación del
historial al reiniciar la consola.

Alternativa descartada: reconstruir el pasado desde `orders`, o extraer evidencia
de textos de pasos. Ambos pierden la semántica de snapshot y su correlación.

### 4. Selección correlacionada dentro de la ficha

La tabla de entrada muestra orden, fixId, estado informado, partición/offset,
resultado SQL y ledgerId. Distingue enviado, condición transaccional, resultado y
verificación; no cuenta un envío abortado como visible read_committed. Mantiene
el orden del productor y no ordena por `transactionTime` ni compara offsets de
particiones distintas como si fueran una secuencia global.

Al pulsar mensaje o ledger, guardar selección por run/escenario/posición/ledgerId,
resaltar la relación y abrir un detalle dentro de la ficha. Mostrar el resultado
histórico y la entrada de ledger, con payload completo plegable. DUPLICATE enlaza
una fila existente; ausencia de resultado no crea una fila ni diagnostica una falla.

Las órdenes muestran primero su última observación capturada, con fuente y hora,
y ledger tabular (ledgerId, fixId, estado y cantidades del ER). Las observaciones
anteriores son seleccionables dentro de la misma sección. Una acción separada
Consultar estado actual usa el endpoint existente y presenta su respuesta con
otra fecha; no reemplaza los snapshots de ejecución. Dos órdenes se comparan en
columnas en escritorio; con más órdenes se usa un selector/listado compacto de
las propias del escenario. El ledger y detalle siguen en la ficha.

Los pasos/casos filtran evidencia sin mezclar corridas. Los cambios de selección
invalidan respuestas de lecturas anteriores. El refresco conserva foco, fila
seleccionada y desplegables; sólo vuelve a renderizar contenido modificado. En
375 px las secciones se apilan con tablas desplazables localmente, sin desborde
horizontal de toda la página. Estados textuales acompañan al color y hay foco visible.

Alternativa descartada: abrir modales anidados o cambiar a las vistas generales.
Oculta el contexto necesario para comparar el mensaje con su efecto.

### 5. Cantidades y exactitud en el navegador

Usar Cantidad solicitada (`nominalAmounts`), Cantidad ejecutada
(`accumulativeNominalAmount`) y Pendiente de ejecutar (`leavesNominalAmount`).
Estado legible conserva el código técnico al lado; ER aplicados y entradas de
ledger se muestran como conteos separados. Para CANCELLED explicar que total menos
ejecutada quedó sin ejecutar, mientras pendiente es cero por cierre de la orden.
La diferencia es explicación de UI, no una nueva cantidad persistida ni liquidación.

Producir resúmenes de payload y diferencia decimal en Java con Jackson/BigDecimal
y serializarlos como cadenas usando los patrones de `ConsoleValues`. No convertir
IDs, offsets, contadores o cantidades a `Number` de JavaScript ni modificar el
payload original al formatear; sólo quitar ceros fraccionarios innecesarios en
presentación. No derivar las cantidades del snapshot SQL de un duplicado como si
fueran los valores de su ER. Mostrar contenidos con `textContent`.

Alternativa descartada: parsear/reconstruir el JSON completo en el navegador para
extraer valores numéricos. Puede redondear IDs de 64 bits y cantidades.

### 6. Ocho escenarios y evidencia específica

| Escenario | Presentación principal adicional a mensajes, órdenes y ledger |
| --- | --- |
| `interleaved-orders` | A FILLED con 4 ER/filas; B CANCELLED con 3; particiones y cantidades. |
| `quantity-snapshots` | Observaciones 0/3/7/10 y reloj de origen separado de la secuencia. |
| `terminal-duplicates` | 4 envíos, 2 aplicaciones, 2 duplicados y 2 filas; relación NEW original/republicado. |
| `restart-persistence` | Antes/después capturados, igualdad y republicaciones; reinicio posterior al lote. |
| `committed-input-only` | Parcial abortado como envío del productor, ausencia verificada de efecto SQL y ledger 1/3. |
| `settlement-once` | Salidas de FILLED/CANCELLED y finalización recuperada, con source y alcance de lectura. |
| `commit-boundary-recovery` | Casos por frontera: observación previa, acción, resultado recuperado, salida y restauración. |
| `poison-recovery` | Original, pausa, aviso, corrección auditada, ER efectivo, sucesor y settlements. |

### 7. README y estado de entrega

Ajustar sólo el recorrido breve para indicar evidencia integrada y herramientas
avanzadas. Agregar un párrafo que ubique paquete Java `com.maxcapital.challenge.demo`
y recursos `src/main/resources/demo` como soporte de evaluación: UI, orquestación,
consultas, controles e instrumentación opcional `demo.recovery`. Evitar describir
todo `demo` como archivos visuales: también ejecuta los ensayos. Las reglas de
negocio siguen en dominio/aplicación/adaptadores.

La mejora pertenece a F-09 y se referencia con su evidencia al verificarla; no
agregar otra feature ni alterar F-08. Este plan no edita README ni declara la
interfaz implementada. No se cambia DECISIONS porque las garantías del núcleo
no cambian. La evidencia futura del change registra validación Maven, smoke y QA.

## Risks / Trade-offs

- Más datos en la respuesta de run -> reutilizar observaciones capturadas y sus
  referencias, sin volver a leer dependencias en GET ni incorporar registros ajenos.
- Evidencia incompleta durante fallas -> conservar parciales y su procedencia;
  la UI muestra falta de observación sin reclasificar el resultado del escenario.
- Contradicción aparente entre NEW y FILLED en un duplicado -> rotular estado del
  mensaje y snapshot del resultado por separado, con prueba focalizada.
- Cancelación y original inválido -> conservar original/efectivo y capturas antes
  de salir; el rediseño no interfiere en la restauración ni agrega mutaciones.
- Tabla extensa en recuperación -> selección por caso/orden y detalle en la misma
  ficha; los ocho casos conservan acceso a sus datos sin mostrar JSON de entrada.
- La UI puede explicar sólo lo observado -> no inferir autoría histórica, ausencia
  definitiva de salidas ni igualdad antes/después desde lecturas actuales.

## Migration Plan

No hay migraciones ni cambios de datos. Implementar captura aditiva en demo,
después ficha/navegación, y luego documentación y verificación. Mantener el mismo
JAR, perfil y puertos. Reconstruir la consola; reconstruir apps sólo si sus fuentes
compartidas empaquetadas cambiaron, conservando bases, topics, offsets y volúmenes.
Revertir la revisión de consola restaura la presentación previa sin transformar
datos. El historial de runs en memoria se pierde al reiniciar la consola y se
informa; no se promete compatibilidad de runs entre procesos.
