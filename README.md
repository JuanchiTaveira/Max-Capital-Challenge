# Max Capital — challenge backend

Servicio de procesamiento de ExecutionReports desarrollado por features con OpenSpec.
El enunciado está en [challenge-arch-max-capital..pdf](challenge-arch-max-capital.pdf),
en la raíz del repositorio.
F-01 aporta infraestructura y herramientas; F-02 procesa ER en secuencia por orden,
persiste estado y ledger y los expone por HTTP desde las dos instancias. F-03
conserva el resultado de cada posición Kafka resuelta para recuperarlo en reentregas.

## Guía rápida para evaluar

Ejecutar los comandos desde la raíz del repositorio, con Docker activo:

1. [Levantar el entorno](#ejecutar): `docker compose up --build`.
2. Abrir [la consola de evaluación](http://localhost:8083). Empieza por **Dos órdenes en paralelo**: leer **Qué demuestra**, **Qué vamos a hacer** y **Qué debería pasar**; elegir **Automático** y pulsar **Ejecutar escenario**.
3. Leer la conclusión y la tabla **Verificación / Esperado / Observado / Resultado**, sin abrir JSON. **Ir al siguiente escenario** sólo lo selecciona; no publica nada. **Ejecutar todos** recorre los cinco y permite consultar cada resultado por separado. Los tres pendientes quedan aparte.
4. En modo **Paso a paso**, leer qué hará **Continuar paso** antes de pulsarlo; recargar conserva la ejecución y el escenario seleccionado por URL mientras la consola siga ejecutándose. **Evidencia técnica** contiene posiciones, snapshots y enlaces para inspeccionar órdenes, ledger y mensajes. En **Mensajes**, el editor permite publicar un NEW y luego un FILLED para la misma key, o republicar exactamente un registro.
5. En **Sistema**, observar salud, consumo, asignaciones y offsets; detener/iniciar o terminar abruptamente una aplicación desde sus controles. Las caídas entre commits, settlement y recuperación de mensajes permanentes continúan pendientes de F-04 a F-06.

El evaluador sólo necesita Docker y un navegador. La verificación portable por API
se ejecuta con `docker compose --profile tools run --build --rm verify-console`:
también prueba caídas de dependencias y restaura los servicios, conservando datos.
Ejecutar los smokes de a uno sobre este entorno de demo.

También se conservan el [smoke F-02](#emitir-er-de-prueba-órdenes-intercaladas-y-duplicados),
la [consulta HTTP](#consultar-una-orden), la [prueba manual F-03](#prueba-manual-de-f-03-en-powershell),
las [consultas SQL](#consultar-las-tablas-con-sql) y la [lectura Kafka por CLI](#ver-los-mensajes-de-kafka).
DBeaver es un cliente externo opcional para inspeccionar tablas fuera de la consola.

## Features

Esta tabla es la única fuente del estado de implementación.

| ID | Feature | Estado |
| --- | --- | --- |
| F-01 | Entorno, OpenSpec, AGENTS.md y Spotless | Implementado |
| F-02 | Procesamiento y consulta de órdenes | Implementado |
| F-03 | Idempotencia y resultados persistidos | Implementado |
| F-04 | Settlement transaccional | Pendiente |
| F-05 | Recuperación ante caídas y rebalanceos | Pendiente |
| F-06 | Cuarentena y recuperación ordenada | Pendiente |
| F-07 | Entrega reproducible y documentación | Pendiente |
| F-08 | Integración continua — opcional | Pendiente |
| F-09 | Consola local de evaluación | Implementado |

## Prerrequisitos y versiones

### Contrato de settlement v1 (F-04 en implementación)

El contrato de salida usa JSON UTF-8 en `settlements`, con key igual al
`numericOrderId` decimal. `schemaVersion` es el entero 1; `settlementId` vale
`settlement:<numericOrderId>`. `numericOrderId`, `fixId`, `ledgerId`, las cantidades
y `source.offset` son cadenas decimales exactas. `source` incluye también topic
y partition (entero). Las cantidades usan representación decimal plana sin ceros
fraccionarios innecesarios; cero se representa como `"0"`.

La salida se reconstruye sólo de un resultado SQL original APPLIED FILLED.
CANCELLED y DUPLICATE no generan salidas, aunque el snapshot del duplicado sea
FILLED. La implementación completa y su evidencia siguen pendientes en la tabla.

Para ejecutar: Docker con contenedores Linux y Docker Compose, acceso a internet en
el primer build y puertos 8081, 8082, 8083, 9092 y 15432 disponibles. Reservar al menos
4 GB de RAM para Docker. Java y Maven del host no son necesarios para Compose.

Para desarrollar y ejecutar Testcontainers: JDK 25, Docker y Maven Wrapper incluido.
El smoke de órdenes F-02 sólo requiere Docker y Compose, igual que el entorno:
no necesita PowerShell 7, Python, Java ni Maven instalados en el host.
El smoke de infraestructura F-01, opcional para desarrollo, requiere PowerShell 7
(`pwsh`). OpenSpec 1.14.1 requiere Node.js.

| Componente | Versión |
| --- | --- |
| Java | 25, sin preview |
| Spring Boot | 4.1.1 |
| Maven Wrapper / Maven | 3.3.4 / 3.9.11 |
| Kafka | 4.2.1 |
| PostgreSQL | 17, imagen bookworm fijada por digest |
| JUnit Jupiter / Testcontainers | 6.0.3 / 2.0.5, BOM de Spring Boot |
| Spotless / google-java-format | 3.10.4 / 1.36.1 |
| OpenSpec | 1.14.1 |

## Ejecutar

```sh
docker compose up --build
```

También funciona `docker compose up`: construye la imagen si aún no existe. Después
de cambiar fuentes, usar `--build` para reconstruirla.

| Servicio | Dirección en el host |
| --- | --- |
| app-1 | http://localhost:8081 |
| app-2 | http://localhost:8082 |
| Consola de evaluación | http://localhost:8083 |
| Kafka | localhost:9092 |
| PostgreSQL | localhost:15432 |

Ambas aplicaciones exponen `/actuator/health/liveness`,
`/actuator/health/readiness` y `/actuator/info`. Readiness depende de PostgreSQL y
Kafka; liveness sólo representa la aplicación. Info distingue `app-1` y `app-2`.

```sh
curl http://localhost:8081/actuator/health/readiness
curl http://localhost:8082/actuator/info
```

La base, usuario y contraseña de demostración son `orders`. Son valores locales
de este ejercicio. Se pueden cambiar con `POSTGRES_DB`, `POSTGRES_USER` y
`POSTGRES_PASSWORD`. Las aplicaciones locales aceptan `DB_URL`, `DB_USER`,
`DB_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`, `INSTANCE_ID` y `SERVER_PORT`.

## Consola de evaluación

El paquete Java `demo` contiene las herramientas y escenarios para evaluar la
aplicación. Los recursos de la interfaz web están en `src/main/resources/demo`.
Para acceder, ejecutar `docker compose up --build` y abrir
[http://localhost:8083](http://localhost:8083). La consola ofrece cuatro vistas:
escenarios, mensajes, órdenes y sistema.
Su proceso usa el perfil `evaluation-console`, no participa del grupo de órdenes
ni ejecuta migraciones, y mantiene su salud propia aunque SQL, Kafka o una
aplicación no respondan. Las aplicaciones exponen observación de sus listeners
sólo con `APP_DEMO_OBSERVATION_ENABLED=true`, configurado en Compose.

Sólo el contenedor de consola incorpora la CLI Docker y monta el socket del motor
Linux, igual que el smoke existente. No requiere Java, Maven, Node ni clientes SQL
en el host. El socket se configura mediante `SMOKE_DOCKER_SOCKET` si es necesario.
El resto de las aplicaciones conserva sus permisos e interfaces habituales.

Las consultas de consola distinguen el estado actual de la orden del snapshot
histórico de cada posición `(topic, partition, offset)`. `Sin resultado registrado`
no significa necesariamente error: puede ser un registro todavía no intentado.
El contador cuenta ER aplicados, incluidos NEW y terminales; no títulos operados.
El ledger completo se ordena por `ledgerId` y se obtiene junto al estado en el
mismo snapshot SQL. Las lecturas de SQL, Kafka y HTTP pueden observar momentos
distintos y muestran su fuente y fecha; un dato anterior tras un error es stale.

La API propia `/demo/api/orders` lista órdenes con límite y cursor;
`/demo/api/orders/{id}` expone el detalle y `/demo/api/results` consulta la tupla
Kafka completa. Sus IDs y cantidades son cadenas para evitar redondeos del
navegador. El contrato numérico público de `/orders/{id}` no cambia. Un duplicado
tardío del NEW puede referir al ledger de NEW y conservar snapshot FILLED, mientras
el resultado de la posición original mantiene snapshot NEW.

La publicación manual valida JSON, key e incompatibilidades conocidas de identidad
y transición antes de enviar. Es un preflight sobre un snapshot: otro productor
puede cambiar la orden después, y la validación definitiva sigue en el consumidor.
Una posición confirmada por Kafka no implica que SQL haya aplicado el ER; ambas
etapas se muestran separadas. Ante confirmación incierta no se reenvía automáticamente.
Republicar conserva key y payload y crea otro offset; no fuerza reentrega de la
misma posición ni resetea offsets. Los mensajes se leen como `read_committed` por
partición, sin participar del grupo de órdenes; puede haber huecos legítimos por
transacciones abortadas. Los topics de salida vacíos no prueban garantías pendientes.

Los controles locales permiten start/stop/restart de app-1, app-2, PostgreSQL y
Kafka, y kill abrupto sólo de las aplicaciones. Resuelven exclusivamente un
contenedor del proyecto Compose, incluyendo los detenidos; rechazan destinos
ambiguos o ajenos. No admiten comandos libres, borrados de volúmenes/topics ni
resets de offsets. Las mutaciones requieren JSON y origen local de la consola.
El socket otorga acceso al motor y debe usarse sólo en esta demo local.

Detener PostgreSQL o Kafka puede detener los consumidores: iniciar primero la
dependencia y luego reiniciar las aplicaciones desde la consola. Si falta acceso
al socket, los controles y logs lo informan, y las lecturas independientes siguen
disponibles. La salud HTTP y el estado del consumidor son observaciones distintas;
las asignaciones actuales no identifican quién procesó un resultado histórico.
El lag cuenta posiciones Kafka, que también pueden incluir registros de control
transaccional: no equivale necesariamente a cantidad de ER pendientes.

El catálogo ofrece órdenes intercaladas, snapshots con reloj no creciente,
duplicados terminales/tardíos, persistencia después de reiniciar y visibilidad de
transacciones confirmadas/abortadas. Cada escenario informa precondiciones,
resultado esperado y verificaciones reales; **Ejecutar todos** recorre sólo
esos cinco y deja explícitos los pendientes de F-04/F-05/F-06. La consola no emite
poison intencional ni certifica settlement o caídas entre commits.

Sólo se admite una ejecución activa. El modo paso a paso permite inspeccionar
cada etapa y continuar, incluso tras recargar la página por su URL. Los pasos de
recuperación tienen plazo de 150 segundos; una pausa manual vence a los 30 minutos.
Cancelar conserva efectos confirmados y evita los próximos envíos; se intentan
restaurar los servicios que la propia ejecución alteró. Se generan órdenes nuevas
y se conserva evidencia SQL/Kafka de cada repetición, sin borrar datos previos.
El historial de hasta 50 ejecuciones vive en memoria: reiniciar la consola pierde
esa coordinación, aunque las órdenes y posiciones sigan disponibles para inspección.
Un poison previo o consumidores detenidos pueden impedir cumplir precondiciones;
se informa el diagnóstico sin saltar registros ni resetear el grupo.

La vista guiada comienza individualmente y separa cada resultado del lote. Las
comparaciones provienen de comprobaciones del servidor y conservan sus observaciones
capturadas; abrir la orden actual no reemplaza un snapshot histórico. Las filas sin
observación, incompletas o fallidas quedan distinguidas de las verificadas. Un aborto
transaccional previsto se explica como parte de la prueba. Las rutas de consola
mantienen sus campos anteriores y agregan guía y resultados agrupados, sin migraciones.

## Inspeccionar la base de datos y Kafka

### Conectar PostgreSQL desde DBeaver

Con el entorno levantado, crear una conexión de tipo **PostgreSQL** en DBeaver:

| Campo | Valor de demostración |
| --- | --- |
| Host | `localhost` |
| Puerto | `15432` |
| Base de datos | `orders` |
| Usuario | `orders` |
| Contraseña | `orders` |

Seleccionar **Probar conexión** y aceptar la descarga del driver PostgreSQL si
DBeaver la solicita. Guardar la conexión y expandir **orders → Esquemas → app →
Tablas**. Para abrir los registros: clic derecho en la tabla → **Ver datos →
Todas las filas**. Si las tablas no aparecen tras actualizar la aplicación,
refrescar el esquema `app`.

| Tabla | Qué permite inspeccionar |
| --- | --- |
| `app.orders` | Estado y cantidades actuales; `applied_execution_count` cuenta ER aplicados, incluidos NEW y terminales, sin contar duplicados. No es la cantidad de títulos operados. |
| `app.execution_report_ledger` | Historial de ER efectivamente aplicados, con reporte completo y `ledger_id` autoincremental. Un duplicado no agrega entradas. |
| `app.processing_results` | Resultado `APPLIED`/`DUPLICATE` y snapshot por posición Kafka. Una republicación puede agregar un resultado sin agregar ledger. |
| `app.flyway_schema_history` | Migraciones de base de datos ejecutadas. |

Los valores de conexión corresponden a Compose desde el host. Si se modificaron
`POSTGRES_DB`, `POSTGRES_USER` o `POSTGRES_PASSWORD`, usar los valores configurados.
Una conexión rechazada puede indicar que PostgreSQL no está levantado: comprobar
`docker compose ps` y, si hace falta, ejecutar `docker compose up -d postgres`.
Las tablas de aplicación se crean al arrancar una instancia de la aplicación y
ejecutar sus migraciones; levantar sólo PostgreSQL en un entorno nuevo no las crea.

### Consultar las tablas con SQL

En el editor SQL de DBeaver, obtener primero IDs que existan en esa base:

```sql
SELECT numeric_order_id, status, applied_execution_count,
       nominal_amounts, accumulative_nominal_amount, leaves_nominal_amount
FROM app.orders
ORDER BY numeric_order_id DESC
LIMIT 20;
```

También se pueden usar los IDs impresos por el smoke. En las consultas siguientes,
reemplazar `13144742` por uno de esos IDs; el valor del ejemplo no se precarga:

```sql
-- Estado actual de la orden.
SELECT * FROM app.orders WHERE numeric_order_id = 13144742;

-- ER aplicados, en orden de inserción.
SELECT ledger_id, fix_id, report
FROM app.execution_report_ledger
WHERE numeric_order_id = 13144742
ORDER BY ledger_id;

-- Resultado de cada registro Kafka y snapshot al resolverlo.
SELECT topic, partition_id, record_offset, outcome, fix_id, ledger_id,
       status, applied_execution_count
FROM app.processing_results
WHERE numeric_order_id = 13144742
ORDER BY topic, partition_id, record_offset;
```

La orden muestra el presente; el ledger explica cómo llegó a ese estado; los
resultados conservan qué ocurrió con cada posición del flujo. Un resultado
histórico puede tener otro snapshot que el estado actual. Los registros aplicados
antes de F-03 pueden tener ledger sin resultados por posición.

Sin DBeaver, las mismas consultas se pueden ejecutar con el cliente del contenedor:

```sh
docker compose exec postgres psql -U orders -d orders
```

Dentro de `psql`, `\dt app.*` lista las tablas y `\q` sale del cliente.

### Ver los mensajes de Kafka

Listar los topics desde la raíz del repositorio:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
```

Leer los ER conservados desde el comienzo y seguir viendo nuevos mensajes:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic execution-reports --from-beginning --isolation-level read_committed --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
```

La salida muestra **key (`numericOrderId`), partición, offset y JSON del ER**.
`Ctrl+C` detiene la visualización. Este comando usa un consumidor independiente:
no elimina los mensajes ni cambia los offsets del grupo de las aplicaciones.
No agregar `--group max-capital-orders-v1`, porque ese grupo corresponde al
procesamiento de órdenes. `read_committed` excluye transacciones productoras
abortadas.

Para una lectura acotada, agregar `--max-messages 10 --timeout-ms 10000` al
comando: termina al leer diez mensajes o al agotar diez segundos sin recibir
mensajes; el timeout puede terminar con código distinto de cero. Las
republicaciones aparecen como registros distintos, aunque la aplicación las haya
clasificado como `DUPLICATE`.

La dirección `localhost:29092` se usa **dentro del contenedor Kafka**. Para conectar
una herramienta desde el host, usar `localhost:9092`. La vista **Mensajes** de la
consola permite inspeccionar el broker; estos comandos son una alternativa.
El estado actual de cada orden también se consulta por HTTP o en `app.orders`.

## Formato y pruebas

### Contrato de ExecutionReport

El productor debe publicar JSON UTF-8 con key igual a `numericOrderId` en decimal
canónico. Campos obligatorios: `fixId` y `numericOrderId` enteros positivos de
64 bits, `status`, `nominalAmounts`, `leavesNominalAmount`,
`accumulativeNominalAmount` y `transactionTime` ISO-8601 local o con offset.
Las cantidades admiten 12 dígitos enteros y 8 decimales sin redondear.
[Ejemplo del formato del enunciado](examples/execution-report.json). Los campos
adicionales se conservan en el ledger. El ejemplo es un parcial: debe publicarse
después del NEW correspondiente.

La orden empieza con NEW (acumulado cero y restante total); los parciales aumentan
el acumulado y reducen el restante. FILLED alcanza el total con restante cero;
CANCELLED termina con restante cero y acumulado no decreciente. Las cantidades
son snapshots: no se suman al acumulado anterior. El contador incluye cada ER
aplicado, también NEW y terminal. `transactionTime` no establece secuencia.

### Procesamiento y errores de F-02

Cada instancia usa un consumidor del grupo `max-capital-orders-v1`. Dos particiones
permiten que ambas consuman órdenes distintas; cada partición se procesa en serie.
El productor debe mantener la misma key para todos los ER de una orden y usar
el particionado estándar por key. No cambiar las particiones con órdenes en curso.

Estado, contador y ledger se escriben en una transacción SQL. La identidad es
`(numericOrderId, fixId)`: un ER idéntico ya aplicado es un no-op, incluso después
de FILLED/CANCELLED; la misma identidad con otro contenido es error. El offset se
confirma después del commit SQL. Una reentrega entre esas confirmaciones no agrega
otra fila ni incrementa el contador.

Un error de JSON, key, transición, conflicto o SQL deja un diagnóstico `ER pendiente`
con topic, partición, offset y key, y detiene el contenedor consumidor de esa
instancia sin confirmar el ER fallido ni saltar al siguiente. Si esa instancia
tenía ambas particiones, ambas se detienen. Tras resolver una falla transitoria,
reiniciar la aplicación retoma desde el offset pendiente. Un ER inválido vuelve
a fallar al reiniciar: F-06 incorporará cuarentena y recuperación ordenada.
F-03 incorpora resultados por posición; F-05 cubrirá más fronteras de caída y
rebalanceo. Todavía no se publican settlements.

### Resultados persistidos de F-03

`app.processing_results` conserva un resultado por `(topic, partition_id,
record_offset)`. `APPLIED` significa que ese registro aplicó el ER; `DUPLICATE`
significa que el ER ya existía con contenido equivalente. Cada fila guarda su
identidad, referencia al ledger y snapshot de estado, cantidades y contador.
Resultado, estado, contador y ledger se confirman en la misma transacción SQL.

Reentregar la **misma posición** devuelve el resultado original, incluso si fue
`APPLIED` y la orden avanzó después. Republicar el mismo JSON crea **otro offset**
y un resultado `DUPLICATE`; no incrementa contador ni ledger. La comparación
JSONB incluye los campos adicionales. Un mismo identificador con otro contenido
sigue siendo un error. Los errores no crean resultados exitosos: ausencia de
fila no distingue entre registro no intentado y fallo.

Para actualizar desde F-02, detener ambas aplicaciones y después reconstruirlas:

```sh
docker compose stop app-1 app-2
docker compose up -d --build --wait app-1 app-2
```

Flyway aplica V2 sin borrar datos ni editar V1. Los ER históricos no tienen
posición registrada, por lo que no se hace backfill ni se resetean offsets. Si
uno aplicado en F-02 se reentrega sin resultado conocido, se registra `DUPLICATE`
por su identidad de negocio. No mezclar consumidores F-02/F-03 durante el despliegue.

### Prueba manual de F-03 en PowerShell

Con el entorno actualizado, generar un ID nuevo y publicar `NEW`, `FILLED` y dos
republicaciones. Estos comandos usan el productor Kafka y psql de los contenedores;
no requieren Java ni herramientas de base instaladas en el host.

```powershell
$manualOrder = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$newEr = [ordered]@{
  fixId = 1; numericOrderId = $manualOrder; status = 'NEW'
  nominalAmounts = 10; accumulativeNominalAmount = 0; leavesNominalAmount = 10
  transactionTime = '2026-07-20T18:08:52.129'; ticker = 'VSCPC'
} | ConvertTo-Json -Compress
$filledEr = [ordered]@{
  fixId = 2; numericOrderId = $manualOrder; status = 'FILLED'
  nominalAmounts = 10; accumulativeNominalAmount = 10; leavesNominalAmount = 0
  transactionTime = '2026-07-20T18:08:53.129'; ticker = 'VSCPC'
} | ConvertTo-Json -Compress
@("$manualOrder|$newEr", "$manualOrder|$filledEr",
  "$manualOrder|$filledEr", "$manualOrder|$newEr") |
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server localhost:29092 --topic execution-reports `
    --property parse.key=true --property 'key.separator=|'
$manualOrder
```

El consumo es asíncrono. Repetir las consultas hasta observar las cuatro filas:

```powershell
Invoke-RestMethod "http://localhost:8081/orders/$manualOrder" | ConvertTo-Json -Depth 8
Invoke-RestMethod "http://localhost:8082/orders/$manualOrder" | ConvertTo-Json -Depth 8
docker compose exec -T postgres psql -U orders -d orders -c "SELECT topic, partition_id, record_offset, outcome, fix_id, ledger_id, status, applied_execution_count FROM app.processing_results WHERE numeric_order_id=$manualOrder ORDER BY topic, partition_id, record_offset;"
```

Ambas consultas HTTP deben mostrar `FILLED`, contador **2** y **2** entradas de
ledger. En SQL se esperan estas filas, en offsets distintos de una misma partición:

| ER enviado | Resultado | Snapshot | Contador | Referencia |
| --- | --- | --- | --- | --- |
| NEW | APPLIED | NEW | 1 | Ledger de NEW |
| FILLED | APPLIED | FILLED | 2 | Ledger de FILLED |
| FILLED repetido | DUPLICATE | FILLED | 2 | Mismo ledger de FILLED |
| NEW repetido tras terminar | DUPLICATE | FILLED | 2 | Mismo ledger de NEW |

Para comprobar persistencia, reiniciar y republicar el terminal:

```powershell
docker compose restart app-1 app-2
"$manualOrder|$filledEr" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 --topic execution-reports --property parse.key=true --property 'key.separator=|'
```

Al reanudar, SQL debe tener una quinta fila `DUPLICATE`; HTTP conserva contador
2 y dos entradas de ledger. Los datos quedan disponibles para inspección. Si se
cambiaron las credenciales de Compose, ajustar `-U` y `-d` en las consultas.

Esta prueba manual republica ER y comprueba persistencia; no fuerza una reentrega
del mismo offset. Para verificar esa frontera sin modificar offsets del entorno,
ejecutar las pruebas aisladas con Docker y JDK 25:

```powershell
.\mvnw.cmd verify '-Dit.test=ProcessingResultsIT,ProcessingResultsSchemaIT,ExecutionReportKafkaIT'
```

`recoversCommittedResultWhenOffsetCommitFailsBeforeReachingKafka` falla una
confirmación antes de enviarla al broker, después de confirmar SQL, y comprueba
reentrega, resultado inmutable y sucesor pendiente hasta recuperar. Usa inyección
de fallo exclusiva de test; no demuestra kill de proceso ni rebalanceo durante
escritura (F-05). La suite también verifica rollback y concurrencia SQL.


### Consultar una orden

`GET /orders/{numericOrderId}` devuelve estado y ledger juntos desde la base
compartida. HTTP 404 significa orden inexistente; 400, identificador inválido.
Las dos instancias exponen la misma consulta:

```sh
curl http://localhost:8081/orders/13144742
curl http://localhost:8082/orders/13144742
```

Ejemplo después de aplicar sólo el NEW (los `ledgerId` son globales y pueden
tener huecos). `report` conserva todos los campos del ER recibido:

```json
{
  "numericOrderId": 13144742,
  "status": "NEW",
  "appliedExecutionCount": 1,
  "nominalAmounts": 10,
  "accumulativeNominalAmount": 0,
  "leavesNominalAmount": 10,
  "ledger": [{
    "ledgerId": 1,
    "report": {
      "fixId": 1, "numericOrderId": 13144742, "status": "NEW",
      "nominalAmounts": 10, "accumulativeNominalAmount": 0,
      "leavesNominalAmount": 10, "transactionTime": "2026-07-20T18:08:52.129"
    }
  }]
}
```

La consulta SQL única lee estado y ledger desde el mismo snapshot confirmado;
el array se ordena por `ledgerId`, no por `transactionTime`.

### Emitir ER de prueba: órdenes intercaladas y duplicados

Desde la raíz del repositorio, el mismo comando funciona en Windows (PowerShell
incluido o CMD), Linux y macOS, con Docker activo en modo contenedores Linux:

```sh
docker compose --profile tools run --build --rm verify-f02
```

El comando construye y levanta el entorno si hace falta, espera ambas aplicaciones
saludables y ejecuta la verificación dentro de un contenedor temporal. Publica dos
órdenes intercaladas en distintas particiones: una termina FILLED con cuatro ER
aplicados y otra CANCELLED con tres. Repite ambos terminales y comprueba que los
duplicados no aumenten el contador ni el ledger. Consulta ambas instancias,
reinicia las dos aplicaciones, reenvía los terminales y verifica que el estado y el
ledger completos sigan iguales. Espera offsets confirmados con lag cero.

Al terminar muestra `F-02 OK`, los IDs generados y las URLs de consulta en 8081/8082.
Un error termina con código distinto de cero y diagnóstico; no hace falta interpretar
los logs para decidir si pasó. El entorno queda levantado y conserva volúmenes y
órdenes de prueba con IDs aislados para inspección. Para repetir sin reconstruir:

```sh
docker compose --profile tools run --rm verify-f02
```

El perfil `tools` sólo se activa al pedirlo. El contenedor usa el socket Docker
para ejecutar el productor Kafka y reiniciar las aplicaciones identificadas por
las etiquetas del proyecto Compose. Ese acceso permite controlar el motor: es una
herramienta local de demostración. No monta el repositorio ni publica puertos.
En Docker rootless, configurar `SMOKE_DOCKER_SOCKET` con la ruta del socket del
motor; se usa `/var/run/docker.sock` por defecto. Este comando requiere un socket
Linux accesible desde el motor y no configura acceso a un Docker remoto.

También se conserva el acceso opcional desde Windows PowerShell 5.1 o PowerShell 7,
sin invocar `pwsh`:

```powershell
powershell -NoProfile -File scripts/verify-f02.ps1
powershell -NoProfile -File scripts/verify-f02.ps1 -SkipBuild
```

**Límite actual:** el smoke reinicia ambas instancias después de consumir el lote.
No verifica una caída de una sola instancia durante el procesamiento ni las fronteras
de commit SQL/Kafka; esos escenarios siguen pendientes en F-05. La portabilidad de
este smoke tampoco completa por sí sola la entrega reproducible de F-07.

### Comandos de verificación

En PowerShell:

```powershell
.\mvnw.cmd spotless:apply
.\mvnw.cmd spotless:check
.\mvnw.cmd verify
pwsh -File scripts/verify-f01.ps1
docker compose --profile tools run --build --rm verify-f02
```

En Linux/macOS, reemplazar `.\mvnw.cmd` por `./mvnw`. El smoke F-02 usa el mismo
comando Compose en las tres plataformas. F-01 usa PowerShell 7 también en Linux/macOS.
La suite Maven usa JUnit y Testcontainers;
falla si Docker no está disponible. No hay Maven global requerido.

Spotless aplica Google Style a Java: dos espacios, imports ordenados y sin uso
eliminados, UTF-8 y LF. `validate` comprueba formato sin editar. `apply` corrige
fuentes. El build de imagen omite tests, pero mantiene ese check; no reemplaza
`verify` ni el smoke.

Los smokes detienen o recrean contenedores del proyecto. Ejecutarlos sobre el entorno
local de demostración, de a uno. F-01 limpia sus marcadores aislados y conserva los
volúmenes; F-02 conserva también sus órdenes. `-SkipBuild` permite usar imágenes
ya construidas en los lanzadores PowerShell.

## Persistencia y diagnóstico

`docker compose stop` / `docker compose start` detienen y reanudan. `docker compose
down` elimina contenedores y conserva volúmenes. **`docker compose down -v` elimina
los datos**: usarlo sólo para reiniciar expresamente una demo descartable.

```sh
docker compose ps -a
docker compose logs app-1 app-2
docker compose logs kafka postgres kafka-init
docker info
```

Si Docker no responde, iniciar Docker Desktop y su motor Linux. Si un puerto está
ocupado, detener el proceso que lo usa o ajustar el mapeo de Compose. El primer
arranque descarga Maven, dependencias e imágenes. `kafka-init` debe terminar con
código 0; se puede repetir sin recrear topics existentes.

## Desarrollo y decisiones

[AGENTS.md](AGENTS.md) guía el trabajo por feature.
[DECISIONS.md](DECISIONS.md) justifica el diseño, sus supuestos y los límites de las garantías.
El change OpenSpec de cada feature conserva requisitos, diseño, tareas y evidencia.
Para validar: `openspec validate --all --strict`.

[Evidencia de F-01](openspec/changes/archive/2026-10-17-f01-runtime-and-specs/verification.md):
tres pruebas de integración, smoke completo de dos instancias, recuperación de
dependencias, persistencia y build desde un clon limpio. Las tres specs están
sincronizadas y el change está archivado.

[Evidencia de F-02](openspec/changes/archive/2026-10-18-f02-order-processing-and-query/verification.md):
cinco pruebas unitarias y once de integración (incluidas las tres de F-01), más
el smoke de dos instancias con secuencia, duplicados y persistencia tras reinicio.

[Evidencia del smoke F-02 portable](openspec/changes/archive/2026-10-19-portable-f02-smoke/verification.md):
comando Compose y lanzador Windows PowerShell 5.1 verificados con Docker Desktop
en Windows; errores de acceso al motor y selección de proyecto comprobados.

Kafka usa un solo broker: el entorno verifica caídas de aplicaciones y reinicios,
no supervivencia a pérdida física del volumen. La presencia de transacciones Kafka
no demuestra por sí sola settlement exactamente una vez. Esa garantía queda
pendiente de F-04/F-05.

[Evidencia de F-03](openspec/changes/archive/2026-10-19-f03-persisted-processing-results/verification.md):
suite completa con 5 unitarias y 21 integraciones, resultados inmutables,
concurrencia y rollback SQL, falla controlada de confirmación Kafka posterior al
commit SQL, migración V1→V2 conservando datos, smoke de dos instancias y prueba
manual Windows PowerShell. Las caídas reales/rebalanceos durante escritura y el
settlement siguen pendientes en F-04/F-05.
