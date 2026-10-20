# Design

## Context

La motivación y el alcance están en [proposal.md](proposal.md). Este documento
describe una propuesta; todavía no hay consola implementada ni evidencia nueva
de ejecución del núcleo.

El repositorio tiene un único artefacto Java 25/Spring Boot, dos servicios de
aplicación, Kafka con dos particiones por topic y PostgreSQL compartido. El HTTP
actual sólo consulta una orden; `JdbcOrderQueries` obtiene estado y ledger con un
SELECT consistente. V2 conserva resultados exitosos por posición, sin errores
persistidos ni autoría histórica por instancia. Readiness verifica dependencias,
pero no el estado del listener. Un error detiene el consumidor de esa instancia.

`scripts/verify-f02.py` ya resuelve contenedores por etiquetas Compose, publica
órdenes intercaladas y verifica persistencia tras reinicio. Su contenedor utiliza
Docker CLI fijada y el socket local. Se toma como referencia, sin sustituir ese
smoke ni atribuirle pruebas de caída durante escritura.

## Goals / Non-Goals

**Goals:**

- Mantener operativa la consola al detener las aplicaciones o sus dependencias.
- Reutilizar Java, JDBC, cliente Kafka y consultas existentes sin agregar una
  cadena de build de frontend ni un tercer consumidor del grupo de órdenes.
- Hacer trazable cada afirmación de la UI a un registro, snapshot, endpoint o
  diagnóstico real, incluyendo respuestas parciales y errores.
- Conservar interfaces públicas, datos históricos y secuencia del núcleo.

**Non-Goals:**

- Resolver settlement, commit incierto, caídas deterministas, cuarentena o replay
  ordenado de mensajes permanentes: corresponden a F-04/F-05/F-06.
- Permitir comandos o SQL arbitrarios, resetear el grupo, borrar datos o administrar
  un motor remoto. Tampoco se incorporan autenticación o hardening de producción.
- Publicar poison intencional en el flujo compartido. Su aislamiento pertenece al
  escenario pendiente de F-06; IDs diferentes no aíslan el bloqueo de una partición.

## Decisions

### 1. Perfil Spring exclusivo en un proceso independiente

El servicio `evaluation-console` usará el mismo JAR bajo el perfil
`evaluation-console` y expondrá `127.0.0.1:8083:8080`. Los adaptadores de consola
vivirán en un paquete propio con activación explícita por perfil; sus recursos se
servirán mediante un mapeo exclusivo, sin habilitar la UI en app-1/app-2.

En ese perfil se excluye el listener de procesamiento, se desactiva Flyway y se
evita que una conexión inicial fallida de Hikari impida arrancar. Las consultas
reportarán esquema todavía no disponible o dependencia caída. La salud propia de
la consola dependerá sólo de su proceso, no de SQL/Kafka ni de las aplicaciones.
Las migraciones siguen a cargo de las dos aplicaciones existentes.

Compose construirá targets explícitos `application` y `evaluation-console`; sólo
el segundo agregará la Docker CLI con la versión y digest ya fijados en
`scripts/Dockerfile.smoke` y acceso al socket. El montaje local seguirá el patrón
configurable del smoke. El usuario/permisos de esa imagen permitirán usarlo en el
motor Linux de Docker Desktop; se documentará que el socket otorga acceso al motor
y que la lista de acciones de la API no constituye aislamiento del socket.

La consola no dependerá del healthcheck de las aplicaciones para iniciar. Podrá
mostrar el arranque y operar un contenedor detenido. Las imágenes de aplicación
conservarán su usuario y carecerán de Docker CLI/socket.

Alternativas: servir todo desde app-1 haría desaparecer los controles al apagarla;
un servidor Python agregaría dependencias para leer SQL y Kafka que ya existen en
Java; un módulo Maven adicional obligaría a reorganizar el build para una demo.

### 2. Datos reales y representación exacta

Los adaptadores de consulta de consola serán de lectura. El detalle reutilizará
el contrato de `OrderQueries` y su SELECT; un listado nuevo acotado leerá `orders`
y otro adaptador consultará `processing_results` por la PK completa. Se reutilizan
dominio y parser para comprobaciones de entrada, sin invocar el caso de uso de
procesamiento desde HTTP ni escribir tablas de negocio.

Los DTO propios de `/demo/api` transportarán IDs, offsets, contadores y cantidades
como cadenas decimales. Payloads y reportes completos viajarán como texto JSON;
el backend podrá producir una vista formateada con lectura decimal exacta. El
navegador no hará un ciclo `JSON.parse`/`JSON.stringify` sobre números de ER para
editar o republicar. El endpoint público `/orders/{numericOrderId}` mantiene su
contrato numérico actual.

Estado actual y snapshot histórico se etiquetan por separado. La falta de resultado
se presenta como `Sin resultado registrado`. Cada lectura lleva `observedAt` y
fuente; no se pretende un snapshot atómico entre SQL, Kafka y salud HTTP. El
ledger mantiene orden por `ledgerId`; la vista Kafka mantiene orden por partición,
sin inventar una secuencia total entre particiones ni usar `transactionTime`.

Alternativa: representar todo como `Number` en JavaScript corrompería valores
válidos del contrato de 64 bits y decimales de ocho posiciones.

### 3. Lectura Kafka que no interviene en el procesamiento

La consola usará consumidores propios de lectura `read_committed`, con
`assign`/`seek`, auto-commit desactivado y sin `subscribe` al grupo de aplicación.
Cada consulta selecciona un topic de la lista declarada y una partición. El
cursor es la próxima posición realmente alcanzada por la lectura, no el número
de filas visibles: puede haber huecos por transacciones abortadas o registros de
control. Las lecturas tendrán plazo y lote acotados, inicialmente hasta 200
registros y tres segundos por petición, y devolverán continuación y rango leído.

El formulario de publicación recibe `{key, payload}` como cadenas, valida el
contrato y hace un preflight de identidad/transición contra un snapshot SQL:
primero compara duplicados con semántica JSONB, después evalúa la transición.
Rechaza errores conocidos sin emitirlos. Este preflight orienta al evaluador y
no reserva el estado ni sustituye la validación transaccional del consumidor:
un productor externo puede cambiar el estado entre consulta y publicación.

La publicación real usa el cliente Kafka, particionado estándar por key,
idempotencia del productor y `acks=all`; sólo confirma posición al recibir metadata.
Una incertidumbre se muestra y no provoca un reenvío de aplicación automático.
Republicar obtiene el registro exacto por posición, comprueba que sea el solicitado
y envía sus bytes/key originales a `execution-reports`. Una posición abortada,
inexistente o sin ER válido no puede usarse para publicar otro registro por error.

La lectura de settlements y rechazos funciona desde el inicio, pero sus vistas
vacías no certifican publicación única ni manejo completo de errores.

Alternativas: usar el grupo de órdenes para observar alteraría sus asignaciones;
reescribir una orden en SQL desde un botón eludiría las garantías evaluadas.

### 4. Observación explícita y controles Docker acotados

Las aplicaciones tendrán `GET /internal/demo/consumer`, habilitado sólo mediante
propiedad de demo en Compose, con instanceId, clientId, estado del listener,
asignaciones y fecha. Se configura clientId distinguible por instancia para
correlacionar el registro local con la vista Admin de Kafka. La consola combina
esa evidencia con health/info HTTP y offsets del grupo obtenidos por Admin.
Una instancia inaccesible da estado de consumo desconocido, no detenido.

Un adaptador Docker separado resuelve contenedores mediante
`com.docker.compose.project` y `com.docker.compose.service`, usando inventario que
incluya detenidos. Exige exactamente un destino por servicio y verifica etiquetas.
El proyecto proviene de Compose; el navegador no puede elegirlo.

Acciones disponibles: start/stop/restart para app-1, app-2, postgres y kafka;
kill con SIGKILL sólo para app-1/app-2. La CLI se invoca con vectores de argumentos,
sin shell ni argumentos libres del usuario, con timeout y salida acotada. Se
rechazan ambigüedades, servicios ajenos y solicitudes destructivas. Los logs se
leen por servicio con rango acotado; `ER pendiente` se muestra como diagnóstico
de logs, sin convertirlo en un registro durable de fallo.

Las mutaciones exigen JSON, un encabezado propio de consola y `Origin` igual al
origen local configurado, sin CORS abierto. El binding a loopback limita la demo
al host; no se presenta este mecanismo como autenticación. Una falla del socket
degrada controles y logs sin derribar las lecturas HTTP/JDBC/Kafka disponibles.

Alternativas: inferir consumo desde readiness oculta el listener detenido;
recibir comandos Docker desde el cliente excedería el control necesario.

### 5. Catálogo cerrado y ejecución secuencial observable

Un coordinador de demo ejecutará un único run activo, con ID, modo, pasos,
publicaciones confirmadas, snapshots y aserciones. La API asíncrona responde con
el ID; el navegador consulta avance y puede recuperar la ejecución tras recargar.
La UI recuerda el ID en su URL y el sistema informa el run activo. Las mutaciones
manuales u otro run reciben 409 mientras haya uno activo; los controles internos
del run pasan por la misma lista de servicios autorizados.

Un run puede ejecutar un escenario o el lote `available`, que recorre el catálogo
ejecutable en serie bajo la misma exclusión. El lote informa por separado aprobados,
fallidos y escenarios pendientes excluidos; recargar el navegador no interrumpe
su coordinación en el servidor.

El registro de runs es acotado y en memoria: reiniciar la consola pierde ese
registro de coordinación, no datos SQL/Kafka. La UI lo informa; no se promete
recuperación durable del runner. Se conservan hasta 50 ejecuciones terminadas,
además de la activa. Los IDs de órdenes se generan positivos de 64 bits y se
comprueba que sean nuevos en SQL y entre los IDs del run.

Cada paso emite sus ER en orden y espera condiciones concretas con deadline,
inicialmente 150 segundos por paso de recuperación. Espera el progreso de las
posiciones del propio run, no exige lag global cero sobre datos ajenos. Ante error
detiene el escenario y guarda observaciones; cancelar evita nuevas publicaciones
y permite completar sólo restauraciones de servicios alterados por el propio run.
No se resetean offsets, se cambian particiones ni se eliminan volúmenes o filas.

| ID | Secuencia y verificaciones propuestas |
| --- | --- |
| `interleaved-orders` | Dos órdenes elegidas para particiones distintas; NEW/parciales/terminales intercalados; FILLED con cuatro ER y CANCELLED con tres. Verificar metadata real, ledger, resultados y consultas de ambas instancias, además de asignaciones. |
| `quantity-snapshots` | Total 10; acumulados 0, 3, 7 y 10 con reloj no creciente. Pausas/aserciones por paso para observar reemplazo de snapshots, contador y orden del ledger. |
| `terminal-duplicates` | NEW, FILLED, FILLED repetido y NEW repetido. Dos APPLIED y dos DUPLICATE; dos filas de ledger; snapshot NEW original y FILLED del duplicado tardío. |
| `restart-persistence` | Lote válido consumido; capturar respuestas; reiniciar ambas instancias; esperar consumidores; republicar terminales y comparar estado/ledger completos y resultados nuevos. |
| `committed-input-only` | Confirmar NEW, abortar parcial y confirmar FILLED. Productor transaccional de demo con ID propio y timeout acotado; abort/close en limpieza. Leer hasta el sucesor confirmado de la misma partición y verificar sólo dos efectos SQL. |

Cada escenario nace con precondiciones que deben observarse, incluyendo esquema,
dependencias y consumidores requeridos. IDs nuevos no reparan un poison previo:
si existe una partición bloqueada el runner informa esa limitación, sin saltarla.
Los resultados de comparaciones HTTP se leen y comparan en el backend con números
exactos. La igualdad HTTP de ambas instancias no prueba por sí sola que ambas
consumieron; se conserva evidencia separada de asignaciones.

El catálogo también incluye tarjetas deshabilitadas para settlement único,
fronteras de caída/rebalanceo y mensajes permanentes/cuarentena. Identifican F-04,
F-05 o F-06 y su resultado esperado, sin código simulado. Son disponibilidad de
escenarios, no una segunda tabla de estado de features: README sigue siendo la
fuente de ese estado. Futuros changes habilitarán cada tarjeta con su evidencia.

La acción manual kill permite explorar fallas, pero no ubica una caída dentro de
un commit ni aprueba automáticamente F-05. Los casos poison requerirán topics,
grupos y consumidores de ensayo aislados cuando se diseñe F-06; no se introducen
ahora ese entorno ni controles para saltar un mensaje inválido.

Alternativas: un botón que sólo imprime éxito o reinicia tras dormir un tiempo no
verifica una frontera de falla; reutilizar literalmente el script actual tampoco
entrega metadata por envío, pasos recuperables en el navegador ni resultados F-03.

### 6. API propia y UI mínima

Todas las rutas de consola viven bajo `/demo/api`; sólo la observación del listener
se agrega a las aplicaciones. El contrato público de órdenes se mantiene intacto.

| Área | Rutas propuestas |
| --- | --- |
| Observación | GET `/system`, `/orders`, `/orders/{id}`, `/results?topic=&partition=&offset=`, `/messages?topic=&partition=&cursor=&limit=` |
| Comparación y logs | GET `/instances/{service}/orders/{id}` y `/instances/{service}/logs` |
| Publicación | POST `/reports` y `/messages/republication` con posición de origen |
| Ejecuciones | GET `/catalog`, POST `/runs`, GET `/runs/{id}`, POST `/runs/{id}/next` y `/runs/{id}/cancel` |
| Servicios | POST `/services/{service}/{action}` |

Las rutas de la tabla son relativas a `/demo/api`. Parámetros, servicios y topics
se validan en el servidor. Los listados SQL usan límite y cursor estables; el
detalle de orden conserva el ledger completo del contrato actual. Errores de
preflight devuelven 400, conflicto de ejecución 409 y escenario pendiente 409 con
dependencia; un resultado inexistente devuelve `result: null`. Una falla de
observación se informa por fuente con sus datos anteriores marcados como stale.

La página utiliza tipografía de sistema, tablas legibles y JSON monoespaciado,
sin recursos de CDN. Refresca inicialmente cada dos segundos, sin solapar
peticiones y con opción de pausa. Controles etiquetados, foco visible, contraste,
estado vacío y mensajes de carga/error tienen prioridad sobre adornos. JSON y
logs se insertan con `textContent`; colores acompañan etiquetas de estado.

## Risks / Trade-offs

- [Acceso Docker amplio del proceso de consola] -> Binding local, comandos
  cerrados, comprobación de origen/proyecto y documentación del permiso real del
  socket; app-1/app-2 siguen sin ese acceso.
- [Consultas separadas observan momentos distintos] -> Fuentes, fechas y estado
  stale explícitos; consistencia SQL del detalle conservada; éxito de escenario
  sólo después de verificar condiciones.
- [Preflight manual sujeto a productores externos] -> Validación definitiva en el
  consumidor y visualización del resultado/diagnóstico real; nunca prometer
  aplicación por haber confirmado un envío.
- [Historial de runs volátil] -> IDs y posiciones visibles; SQL/Kafka conservan la
  evidencia; se documenta que reiniciar la consola pierde sólo su coordinación.
- [Dependencia o poison previo impide una prueba] -> Precondiciones y timeout
  visibles, sin borrar datos ni avanzar offsets para conseguir un resultado verde.
- [Controles manuales aparentan pruebas de F-05] -> Acción y alcance etiquetados;
  escenarios de frontera siguen pendientes hasta tener instrumentación y evidencia.

## Migration Plan

1. Implementar por incrementos según `tasks.md`, manteniendo las migraciones V1/V2
   y el procesamiento existente. No se necesita backfill ni migración SQL nueva.
2. Construir targets y levantar Compose con la consola y observación habilitada.
   Comprobar exactamente dos miembros de procesamiento y ambos endpoints públicos.
3. Actualizar README con puerto, recorrido, permisos, controles y disponibilidad
   observada; registrar en DECISIONS la excepción acotada para evaluación y sus
   límites. Sólo marcar implementada la consola con evidencia, conservando estados
   de F-04 a F-06 y preservando modificaciones ajenas de ambos documentos.
4. Ejecutar suite Maven, smoke anterior y un nuevo smoke portable `verify-console`
   a incorporar al perfil tools. Registrar resultados, sincronizar y archivar el
   change sólo al satisfacer sus escenarios y validar OpenSpec.
5. Rollback: detener únicamente la consola, retirar su servicio/perfil y desactivar
   observación de demo en las aplicaciones. Se conservan tablas, mensajes,
   volúmenes y offsets; no se revierte el núcleo ni se limpian datos automáticamente.
