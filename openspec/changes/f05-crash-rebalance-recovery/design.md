# Design

## Context

Ver `proposal.md` para motivación y alcance. Esta propuesta todavía no aporta
evidencia nueva de ejecución.

`ProcessExecutionReport` confirma mediante `orderTransactionManager`: busca el
resultado por posición, inicializa y bloquea la orden, vuelve a buscar el resultado,
deduplica por ER y escribe estado, ledger y resultado juntos. `HandleExecutionReport`
recibe el resultado después del commit JDBC y publica el settlement elegible dentro
de la transacción Kafka del contenedor. El offset siguiente se confirma al retornar
el listener. `StopAfterTransactionFailure` detiene la sesión sin hacer seek local;
`stopContainerWhenFenced` protege la pertenencia obsoleta.

`SettlementKafkaIT`, `TransactionProbe` y `SettlementProcess` ya prueban varias
fronteras de F-04, incluido kill de JVM después de SQL. Las barreras actuales son
exclusivas de test y sólo seleccionan envíos terminales; no permiten demostrar SQL
abierto ni ejecutar los recorridos desde Compose.

La consola dispone de exclusión por `DemoGate`, ejecución por `ScenarioRunner`,
observaciones capturadas por `DemoRun` y lectura paginada read_committed mediante
`ConsoleKafka`. Actualmente ofrece cinco escenarios y tres pendientes. Las specs
de catálogo, explicación y resultados contienen ese límite y deben cambiar juntas.
`ScenarioRunner` sólo restaura servicios con start: iniciar un contenedor que sigue
vivo con listener detenido no recupera ese consumo. `DemoScenariosIT` sustituye
Docker y por ello no basta para demostrar caídas reales.

## Goals / Non-Goals

**Goals:**

- Usar el protocolo existente como base y localizar fallas con evidencia de la
  frontera real, reutilizando el mismo camino de aplicación en pruebas y demo.
- Mantener instrumentación fuera del dominio y de los contratos de negocio;
  distinguir infraestructura de ensayo de las garantías que se están comprobando.
- Tener ventanas críticas automáticas y acotadas, incluso en modo paso a paso,
  y terminar con dos consumidores activos o un diagnóstico de restauración fallida.

**Non-Goals:**

- No introducir un coordinador distribuido, una nueva outbox ni fencing SQL por
  generación sin una falla demostrada que lo haga indispensable.
- No agregar reintentos de negocio automáticos, clasificación de errores,
  cuarentena ni recuperación de pérdida física de datos.
- No prometer atomicidad distribuida SQL/Kafka ni ejecución única downstream.

## Decisions

### 1. Mantener SQL idempotente y salida/progreso Kafka transaccionales

Una reasignación no cancela por sí sola una transacción SQL del propietario anterior.
Ese intento puede terminar confirmando; el lock de la orden, la segunda búsqueda de
resultado y las restricciones SQL deben convertir el intento del nuevo propietario
en recuperación del mismo resultado. El propietario anterior ya no debe poder
confirmar la transacción Kafka con metadatos de grupo obsoletos. Una transacción SQL
revertida permite la primera aplicación por el nuevo propietario.

Se verifica cada desenlace, incluyendo el final de ambos intentos y los sucesores.
Se prefiere esto a agregar épocas SQL: F-03/F-04 ya tienen los mecanismos necesarios,
y otra coordinación sin necesidad probada aumentaría las fronteras a resolver.
Si una prueba revela una brecha, se corrige de forma mínima dentro de este change
y se actualiza este diseño antes de atribuir garantías.

| Frontera | Recuperación prevista | Evidencia exigida |
| --- | --- | --- |
| Escrituras SQL sin commit | Rollback y primera aplicación por reentrega | Ausencia de efectos parciales; un APPLIED y un ledger |
| SQL confirmado, Kafka pendiente | Recuperar resultado original y completar Kafka | Snapshot/ledger originales intactos; progreso y salida elegible |
| Send aceptado, Kafka abierto | Resolver transacción anterior y recuperar desde progreso estable | Intento abierto/abortado invisible; una salida final |
| Commit Kafka confirmado sin respuesta | Nueva sesión obtiene progreso estable | No reenviar finalización; sucesor continúa |
| Reasignación con SQL abierto | Serializar intentos SQL y rechazar commit Kafka obsoleto | Reasignación observada, ambos intentos terminados, secuencia intacta |
| Ambas aplicaciones terminadas | Reiniciar conservando base, cluster y grupo | Pendientes resueltos; dos miembros; nuevas órdenes en ambas particiones |

El reinicio operativo de una sesión detenida sigue siendo explícito. La toma de
particiones por la instancia sobreviviente usa el grupo Kafka existente. No se
equiparan estos dos mecanismos ni se introduce un supervisor automático de errores.

### 2. Instrumentación opt-in en wiring de demo y adaptadores

Agregar configuración condicionada por `app.demo.recovery-enabled`, ausente/false
por defecto y habilitada explícitamente para app-1/app-2 en Compose. El mismo
artefacto conserva el procesamiento normal sin control de fallas al deshabilitarla.
No se agregan callbacks al modelo de dominio ni escrituras de estado por HTTP.

Las fronteras son `BEFORE_SQL_COMMIT`, `AFTER_SQL_COMMIT` y
`AFTER_SEND_ACK_BEFORE_KAFKA_COMMIT`. Para SQL se decorará el puerto de resultados
en el wiring de demo y se registrarán sincronizaciones de la transacción JDBC:
la primera barrera se alcanza después de las escrituras, antes del commit; la
segunda sólo después de confirmar. El decorador delega en persistencia real y no
captura una excepción para simular éxito. Para Kafka se reutiliza el enfoque del
postprocesador de ProducerFactory de F-04: esperar la aceptación real del send
antes de señalar la barrera, sin confirmar ni abortar fuera del contenedor.

Los ensayos de replay no vuelven a armar una barrera consumida. Para SQL, seleccionar
un resultado nuevo es suficiente: el replay recupera el resultado sin insertarlo.
La instrumentación deberá probar el orden real de callbacks respecto del commit
desde otra conexión SQL; el nombre de un hook no demuestra la frontera.

Alternativas descartadas: sleeps antes de kill (no ubican el momento), triggers
temporales instalados en la base compartida de demo (dejan estado global y afectan
datos ajenos), y un listener/procesador alternativo (no ejercita el núcleo real).

### 3. Barreras dirigidas, observables y con vencimiento

Mantener una barrera activa por instancia con `runId`, identidad nueva de orden/ER,
frontera y token de intento. Al armar todavía no se conoce el offset: sólo cuando
llega el ER se captura su `InputPosition`, instancia y generación de proceso. La
consola selecciona el dueño de la partición observada antes de publicar. Si cambió
el dueño o no se alcanza la barrera, el caso falla sin atribuir una frontera ni
provocar un kill a ciegas.

El controlador interno permite armar, observar y liberar/invalidar, sólo con
operaciones enumeradas, identidad de ejecución y token coincidentes. Se usa el
cliente interno de instancias; el navegador sigue entrando por la API existente
de escenarios y su control de origen/exclusión. Las barreras no habilitan scripts,
SQL, payloads ni destinos arbitrarios. Esto es control local de demo, no una capa
de autenticación de producto.

Estados: ARMED, REACHED, RELEASED, EXPIRED y CANCELLED; ausencia tras restart se
informa como intento anterior no disponible, nunca como barrera alcanzada. El
estado guarda posición, frontera y marcas de tiempo. Una barrera vencida aborta
el intento o detiene la sesión según la frontera: si SQL ya confirmó queda pendiente
de recuperación; no se transforma expiración en éxito. Reiniciar descarta el estado
en memoria y tokens viejos no controlan una barrera nueva.

Se coordinarán los tiempos exclusivamente en demo/tests: un presupuesto inicial
es max.poll.interval de 10 s, sesión de 6 s, heartbeat de 1 s, transacción Kafka
de 60 s, barrera de hasta 45 s y paso de hasta 150 s. El timeout SQL de espera del
nuevo propietario debe permitir liberar al anterior tras observar reasignación;
el socketTimeout actual de 3 s necesita ajuste explícito para este ensayo. Los
valores finales se fijan y justifican con evidencia en README. No se espera al
timeout de transacción Kafka para demostrar pérdida de pertenencia.

### 4. Cada ventana crítica cabe en un paso

Los dos IDs existentes se habilitan sin crear escenarios de catálogo adicionales.
`settlement-once` combina FILLED/CANCELLED en particiones distintas, republicaciones,
comparación del contrato y un FILLED nuevo interrumpido después de SQL.
`commit-boundary-recovery` recorre casos con órdenes nuevas por caso:

1. Kill con SQL abierto y sucesores pendientes.
2. Kill después de SQL antes de publicación.
3. Kill después de send aceptado antes de confirmar Kafka.
4. Reasignación por vencimiento de poll con SQL abierto, liberando al anterior
   sólo tras observar la asignación del nuevo propietario.
5. Kill de ambas aplicaciones con resultado SQL pendiente de Kafka y sucesores;
   reinicio, recuperación y nuevas órdenes en ambas particiones.

Preparación, publicaciones normales y comparación final pueden ser pasos separados.
Armar, publicar el ER objetivo, observar REACHED, provocar la falla/reasignación y
resolver la ventana ocurren juntos dentro de un paso acotado. En modo manual no
se espera Continuar con una transacción suspendida: una pausa de 30 minutos es
incompatible con expiración y max.poll. La UI describe previamente todo lo que hará
ese paso; recargar conserva el modelo de avance existente mientras vive la consola.

Separar los nuevos casos en un colaborador específico evita seguir acumulando
responsabilidades en `ScenarioSteps`. `ScenarioGuidance` y las conclusiones de
`DemoRun` deben agregar checks explícitos, sin inferirlos de títulos o texto de error.

### 5. Evidencia estable y restauración como parte del resultado

Antes del ensayo capturar cursores de ambas particiones de settlements. Después de
finalizar los intentos y observar progreso estable, recorrer páginas hasta el límite
estable posterior. Se cuentan registros por las órdenes nuevas y se compara el
mensaje con el APPLIED FILLED original. No se concluye unicidad desde una página
vacía, un log o un límite tomado antes de resolver la transacción anterior.

Durante una caída se consulta SQL, Kafka, el grupo y la instancia sobreviviente;
comparar HTTP entre ambas se reserva para cuando vuelven. Capturar resultados antes
y después permite distinguir recuperación de APPLIED de republicación DUPLICATE.
En reasignación se registra el rechazo Kafka del intento anterior desde diagnóstico
atribuido al caso y se espera su finalización antes de contar salidas definitivas.

Extender el finally de `ScenarioRunner` para invalidar/liberar barreras de esa
ejecución, iniciar instancias terminadas y reiniciar las que sigan vivas con listener
detenido. Luego verificar dos miembros, una partición por instancia y consumo real.
La limpieza usa un presupuesto propio y se ejecuta aun si el run fue cancelado o su
paso expiró; no llama a una espera que aborte inmediatamente por cancelación.
Si falla, el run queda fallido y expone estado y acción operativa pendiente.

La exclusión permanece adquirida hasta terminar la restauración. El estado de
barreras no es durable, por lo que su vencimiento protege también una caída de la
consola; README explica cómo iniciar/reiniciar las apps en ese caso. No se promete
recuperar historial en memoria tras perder el proceso de consola.

### 6. Evidencia por capas y documentación del núcleo

- Pruebas de procesos y Kafka/PostgreSQL reales cubren kill antes del commit SQL,
  después de send y ambas instancias, SQL abierto con commit y rollback tras reasignación,
  NEW interrumpido, sucesores y retorno a dos miembros. Reutilizar F-04 para sus
  casos ya cubiertos, incluido commit real con respuesta perdida.
- Pruebas de consola verifican contrato, checks, cancelación/timeout, tokens,
  barreras deshabilitadas y modos de ejecución. Sustituir Docker sólo demuestra
  coordinación, no kill; esa limitación se conserva explícita.
- Smoke portable `verify-f05` ejecuta los dos escenarios nuevos sobre Compose con
  procesos reales. Actualizar `verify-console` a siete/un pendiente y conservar
  los smokes existentes. Ejecutarlos secuencialmente con datos previos preservados.
- Revisar en navegador explicación, avance, cancelación, recarga y resultados de
  ambos modos. Sólo adaptar UI donde las nuevas verificaciones lo requieran.
- `DECISIONS.md` debe actualizar recuperación, protocolo, límites y consola con la
  matriz de estrategias y referencias a evidencia observada. Comandos y tiempos
  van en README; ninguna propuesta se redacta como garantía ya implementada.

## Risks / Trade-offs

- Barreras o transacciones vencen en hosts lentos -> presupuestos coherentes,
  diagnóstico por fase y falla explícita; no ampliar a esperas indefinidas.
- Un propietario pierde la partición mientras conserva un lock SQL -> verificar
  commit y rollback del anterior y resolución del nuevo; no tomar reasignación como
  prueba suficiente de progreso.
- Una lectura read_committed queda detrás de una transacción abierta -> esperar
  resolución y repetir lectura hasta un límite estable posterior al ensayo.
- Cancelación con listener detenido -> invalidar barreras y reiniciar el proceso
  si start no restablece consumo; comprobar grupo antes de liberar exclusión.
- Instrumentación aumenta el artefacto de aplicación -> wiring condicionado,
  operaciones mínimas y prueba de ausencia cuando está deshabilitada.
- La demo controla servicios compartidos -> exclusión local, órdenes nuevas,
  conservación de datos y ejecución secuencial de smokes; no proteger de clientes
  externos que publiquen/controlen el mismo entorno simultáneamente.

## Migration Plan

No se requiere migración SQL ni backfill. Conservar volúmenes, topics, particiones,
retención, grupo y prefijos transaccionales distintos por instancia. Detener y
reconstruir ambas aplicaciones juntas, habilitando explícitamente los controles
de recuperación sólo en Compose de demo.

Deshabilitar la instrumentación no cambia el protocolo de F-04. Si falla el
despliegue, detener ambas aplicaciones y conservar estado para corregir; no volver
a una versión anterior a F-04 que confirme offsets sin settlement. La tabla de
README sólo marca F-05 implementada tras verify, smokes reales y criterios cumplidos.
Al cerrar apply se sincronizan/validan specs y se archiva el change con evidencia.
