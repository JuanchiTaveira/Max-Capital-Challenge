# Verification

## Evidencia incremental

La evidencia incremental siguiente conserva el alcance de cada ejecución. No se
atribuye evidencia al build de imagen ni a la disponibilidad del catálogo.

- `.\mvnw.cmd spotless:apply test '-Dtest=RecoveryBarrierTest'`: SUCCESS, dos casos,
  cero fallas/errores/omisiones; alcance, tokens, consumo único, vencimiento y cancelación.
- `.\mvnw.cmd spotless:apply verify '-Dtest=RecoveryBarrierTest' '-Dit.test=RecoveryKafkaIT'`:
  SUCCESS, dos unitarias y cuatro integraciones, cero fallas/errores/omisiones.
  Kafka y PostgreSQL reales con imágenes fijadas; los kills usan destroyForcibly
  sobre JVM con PID verificado contra el control de barrera, sin cierre Spring.

## Correspondencia inicial

| Caso de RecoveryKafkaIT | Qué observa |
| --- | --- |
| observesRealSqlBoundariesExpiresWithoutSkippingAndDisablesControlsByDefault | HTTP 404 sin instrumentación; SQL invisible/visible según frontera; envío aceptado invisible; vencimiento deja offset pendiente y replay recupera. |
| killsOpenSqlDuringNewAndUpdateAndRecoversTheEntireSequence | Kill con NEW, PARTIAL o FILLED con SQL abierto; ausencia de orden parcial y de escrituras confirmadas del intento; ledger completo, sucesores y una salida por FILLED. |
| reassignsWithOpenSqlAndRejectsOldOwnerForCommitAndRollback | Reasignación real por poll antes de liberar SQL; desenlaces commit y rollback del anterior; rechazo de su intento y una sola aplicación. |
| killsAfterAcknowledgedSendAndAfterBothProcessesAndResumesOnBothPartitions | Dos JVM reales, kill después de send y luego de ambas con SQL confirmado; mismo resultado durable, una salida y continuación/CANCELLED sin salida en ambas particiones. |

Los resultados ampliados se registran en las secciones siguientes.
La prueba de respuesta perdida de F-04 conserva
su alcance: timeout inyectado después de delegar commit real; no corte físico de red.

## Recorridos e incrementos observados

- Suite dirigida RecoveryKafkaIT + DemoScenariosIT: ocho integraciones aprobadas,
  cero fallas/errores/omisiones. El fixture de DemoScenariosIT sustituye Docker y
  selecciona los cinco recorridos originales; verifica el catálogo real de siete
  y rechaza poison-recovery. Los dos recorridos nuevos se prueban con Compose real.
- RecoveryScenariosTest: cancelación invalida el control y reinicia un listener
  detenido; limpieza mantiene exclusión; fallo de restauración impide PASSED
  incluso después de verificar el escenario. Las primeras tres pruebas pasaron
  junto con las dos de RecoveryBarrierTest. La prueba adicional de paginación y
  lectura incompleta se incluye en la suite global posterior.
- `docker compose up -d --build --wait`: entorno reconstruido y saludable, con
  instrumentación de demo habilitada y volúmenes anteriores conservados.
- `docker compose --profile tools run --build --rm verify-f05`: exit 0.
  Settlement único run `141375ca-161c-48a7-9b90-195c2e524f07` y recuperación run
  `966ebd23-96c6-4807-b549-0d704b555c9f`, ambos PASSED, RESTORED y todos sus checks
  completos. Los cinco casos incluyen SQL abierto, SQL confirmado, send aceptado,
  reasignación durante SQL y caída de ambas aplicaciones. El runner comprobó
  igualdad de todas las órdenes previas y dos consumidores finales.
- Se reforzó después el alcance del control: el ER debe llevar el scenarioRun
  armado y no puede tener ledger previo al armar. RecoveryKafkaIT pasó nuevamente
  sus cuatro casos tras ese cambio; los smokes finales volverán a reconstruirlo.

La salida completa de cada run se imprime como JSON en el runner, con posiciones,
snapshot anterior, resultado recuperado, grupo, barrera/PID/generación y salidas.
Los logs locales de desarrollo en target no son el artefacto durable de entrega;
esta correspondencia y los IDs permiten inspeccionar los datos conservados.

## Revisión de navegador observada

La consola mostró siete escenarios ejecutables y sólo poison-recovery pendiente.
Se revisaron explicación previa, acciones, resultados sin JSON y las verificaciones
de los dos runs automáticos del primer smoke. Luego se completaron desde el navegador:

- `c994b12d-f3e8-4139-85c5-e34f6a2ceb12`: recuperación en modo paso a paso, PASSED.
  Se recargó después de Continuar en el preflight; la URL, el run y el siguiente
  caso se conservaron. Cada frontera se ejecutó dentro de su paso, sin transacción
  abierta durante las pausas humanas.
- `6b640e1f-f39a-44d4-adf4-648211e00331`: settlement en modo paso a paso, PASSED,
  con una salida FILLED, cero CANCELLED, duplicados y recuperación tras kill.
- `885006e3-3555-4e91-b864-a76ea8d45b9c`: cancelación entre preflight y primera
  publicación de negocio, CANCELLED con RESTORED. La vista explicó que los checks
  pendientes no se completaron; se observaron dos consumidores, una partición
  cada uno y lag cero. La cancelación después de alcanzar una barrera se cubre
  además con RecoveryScenariosTest y la prueba de barrera real.

Se guardó captura local de settlement aprobado en `target/f05-manual-settlement.png`.
No se modificaron CSS ni estructura de navegación: las guías, checks y conclusiones
nuevas usan la presentación existente, incluyendo enlaces de evidencia a órdenes.

## Ejecuciones que requirieron corrección

- Un primer build local detectó que el getter del AfterRollbackProcessor pertenece
  al contenedor, no a la factory. Se corrigió la personalización al crear el
  contenedor y luego pasaron las pruebas dirigidas.
- El primer intento de verify-f05 encontró formato pendiente en un test recién
  agregado. Se aplicó Spotless; el siguiente build y smoke terminaron correctamente.
- La primera suite global terminó con 15 unitarias aprobadas y una de las 50
  integraciones en error: ClassNotFoundException de RecoveryProcess al iniciar un
  hijo. Coincidió con otra compilación dirigida sobre target/test-classes lanzada
  durante la suite. Se repite la verificación global sin compilaciones concurrentes;
  ese intento fallido no se usa como evidencia de cumplimiento.
- El intento global del 22 de octubre aprobó las 18 unitarias pero no pudo
  ejecutar las integraciones: Docker Desktop estaba detenido y no existía su pipe.
  Se inició el motor y se repitió la suite; ese intento no cuenta como aprobación.

## Smokes finales secuenciales observados

Sobre Compose reconstruido, sin borrar volúmenes, resetear offsets ni cambiar el
grupo, terminaron con exit 0, en este orden:

1. `docker compose --profile tools run --build --rm verify-f02`: F-02 OK;
   órdenes, duplicados y consultas conservados.
2. `docker compose --profile tools run --build --rm verify-f04`: F-04 OK;
   FILLED `179262282200901`, CANCELLED `179262282200920`, settlement `0/36`,
   fuente `execution-reports/0/213`, ledger `269`.
3. `docker compose --profile tools run --build --rm verify-f05`: F-05 OK;
   settlement `f35c77ae-2605-40d5-8421-597a91d073f4`, recuperación
   `d7f62813-4e9b-40ba-8271-681c287e7e61`, PASSED y RESTORED. Posiciones finales
   estables `0/243` y `1/138`, iguales a sus extremos; dos consumidores, una
   partición cada uno y todas las órdenes previas sin cambios.
4. `docker compose --profile tools run --build --rm verify-console`: CONSOLE OK,
   run `32b2fcbe-aecc-40b2-9795-05157b0b17de`, siete escenarios aprobados y uno
   excluido; `poison-recovery` rechazado, restauración y 113 órdenes previas
   conservadas. Incluye fallas de dependencias y recuperación de la consola.

Estos smokes corresponden al código de aplicación final; los cambios posteriores
amplían exclusivamente las pruebas de kill terminal y ausencia de kill sin barrera.

## Correspondencia completa de las delta specs

En esta tabla R1–R4 nombran, en orden, los cuatro métodos de RecoveryKafkaIT de la
tabla inicial. S es el run settlement final; C es el run recuperación final;
L es el lote de siete escenarios final. Todos usan Kafka/PostgreSQL reales;
S/C/L también usan Docker real. RecoveryScenariosTest sustituye dependencias y
prueba coordinación y resultados de fallas, no demuestra un kill real.

| Capacidad / Scenario | Evidencia |
| --- | --- |
| execution-report-consumption / Process killed with an open SQL transaction | R2: kill NEW/PARTIAL/FILLED, replay, ledger, cantidades y sucesores; C sql-open. |
| Initial order creation interrupted | R2: NEW no visible antes de kill y creación completa al recuperar. |
| Previous owner commits SQL after losing membership | R3 commit: nueva asignación antes de liberar, intento antiguo rechazado, resultado y sucesores; C rebalance. |
| Previous owner rolls back after losing membership | R3 rollback: aplicación única y secuencia recuperada. |
| Both processes killed with pending work | R4 y C both-down: resultado original, sucesores, dos miembros y nuevas órdenes en ambas particiones. |
| Failed consumption session requires operational restart | R1: listener detenido tras vencer, reinicio y recuperación; restauración de C comprueba consumo además de HTTP. |
| settlement-publication / Process dies between SQL commit and publication | SettlementKafkaIT, R4, S y C sql-committed: resultado original y una salida. |
| Broker commits but response is lost | SettlementKafkaIT: timeout inyectado tras commit real, offset estable, una salida y sucesor. No corte físico de red. |
| Process dies before terminal SQL commit | R2 FILLED: kill antes de commit, SQL invisible, replay con una aplicación y settlement original único. |
| Process dies after acknowledged settlement send | R4 y C send-ack: envío aceptado invisible antes de kill, transacción resuelta y una salida final. |
| Both applications restart with a pending completion | R4 y C both-down: resultado conservado, FILLED uno/CANCELLED cero. |
| evaluation-console / Instrumentation disabled | R1: HTTP 404 sin flag; suite del núcleo ejecutada con configuración ordinaria. |
| Scoped barrier control | RecoveryBarrierTest: run/identidad/token/consumo único; R1: control ajeno 409; DemoScenariosIT rechaza interferencia manual; runtime conserva dos miembros. |
| Arm before publishing and inspect after arrival | R1 y S/C: ARMED a REACHED con posición, PID, generación; visibilidad SQL y ack diferenciados. |
| Expiry or process restart | R1: vencimiento en las tres fronteras, offset pendiente/replay; RecoveryBarrierTest: controles viejos y generación nueva. |
| Original instance unavailable during takeover | S/C: SQL, Kafka y grupo disponibles tras kill; comparación HTTP sólo al retornar la instancia. |
| Open output transactions and complete counting | R1/R4 y S/C: read_committed, resolución y extremos estables; RecoveryScenariosTest prueba paginación e impide aprobar rango incompleto. |
| demo-scenarios / Current implementation boundary | DemoScenariosIT verifica catálogo real siete/un pendiente; L ejecuta siete con Docker real. |
| Pending execution request | DemoScenariosIT y verify-console: poison-recovery 409 sin ejecución. |
| Persisted effects after restart | L y verify-f02/F04: estado/ledger iguales y republicaciones DUPLICATE; no se atribuye caída dentro de commit a ese reinicio. |
| First individual scenario | DemoScenariosIT y revisión de navegador: selección/explicación previa, sin publicar al seleccionar. |
| Recovery explanation before execution | Revisión de S/C automática y manual: guía de fronteras, caída, recuperación y restauración antes de actuar. |
| Five explained results | L y fixture de los cinco originales: 4/3, acumulados, duplicados, reinicio y abort previsto. |
| Seven explained results | L: siete conclusiones con checks; S/C agregan frontera y conteos uno/cero. |
| Partial or unavailable observation | RecoveryScenariosTest: evidencia INCOMPLETE conservada al cancelar; fallo de restauración impide PASSED; cancelación real en navegador. |
| Terminals and business duplicates | S: FILLED/CANCELLED en distintas particiones, republicaciones y NEW tardío DUPLICATE, comparación contra resultado original. |
| Completion redelivered after process death | S: kill AFTER_SQL_COMMIT observado, resultado idéntico, salida única y dos consumidores al final. |
| Open SQL transaction killed and recovered | C sql-open y R2: invisibilidad SQL, kill, replay y sucesores. |
| Post SQL and acknowledged send process deaths | C sql-committed/send-ack y R4: resultado SQL previo, cero salida antes, una después. |
| Reassignment while SQL remains open | C rebalance y R3: nueva asignación antes de liberar, rechazo viejo y conteo tras finalizar ambos intentos. |
| Both applications killed and returned | C both-down y R4: pendientes, ambas particiones, conteos FILLED/CANCELLED y dos miembros. |
| Manual progression and reload | Navegador: recuperación c994b12d y settlement 6b640e1f completos paso a paso, recarga entre casos. |
| Cancellation or timeout at a barrier | RecoveryScenariosTest: cancela después de REACHED, preserva evidencia y restaura; R1 vence barreras reales; navegador cancela entre pasos. |
| Restoration or barrier observation fails | RecoveryScenariosTest: sin REACHED no hay kill; control inaccesible reinicia; limpieza retiene exclusión; falla invalida incluso éxito previo. |

## Cierre global observado el 22 de octubre de 2026

- `.\mvnw.cmd spotless:apply spotless:check verify` aplicó/comprobó formato y
  aprobó 18 unitarias, pero falló integraciones por Docker detenido, como se
  registra arriba. Tras iniciar Docker Desktop, `.\mvnw.cmd spotless:check verify`
  terminó BUILD SUCCESS: 18 unitarias y 50 integraciones, cero fallas, errores
  u omisiones. RecoveryKafkaIT aprobó sus cuatro métodos, incluido el tercer
  caso NEW/PARTIAL/FILLED; RecoveryScenariosTest aprobó sus siete casos.
- `docker compose up -d --wait` restauró el entorno conservado después del inicio
  del motor. La API de sistema observó PostgreSQL/Kafka disponibles, dos miembros
  y ambos consumidores en ejecución con una partición cada uno.
- La tabla de README se actualiza sólo después de estos resultados y de los
  cuatro smokes anteriores. F-06 no se inició.
- OpenSpec confirmó 30/30 tareas completas. Se sincronizaron nueve requisitos
  nuevos y cinco modificados en las cuatro capacidades existentes, preservando
  títulos, propósitos, requisitos no afectados y escenarios anteriores. La
  comparación posterior confirmó las cuatro delta specs íntegramente aplicadas.
- Change archivado en `2026-10-22-f05-crash-rebalance-recovery`, con su metadata.
  `openspec validate --all --strict`: diez specs aprobadas, cero errores;
  `openspec list --json`: sin changes activos. `git diff --check`: sin errores.
  Se conservan commits locales; no se hizo push.
