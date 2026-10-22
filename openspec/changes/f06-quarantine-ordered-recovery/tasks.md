# Tasks

## 1. Persistencia y resoluciones de entrada

- [ ] 1.1 Crear migración aditiva V3 para ingresos/resoluciones, intentos, barreras, pendientes, solicitudes/revisiones y despacho de comandos; verificar migración V2->V3 con órdenes, ledger y resultados previos idénticos y restricciones de identidad/concurrencia activas.
- [ ] 1.2 Incorporar puertos y modelos de aplicación para evidencia binaria, posición fuente y resoluciones distintas de APPLIED/DUPLICATE; verificar con pruebas de persistencia que null, bytes vacíos, UTF-8 inválido y headers repetidos se reconstruyen exactamente y no requieren crear una orden.
- [ ] 1.3 Implementar coordinación SQL partición -> orden -> pendiente y lectura de resolución antes/después del lock; verificar con transacciones concurrentes que una reentrega conserva la resolución y snapshot originales y no duplica filas.
- [ ] 1.4 Documentar en DECISIONS las resoluciones y atomicidad que ya estén verificadas, distinguiendo el protocolo aún pendiente; verificar enlaces a las pruebas y conservar F-06 pendiente en README.

## 2. Clasificación y reintentos durables

- [ ] 2.1 Introducir clasificación explícita por causa y etapa con códigos de validación, rollback conocido y fallo operativo; verificar casos de JSON, key, identidad, transición, deadlock, indisponibilidad, excepción desconocida y commit incierto, sin clasificar toda RuntimeException como permanente.
- [ ] 2.2 Implementar reserva por posición/ronda, política congelada, presupuesto y nextAttemptAt con backoff exponencial y jitter; verificar con reloj/RNG controlados los cinco intentos máximos, límites de intervalo/tiempo, reserva interrumpida y preservación de la espera tras reinicio.
- [ ] 2.3 Integrar espera posterior al rollback sin transacciones abiertas y con polls/heartbeats activos, conservando detención ante commit incierto/fencing; verificar un transitorio recuperado sin sucesores adelantados y un rebalanceo durante la espera sin renovar presupuesto.
- [ ] 2.4 Implementar autorización local auditada de nueva ronda, idempotencia y recepción RETRY_AUTHORIZED; verificar que reiniciar sin autorización no regenera intentos y repetir la autorización no renueva dos veces, incluso cuando su comando está detrás del pendiente.
- [ ] 2.5 Documentar configuración, conteo conservador, agotamiento y reanudación en README/DECISIONS; verificar los valores efectivos y que el procedimiento no utiliza DLQ para una dependencia caída ni relaja los tiempos del ensayo F-05.

## 3. Cuarentena, retención y DLQ

- [ ] 3.1 Adaptar listener/deserialización para conservar bytes y distinguir comandos, recuperar resoluciones antes de parsear y capturar permanentes después del rollback; verificar rollback atómico de cuarentena/barrera y ausencia de cambios en orden, contador, ledger y resultados exitosos.
- [ ] 3.2 Implementar barreras de orden/partición y captura de sucesores con selección del menor offset; verificar que una orden sana de la misma partición continúa, una key/payload contradictoria retiene conservadoramente la partición, y no se pierde un sucesor llegado durante apertura/cierre de barrera.
- [ ] 3.3 Implementar contrato de rechazo v1 y publicación transaccional con offset, asegurando rejected con dos particiones; verificar reconstrucción binaria, IDs exactos, envío fallido sin avance y aborto con ausencia read_committed.
- [ ] 3.4 Agregar ensayos Kafka de kill después de SQL de cuarentena, envío de DLQ aceptado y respuesta de commit perdida, con reasignación real a otra instancia; verificar un único rechazo confirmado, barrera conservada y ausencia de settlement o resultado exitoso del inválido.
- [ ] 3.5 Adaptar ExecutionReportKafkaIT para sustituir detención obligatoria de permanentes por cuarentena y retención, conservando los casos de incertidumbre; verificar las pruebas afectadas y documentar el nuevo significado de offset confirmado en README/DECISIONS.

## 4. Corrección auditada y comandos de recuperación

- [ ] 4.1 Implementar aceptación durable de solicitudes con requestId, revisión, motivo, responsable declarado y ER efectivo, más validación de alcance; verificar concurrencia, repetición equivalente, reutilización conflictiva, revisión obsoleta, éxito previo y asociación ambigua incompatible con la partición.
- [ ] 4.2 Implementar despacho por las apps con claim/lease, commandId estable y partición original, sin depender de consola viva; verificar caída entre aceptación/envío y recuperación del claim, mostrando que envíos duplicados siguen siendo posibles sin duplicar aplicación.
- [ ] 4.3 Implementar validación del envelope recovery.v1 contra solicitud durable y resolución por posición/commandId; verificar controles falsificados, mal encaminados, obsoletos y copias en nuevos offsets sin aplicación ni salida adicional y sin bloquear un control válido posterior.
- [ ] 4.4 Aplicar una corrección o pendiente por comando bajo locks, guardando éxito en posición del comando y referencia a fuente/revisión; verificar original inmutable, corrección idéntica como DUPLICATE y rollback de estado, ledger, resultado, auditoría y avance ante falla SQL.
- [ ] 4.5 Programar RESUME del siguiente pendiente en el mismo commit SQL y liberar barreras sólo al agotarse; verificar llegada concurrente de nuevos ER, múltiples órdenes bajo barrera de partición, otro permanente durante recuperación y reinicio de ambas apps con trabajo restante.
- [ ] 4.6 Documentar en DECISIONS el protocolo y sus límites de routing y en README cómo solicitar, consultar y repetir una corrección; verificar ejemplos contra el contrato implementado y conservar separados original, revisión efectiva y posición del comando.

## 5. Settlement durante recuperación

- [ ] 5.1 Extender resultado/publicador para conservar intención original APPLIED FILLED del comando y source del ER retenido, manteniendo contrato v1; verificar cero salidas para retención, cuarentena, CANCELLED, DUPLICATE y copias de comando en otras posiciones.
- [ ] 5.2 Extender las barreras/procesos de prueba para matar una JVM con recuperación SQL abierta, después del commit SQL y después del envío de settlement aceptado; verificar takeover real, secuencia, un ledger por ER y exactamente un settlement read_committed tras resolver todos los intentos.
- [ ] 5.3 Verificar respuesta de commit perdida y rebalanceo con SQL abierto durante recuperación, incluidos commit y rollback del propietario anterior; comprobar recuperación del resultado del mismo comando y rechazo Kafka del propietario obsoleto sin doble salida.
- [ ] 5.4 Documentar en DECISIONS/README la frontera comando/salida y la diferencia entre outbox de comandos al menos una vez y unicidad confirmada; verificar correspondencia de source, ledgerId y snapshot en las pruebas de contrato.

## 6. Consulta y consola existente

- [ ] 6.1 Agregar processing a consulta de orden desde el mismo snapshot SQL y consultas paginadas de cuarentena/solicitud; verificar ACTIVE/QUARANTINED/RECOVERING, barrera de partición, identidad sin orden aplicada, exactitud numérica y compatibilidad de 404/400.
- [ ] 6.2 Exponer aceptación/consulta local de correcciones y nueva ronda con respuesta durable separada de aplicación; verificar 202, repetición tras respuesta HTTP perdida, conflictos, origen ajeno y exclusión DemoGate sin escrituras directas de negocio.
- [ ] 6.3 Incorporar evidencia y acciones mínimas a Mensajes/Órdenes sin nueva navegación ni bypass del preflight ordinario; verificar en navegador original binario, auditoría, error de revisión, estado recuperable tras reinicio de consola, teclado y ancho de 375 px.
- [ ] 6.4 Documentar consultas y acciones en README y su límite de responsable declarado sin autenticación; verificar los ejemplos por API y que la UI distingue captura SQL, aplicación y progreso Kafka observado.

## 7. Ensayo guiado y smoke portable

- [ ] 7.1 Implementar poison-recovery y controles opt-in limitados a su ejecución para JSON/key inválidos, identidad, transición, transitorio y corrección; verificar automático y paso a paso con dos miembros, orden sana de la misma partición, barrera ambigua y resultados antes/después de corregir.
- [ ] 7.2 Implementar restauración mediante correcciones predefinidas propias, conservando originales y datos ajenos; verificar recarga, cancelación, timeout y fallo de limpieza con diagnóstico explícito y sin éxito ficticio ni eliminación de evidencia.
- [ ] 7.3 Agregar scripts/verify-f06.py y servicio Compose tools, adaptar verify-console al lote de ocho; verificar dos corridas secuenciales que conserven órdenes/cuarentenas previas y cuenten por completo los rangos estables de DLQ/settlement.
- [ ] 7.4 Actualizar catálogo, explicación y guía README sólo con observaciones del ensayo; verificar que ejecutar todos incluye ocho resultados independientes y que borrar/alterar datos previos no forma parte de la restauración.

## 8. Integración y evidencia de entrega

- [ ] 8.1 Ejecutar .\mvnw.cmd spotless:apply para Java editado y .\mvnw.cmd verify con Docker; verificar formato, suite completa sin omisiones y regresión de F-02/F-04/F-05, incluidos resultados V2 pendientes y activación con un poison previo.
- [ ] 8.2 Ejecutar secuencialmente docker compose --profile tools run --build --rm verify-f02, verify-f04, verify-f05, verify-f06 y verify-console; registrar resultados reales y confirmar dos consumidores al finalizar, sin resetear offsets ni eliminar datos.
- [ ] 8.3 Crear verification.md con evidencia por criterio, comandos, conteos y límites; actualizar README/DECISIONS y marcar F-06 implementada sólo si están verificados todos los criterios. Validar con openspec validate --all --strict y revisar git diff --check; si una dependencia impide verificar, registrar el motivo y conservar tareas/feature pendientes.

## Workflow follow-up

- Conservar commits locales Conventional Commits por incremento coherente, con formato corregido; no publicar ni hacer push.
- Tras completar todas las tareas y verificar criterios, sincronizar specs y archivar mediante openspec-archive-change; comprobar la validación estricta del resultado archivado.
- Detenerse al entregar F-06 y su evidencia, sin comenzar F-07 ni F-08.
