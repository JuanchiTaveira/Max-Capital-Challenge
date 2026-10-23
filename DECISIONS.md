# Decisiones de diseño

## 1. Kafka y orden por orden

Kafka conserva los ExecutionReports (ER) y permite repartir su consumo entre las dos instancias. Se usan dos particiones, un mismo grupo de consumidores y `numericOrderId` como clave. Kafka envía los reportes de una misma orden a la misma partición; cada consumidor procesa su partición en orden. `transactionTime` no se usa para ordenar.

Se asume que los reportes se publican en secuencia y que el número de particiones se mantiene estable mientras haya órdenes en curso. Una orden lenta puede retrasar a otras de su partición. El lock de SQL evita que dos instancias actualicen una orden a la vez, pero no reemplaza el orden de Kafka.

## 2. Identidad y duplicados

Cada ER se identifica por `(numericOrderId, fixId)`. La base impide repetir esa combinación y compara el JSON completo. Se asume que `fixId` es estable y único dentro de una orden; el enunciado no lo especifica.

Si llega la misma identidad con otro contenido, se informa un error. Si el contenido coincide, el reporte se reconoce como duplicado antes de validar transiciones terminales, y no modifica el contador ni el ledger.

Kafka conserva la misma posición al reentregar un mensaje. Una republicación, en cambio, crea otra posición. Por eso se guarda un resultado inmutable para cada posición: `APPLIED` o `DUPLICATE`, junto con el snapshot original del resultado. Así, una consulta histórica no cambia cuando la orden avanza.

## 3. Escrituras atómicas en PostgreSQL

Las dos instancias comparten PostgreSQL. Una transacción y un lock por orden (`SELECT FOR UPDATE`) coordinan sus escrituras. El estado, el contador, el ledger y el resultado de cada posición se guardan juntos. Si algo falla, se revierte el conjunto. Después de obtener el lock, se vuelve a consultar si esa posición ya tiene resultado, porque otro intento pudo haber confirmado mientras tanto.

Este lock simplifica la concurrencia, aunque puede hacer que una instancia espere a la otra. Dura sólo durante la transacción SQL y no incluye la confirmación de Kafka.

## 4. Estado y consulta

El estado nuevo se calcula a partir del estado guardado y del ER recibido. La primera transición debe ser `NEW`, el acumulado no puede disminuir y no se aceptan ER nuevos después de `FILLED` o `CANCELLED`.

Las cantidades del ER son snapshots: muestran el estado acumulado, no un incremento. Se validan y luego se guardan. El contador suma uno por cada ER aplicado, incluidos `NEW` y el terminal. Se usan decimales exactos. Se asume que el total de la orden no cambia y que una cancelación deja cero pendiente.

El ledger conserva el ER completo y ordena sus entradas por inserción. Puede tener huecos si una transacción se revierte. La consulta HTTP obtiene el estado y el ledger desde la misma vista confirmada de la base. Guardar ambos permite consultar sin reconstruir el estado, pero requiere actualizarlos en una única transacción.

## 5. Publicación del settlement

Sólo el resultado original `APPLIED` de un ER `FILLED` genera settlement. Un `CANCELLED` o un duplicado no lo genera. El resultado guardado permite recuperar la salida después de una caída.

Después del commit SQL, Kafka confirma el settlement y el offset siguiente en una transacción. Un offset es la posición de lectura de la partición. Cada instancia usa un prefijo transaccional propio y sólo publica mientras siga siendo miembro vigente del grupo.

La unicidad depende del resultado SQL y de confirmar juntos salida y progreso en Kafka; un ID estable por sí solo no evita duplicados. Un consumidor con `read_committed` —que sólo lee transacciones Kafka confirmadas— ve un settlement para un `FILLED` cubierto por la activación y ninguno para `CANCELLED`. Las posiciones ya confirmadas no se liquidan retroactivamente.

SQL y Kafka confirman por separado. Por eso la API puede mostrar `FILLED` antes de que el settlement sea visible. Los sistemas que consumen esa salida deben resolver sus propios efectos y reintentos.

## 6. Recuperación ante fallas

Se aceptan las reentregas de Kafka. El estado y los resultados guardados permiten recuperar el trabajo sin depender de la memoria de las aplicaciones.

| Falla | Recuperación |
| --- | --- |
| SQL no confirma | Se revierte el intento y se procesa de nuevo esa posición antes que sus sucesoras. |
| SQL confirma, Kafka queda pendiente | Se recupera el resultado original sin repetir el efecto de negocio y se completa la salida y el avance del offset. Un envío no es visible hasta que Kafka confirma la transacción. |
| No se sabe si Kafka confirmó | Se detiene esa sesión y se comprueban los offsets confirmados antes de reanudar. Un timeout no prueba que la transacción haya abortado. |
| Hay un rebalanceo con SQL abierto | El intento puede confirmar o revertirse. El lock y el resultado guardado evitan aplicar dos veces; Kafka rechaza al consumidor que perdió la partición. |

Si se caen ambas aplicaciones, el trabajo pendiente se conserva mientras PostgreSQL y Kafka sigan disponibles. La recuperación por la instancia sobreviviente y el reinicio de una sesión detenida son casos distintos.

## 7. Errores y límites

**Mensaje inválido.** El mensaje original y el motivo del error se guardan en `quarantined_reports`. Las correcciones y sus resultados se guardan en `report_corrections`. La partición queda pausada y su posición no se confirma, para no procesar sus sucesoras fuera de orden. Las demás particiones pueden seguir. Esto también demora las otras órdenes de la partición afectada.

**Reintentos.** Sólo se reintentan errores SQL conocidos —como deadlock o serialización— cuando el rollback está confirmado. Se permiten hasta cinco intentos por sesión, con backoff exponencial y jitter. Si se agotan, la partición queda pausada. El límite vuelve a empezar al reiniciar o cambiar de consumidor. Una caída de infraestructura, un error desconocido o una confirmación Kafka incierta también detienen el procesamiento; no se tratan como errores del mensaje.

**Corrección.** El operador envía `effectiveKey` y `effectiveReport` a `POST /quarantine/corrections`; ambos quedan registrados en `report_corrections` para la posición original. Después reinicia las aplicaciones consumidoras con `docker compose restart app-1 app-2`. Al volver a leer esa posición, el consumidor busca la corrección guardada y aplica ese contenido antes de procesar sus sucesoras. La corrección conserva la identidad y la partición. La DLQ (cola de mensajes rechazados) publica un aviso, pero puede repetirlo si el proceso cae entre la publicación y el registro de que se envió. El detalle del flujo y sus verificaciones está en la [evidencia de F-06](openspec/changes/archive/2026-10-23-f06-quarantine-ordered-recovery/verification.md).

**Límites del entorno.** Esta solución de challenge no cubre la pérdida física de datos, alta disponibilidad, cambios arbitrarios de offsets ni la recreación de topics. Las garantías dependen de conservar PostgreSQL, Kafka, el grupo y las particiones. Las APIs son locales y no tienen autenticación; el responsable de una corrección es declarado.
