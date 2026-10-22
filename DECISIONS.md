# Decisiones de diseño

## 1. Kafka y secuencia por orden

Kafka permite conservar y releer ER, y repartir el trabajo sin un coordinador propio.
Se usan dos particiones, un mismo grupo y la key `numericOrderId` en decimal canónico.
Cada consumidor procesa secuencialmente su partición; `transactionTime` no ordena.

Se asume publicación en secuencia y particionado estable por key. No se aumentan
particiones con órdenes en curso. Hay hasta dos consumidores activos: una orden lenta
puede demorar otras de su partición. El lock SQL protege concurrencia, pero la
secuencia depende del particionado y del consumo en serie.

## 2. Identidad e idempotencia

Se identifica cada ER por `(numericOrderId, fixId)`, con restricción única y comparación
del JSONB completo. Se asume que `fixId` es estable y único dentro de la orden;
el enunciado no lo garantiza expresamente. Una identidad con contenido distinto es
un error; un duplicado equivalente se reconoce antes de validar terminales y no
incrementa contador ni ledger.

La reentrega conserva `(topic, partition, offset)`; una republicación ocupa otra
posición. Por eso se conserva además un resultado inmutable por posición:
`APPLIED` o `DUPLICATE`, con su snapshot original, sin reconstruirlo desde la orden actual.

## 3. Persistencia atómica

PostgreSQL compartido permite coordinar ambas instancias con transacciones,
restricciones y bloqueo por orden (`SELECT FOR UPDATE`). Estado, contador, ledger
y resultado por posición se escriben en una sola transacción: una falla revierte todo.
Tras adquirir el lock se vuelve a buscar el resultado, porque otro intento pudo confirmar.

La espera explícita simplifica la concurrencia frente a reintentos optimistas, a costa
de contención y una dependencia común. El lock termina con SQL; no abarca el commit Kafka.

## 4. Estado y consulta coherentes

La transición se calcula desde el estado persistido y el ER: NEW inicial, acumulado
no decreciente y ningún ER nuevo después de FILLED o CANCELLED. Las cantidades son
snapshots que se validan y adoptan; el contador suma uno por ER aplicado, incluidos
NEW y terminal. Se usan decimales exactos y se asumen total constante y restante
cero al cancelar.

El ledger conserva el ER completo con clave autoincremental, que ordena inserciones
por orden y admite huecos por rollback. La consulta HTTP lee estado y ledger desde
un mismo snapshot confirmado. Mantener ambos evita reconstruir el estado y exige
escribirlos atómicamente.

## 5. Settlement único

Sólo el resultado original `APPLIED FILLED` origina settlement; CANCELLED y DUPLICATE
no publican. Ese resultado durable permite reconstruir la misma salida tras una caída.
Una transacción Kafka confirma juntos settlement y offset siguiente, después del commit
SQL. Se usan prefijos transaccionales distintos por instancia y pertenencia vigente al grupo.

La unicidad combina idempotencia SQL, resultado inmutable y salida/progreso
transaccionales; un ID estable por sí solo no evita publicaciones duplicadas.
`read_committed` observa un registro confirmado por FILLED cubierto por la activación,
y cero por CANCELLED. No hay liquidación retroactiva de posiciones ya confirmadas.
SQL y Kafka tienen commits independientes: HTTP puede mostrar FILLED antes de la
salida. Los efectos externos del consumidor downstream requieren su propia recuperación.

## 6. Recuperación

Se acepta la reentrega y se recupera desde datos persistidos y offsets estables;
la corrección no depende de memoria de las aplicaciones.

| Frontera de falla | Recuperación |
| --- | --- |
| SQL sin confirmar | Rollback del intento y aplicación de la misma posición, antes de sus sucesores. |
| SQL confirmado, Kafka pendiente | Recuperar el resultado original sin repetir efectos y completar salida/progreso. Un envío aceptado sigue invisible hasta confirmar Kafka. |
| Commit Kafka incierto | Detener la sesión y resolver offsets estables antes de reanudar; un timeout no demuestra aborto. |
| Rebalanceo con SQL abierto | El intento anterior puede confirmar o revertir SQL. Lock y resultado serializan el efecto; Kafka rechaza al propietario obsoleto. |

Si caen ambas aplicaciones, reiniciarlas conserva pendientes mientras sobrevivan
PostgreSQL y Kafka. Recuperación por el sobreviviente y reinicio operativo de una
sesión detenida son mecanismos distintos.

## 7. Errores y límites

Ante error se conserva el ER sin confirmar, se registra diagnóstico con su posición
y se detiene el consumidor. Resolver una falla transitoria requiere corregir su causa
y reiniciar. Un mensaje permanente puede bloquear indefinidamente su partición:
clasificación, reintentos acotados, cuarentena y tratamiento de órdenes incompletas
siguen pendientes de F-06. Descartar sólo el ER fallido y aplicar sucesores rompería
la secuencia.

El entorno simplificado no cubre pérdida física de volúmenes, alta disponibilidad,
resets arbitrarios de offsets ni recreación de topics. Las garantías requieren
conservar datos, grupo y particiones. La evidencia y sus límites están en OpenSpec:
[settlement](openspec/changes/archive/2026-10-21-f04-transactional-settlement/verification.md)
y [recuperación](openspec/changes/archive/2026-10-22-f05-crash-rebalance-recovery/verification.md).
