# Design

## Context

F-01 aporta un módulo Java 25 con Spring Boot, dos instancias Compose, un grupo
consumidor configurado pero sin listener, PostgreSQL compartido con Flyway en el
esquema `app` y `execution-reports` con dos particiones. No hay tablas de negocio.
El enunciado `challenge-arch-max-capital..pdf` está en la raíz del repositorio;
AGENTS.md y README.md deben enlazarlo como fuente de requisitos.
El PDF muestra una estructura de ER aproximada, no un esquema JSON
formal, y pide una consulta HTTP que reúna estado y ledger. Ver `proposal.md` y
las tres specs de este change para el contrato propuesto.

## Goals / Non-Goals

**Goals:**

- Hacer observables la secuencia por orden, la actualización SQL y el historial
  desde ambas instancias.
- Mantener el dominio libre de Spring, Kafka y JDBC; la aplicación coordina el
  caso de uso y los adaptadores resuelven transporte y persistencia.
- Conservar los campos del ER que todavía no intervienen en F-02, en especial
  precios e identidades de ejecución que podrían necesitarse en F-03/F-04.
- Dejar fallas de entrada o persistencia diagnosticables sin avance silencioso
  del offset.

**Non-Goals:**

- Publicar settlements o resultados en Kafka, ni afirmar exactamente una vez
  para efectos externos. F-04/F-05 tratarán esa frontera.
- Persistir un resultado por `(topic, partition, offset)` o resolver commits Kafka
  de resultado incierto; corresponde a F-03/F-05.
- Cuarentena, salto controlado del registro inválido y recuperación automática;
  corresponde a F-06.

## Decisions

### Contrato del ER y transición

Se adopta JSON UTF-8 como codificación del mensaje ilustrado en el PDF. F-02
requiere sólo los campos necesarios para identificar, secuenciar y aplicar el ER:
`fixId`, `numericOrderId`, `status`, `nominalAmounts`, `leavesNominalAmount`,
`accumulativeNominalAmount` y `transactionTime`. Ejemplo compatible:

```json
{
  "fixId": 523130930000307,
  "numericOrderId": 13144742,
  "marketOrderId": "O0S6tDQoQqVy",
  "ticker": "VSCPC",
  "side": "BUY",
  "securityType": "COMMON_STOCK",
  "status": "PARTIALLY_FILLED",
  "orderPrice": 104.25,
  "nominalAmounts": 4956,
  "leavesNominalAmount": 1200,
  "accumulativeNominalAmount": 3756,
  "executionNominalAmount": 800,
  "executionPrice": 104.30,
  "avgPrice": 104.28,
  "secondaryTradeId": "trade-1",
  "operationNumber": "operation-1",
  "transactionTime": "2026-07-20T18:08:52.129"
}
```

La key Kafka será `"13144742"`. Los campos adicionales y futuros se aceptan
y se guardan con el ER en el ledger, sin inventar validaciones de negocio para
F-02. Las cantidades requeridas se parsean como `BigDecimal`, sin `double` ni
redondeo. El tiempo local mostrado por el PDF es válido; también se acepta un
offset si el origen lo envía, pero nunca se usa para ordenar. Se conserva la
cadena recibida. Esto evita atribuir una zona horaria inexistente al ejemplo.

El dominio valida snapshots contra el estado anterior: `NEW` empieza con
acumulado cero y restante igual al total; `PARTIALLY_FILLED` aumenta acumulado
sin completar el total y tiene restante `total - acumulado`; `FILLED` alcanza
el total y deja restante cero; `CANCELLED` conserva acumulado no decreciente y
deja restante cero. Esta regla para cancelación interpreta «cantidad que resta
operar» como cero tras terminar la orden. `nominalAmounts` no cambia. Se elige
validar cantidades además del status para detectar ER contradictorios sin
convertir snapshots en deltas.

### Consumo y frontera de confirmación

Las instancias comparten `group-id`; cada listener procesa su partición en serie,
con concurrencia máxima dos para el topic de dos particiones. No se introducen
tareas asíncronas por registro ni se cambia el número de particiones. Se valida
la key contra el payload antes de tocar SQL. El consumidor mantiene
`read_committed`, `enable-auto-commit=false` y confirma manualmente el offset tras
retornar la transacción SQL. Auto-commit podría avanzar aunque falle la escritura.

Una excepción detiene o pausa la partición afectada sin recoverer que salte el
registro ni publicación a `execution-reports-rejected`. El diagnóstico incluye
topic, partición, offset, key, identificador si es legible y causa, sin secretos.
El registro y offset quedan pendientes. F-06 definirá la salida de ese estado.
Una prueba con un ER inválido seguido de uno válido en la misma partición debe
demostrar que el segundo no se aplica ni se adelanta el offset.

### Persistencia, deduplicación y concurrencia

Flyway agrega `app.orders` (`numeric_order_id` PK, status,
`nominal_amounts`, `accumulative_nominal_amount`, `leaves_nominal_amount` y
`applied_execution_count`) y `app.execution_report_ledger` (`ledger_id` identity
PK, FK a orden, `fix_id` y ER completo en `JSONB`). Las cantidades usan
`NUMERIC(20,8)`; el ledger tiene `UNIQUE (numeric_order_id, fix_id)` e índice
`(numeric_order_id, ledger_id)`. El `ledger_id` global refleja inserción, no
pretende ser consecutivo por orden.

En el único topic de entrada de F-02 se usa `(numericOrderId, fixId)` como
identidad del ER individual. Es una decisión explícita porque el PDF muestra
`secondaryTradeId` y `operationNumber` como identidades de ejecución, pero el
duplicado pedido es el ER, no la orden ni necesariamente una ejecución individual.
Se asume que `fixId` es estable y único por orden en este flujo. Una futura fuente
adicional necesitará ampliar el ámbito de esa identidad. El contenido se compara
como JSON semántico, sin depender del orden de campos; distinto contenido para
la misma identidad es conflicto, no un no-op.

El caso de uso abre una transacción SQL. Busca la identidad en el ledger y
selecciona explícitamente `orderTransactionManager` (JDBC): el productor
transaccional de F-01 también puede registrar un transaction manager Kafka,
que no debe interceptar este caso de uso. El listener usará una factory sin
transaction manager Kafka. El caso de uso
compara contenido antes de validar el estado terminal: una reentrega idéntica
termina en no-op y una conflictiva falla. Para un ER nuevo, bloquea la fila de
orden existente con `SELECT ... FOR UPDATE`, valida la transición, actualiza
estado y contador e inserta el ledger. La creación por `NEW` se protege con PK;
si surgiera una carrera, se reevalúa bajo la misma transacción. La unicidad SQL
respalda la serialización Kafka. Una transacción corta evita que las consultas
vean contador o estado desacoplados del ledger.

Un crash entre commit SQL y confirmación de offset puede reentregar el ER; la
identidad evita duplicar estado, contador y ledger. Esto no persiste un resultado
por offset ni prueba exactamente una vez en Kafka. El productor transaccional de
F-01 queda sin uso en F-02.

### Consulta

Un adaptador HTTP expone `GET /orders/{numericOrderId}` según
`order-query/spec.md`: estado y ledger juntos, con `ledger_id ASC`, desde la
base compartida. Devuelve 404 para orden ausente y 400 para ID inválido. Sólo
lee filas confirmadas. Se elige un endpoint para cumplir la consulta conjunta
pedida por el enunciado. No se agrega paginación en F-02; el tamaño de la demo
es acotado y una futura paginación deberá preservar el orden definido.

## Risks / Trade-offs

- **El PDF muestra un ER aproximado, no un esquema formal** → La spec enumera
  campos obligatorios y el ledger conserva los demás sin descartarlos.
- **`fixId` podría no ser único entre fuentes futuras** → F-02 recibe un solo
  flujo; F-03 podrá ampliar la identidad sin confundir orden con ER.
- **Registro inválido bloquea su partición** → Es visible y no se pierde;
  cuarentena y avance controlado quedan en F-06.
- **Crash entre commit SQL y offset** → Puede haber reentrega; la identidad de ER
  protege estado y ledger, y F-03 añadirá resultados por offset.
- **Un broker y dos particiones** → Verifica secuencia y paralelismo, sin prometer
  resistencia a pérdida física del volumen ni escalabilidad arbitraria.

## Migration Plan

Aplicar una migración Flyway aditiva antes de arrancar los listeners. El entorno
actual no contiene tablas de órdenes. Verificar con `.\mvnw.cmd verify` y un
smoke Compose que publique ER con IDs aislados, consulte ambas instancias y
compruebe secuencia, duplicado y persistencia al reiniciar. Una reversión de
aplicación puede volver al binario F-01 dejando tablas y datos intactos; no se
elimina el volumen ni se agrega una migración destructiva.
