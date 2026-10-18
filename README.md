# Max Capital — challenge backend

Servicio de procesamiento de ExecutionReports desarrollado por features con OpenSpec.
El enunciado está en [challenge-arch-max-capital..pdf](challenge-arch-max-capital..pdf),
en la raíz del repositorio.
F-01 prepara infraestructura y herramientas; las garantías de negocio se incorporan
desde F-02. Las dos instancias de F-01 están conectadas, todavía sin consumir ER.

## Features

Esta tabla es la única fuente del estado de implementación.

| ID | Feature | Estado |
| --- | --- | --- |
| F-01 | Entorno, OpenSpec, AGENTS.md y Spotless | Implementado |
| F-02 | Procesamiento y consulta de órdenes | Pendiente |
| F-03 | Idempotencia y resultados persistidos | Pendiente |
| F-04 | Settlement transaccional | Pendiente |
| F-05 | Recuperación ante caídas y rebalanceos | Pendiente |
| F-06 | Cuarentena y recuperación ordenada | Pendiente |
| F-07 | Entrega reproducible y documentación | Pendiente |
| F-08 | Integración continua — opcional | Pendiente |

## Prerrequisitos y versiones

Para ejecutar: Docker con contenedores Linux y Docker Compose, acceso a internet en
el primer build y puertos 8081, 8082, 9092 y 15432 disponibles. Reservar al menos
4 GB de RAM para Docker. Java y Maven del host no son necesarios para Compose.

Para desarrollar y ejecutar Testcontainers: JDK 25, Docker y Maven Wrapper incluido.
Para el smoke automatizado: PowerShell 7 (`pwsh`). OpenSpec 1.14.1 requiere Node.js.

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
F-03/F-05 cubrirán resultados por offset y más fronteras de caída/rebalanceo.
F-02 todavía no publica settlements.

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

### Comandos de verificación

En PowerShell:

```powershell
.\mvnw.cmd spotless:apply
.\mvnw.cmd spotless:check
.\mvnw.cmd verify
pwsh -File scripts/verify-f01.ps1
```

En Linux/macOS, reemplazar `.\mvnw.cmd` por `./mvnw`. El script smoke también
funciona con PowerShell 7 en Linux/macOS. La suite Maven usa JUnit y Testcontainers;
falla si Docker no está disponible. No hay Maven global requerido.

Spotless aplica Google Style a Java: dos espacios, imports ordenados y sin uso
eliminados, UTF-8 y LF. `validate` comprueba formato sin editar. `apply` corrige
fuentes. El build de imagen omite tests, pero mantiene ese check; no reemplaza
`verify` ni el smoke.

El smoke detiene y recrea contenedores del proyecto. Ejecutarlo sobre el entorno
local de demostración. Usa datos aislados y los limpia; conserva los volúmenes.

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

Kafka usa un solo broker: el entorno verifica caídas de aplicaciones y reinicios,
no supervivencia a pérdida física del volumen. La presencia de transacciones Kafka
no demuestra por sí sola settlement exactamente una vez. Esa garantía queda
pendiente de F-04/F-05.
