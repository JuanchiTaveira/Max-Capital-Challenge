# Max Capital — challenge backend

Servicio en Java 25 y Spring Boot que consume ExecutionReports desde Kafka con
dos instancias en paralelo. Mantiene la secuencia por orden, deduplica reportes,
persiste estado y ledger en PostgreSQL y publica settlement al completar una orden.
Incluye recuperación ante fallas, cuarentena y escenarios guiados para evaluarlo.

[Enunciado](challenge-arch-max-capital.pdf) · [Decisiones de diseño](DECISIONS.md)

## Features

Esta tabla es la única fuente del estado de implementación.

| ID | Feature | Estado |
| --- | --- | --- |
| F-01 | Entorno, OpenSpec, AGENTS.md y Spotless | Implementado |
| F-02 | Procesamiento y consulta de órdenes | Implementado |
| F-03 | Idempotencia y resultados persistidos | Implementado |
| F-04 | Settlement transaccional | Implementado |
| F-05 | Recuperación ante caídas y rebalanceos | Implementado |
| F-06 | Cuarentena y recuperación ordenada | Implementado |
| F-07 | Entrega reproducible y documentación | Implementado |
| F-08 | Integración continua — opcional | Pendiente |
| F-09 | Consola local de evaluación ([evidencia de la ficha integrada](openspec/changes/scenario-evidence-workspace/verification.md)) | Implementado |

## Levantar el proyecto

Requiere Docker con contenedores Linux y Compose, al menos 4 GB de RAM y los puertos
8081, 8082, 8083, 9092 y 15432 libres. El primer build necesita internet; no requiere
Java ni Maven instalados en el host.

Desde la raíz del repositorio:

```sh
docker compose up --build
```

Levanta Kafka, PostgreSQL, dos instancias de la aplicación y la
[consola de evaluación](http://localhost:8083). Las aplicaciones atienden en
[localhost:8081](http://localhost:8081) y [localhost:8082](http://localhost:8082);
la consulta de una orden es `GET /orders/{numericOrderId}` e incluye estado,
contador de ER aplicados y ledger en orden de inserción.

## Ejecutar y verificar los escenarios

1. Abrir [http://localhost:8083](http://localhost:8083) y entrar en **Escenarios**.
2. Empezar por **Dos órdenes en paralelo** y leer la explicación y el resultado esperado.
3. Elegir **Automático** y pulsar **Ejecutar escenario**, o **Ejecutar todos** para recorrer los ocho. En **Paso a paso**, pulsar **Continuar paso** para avanzar.
4. Verificar la conclusión y la tabla **Verificación / Esperado / Observado / Resultado**. Cada escenario debe terminar aprobado; un error o timeout indica que la verificación falló.
5. En la misma ficha, inspeccionar **Mensajes enviados**, seleccionar un mensaje y relacionarlo con su resultado SQL y fila de ledger. Las tarjetas de órdenes muestran cantidades, estado e historial de snapshots; **Salidas e incidentes** reúne settlements y recuperación. El payload y los detalles técnicos se pueden desplegar.
6. Usar **Herramientas avanzadas** para publicar o republicar mensajes, consultar órdenes y diagnosticar el sistema. Al volver a **Escenarios** se conserva la ejecución seleccionada. Seleccionar un escenario abre su explicación; sólo **Ejecutar escenario** inicia el recorrido.

Los ocho escenarios cubren órdenes intercaladas, snapshots, duplicados, persistencia,
transacciones abortadas, settlement único, caídas entre commits/rebalanceos y
cuarentena con corrección. Los ensayos de fallas restauran los servicios y conservan
los datos. El historial de ejecuciones de la consola vive en memoria.

El paquete `com.maxcapital.challenge.demo`, los recursos web de `resources/demo`
y la instrumentación opcional `demo.recovery` son soporte auxiliar para esta UI,
los escenarios y los ensayos de fallas. Contienen el ejecutor, consultas de evidencia
y controles locales; las reglas de negocio del procesamiento de órdenes están en
el núcleo de la aplicación.

Para ejecutar la misma verificación por API, con el entorno levantado:

```sh
docker compose --profile tools run --build --rm verify-console
```

Debe terminar con código 0 y `CONSOLE OK`, `scenarios=8` y `pending=0`. También verifica
caídas de PostgreSQL/Kafka y restauración. Ejecutar los recorridos de a uno, sin
controles manuales concurrentes. La consola necesita acceso al socket Docker local;
Compose lo monta para controlar únicamente los servicios de esta demo.

## Ejecutar los tests

Requiere JDK 25 y Docker activo para Testcontainers. Maven Wrapper está incluido.

En PowerShell:

```powershell
.\mvnw.cmd verify
```

En Linux/macOS:

```sh
./mvnw verify
```

`verify` comprueba formato y ejecuta pruebas unitarias y de integración; debe terminar
con `BUILD SUCCESS`, sin fallas ni omisiones. Construir la imagen no reemplaza estos
tests. Para formatear Java: `spotless:apply` con el mismo Wrapper.

## Detener y diagnosticar

```sh
docker compose stop
docker compose start
docker compose logs app-1 app-2
```

Los volúmenes conservan los datos. `docker compose down` elimina contenedores;
`docker compose down -v` también elimina los datos.

Las garantías, supuestos y límites están en [DECISIONS.md](DECISIONS.md).
