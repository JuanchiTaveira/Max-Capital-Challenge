# runtime-environment Specification

## Purpose
Proveer un entorno reproducible con dos instancias, infraestructura persistente y
verificaciones observables de salud, conectividad y transacciones del broker.

## Requirements

### Requirement: Reproducible runtime
El repositorio SHALL construir Java 25 con Maven Wrapper y levantar mediante
`docker compose up` PostgreSQL, Kafka, dos instancias de la misma aplicación y una
consola de evaluación independiente accesible en `http://localhost:8083`.

#### Scenario: Clean startup
- **WHEN** se construye y ejecuta Compose desde un clon limpio con Docker disponible
- **THEN** ambas instancias alcanzan readiness saludable con identidades distintas
- **AND** Kafka contiene los tres topics declarados con dos particiones
- **AND** la consola queda accesible en el puerto 8083 ligado a loopback, sin herramientas del host adicionales a Docker y un navegador

#### Scenario: Exactly two processing consumers
- **WHEN** las dos aplicaciones y la consola están ejecutándose y el grupo está estable
- **THEN** el grupo de procesamiento tiene exactamente dos miembros y una partición asignada a cada uno
- **AND** la consola no participa del grupo ni ejecuta migraciones o escrituras de negocio

#### Scenario: Console remains available during outages
- **WHEN** una aplicación o una dependencia del núcleo se detiene después del arranque
- **THEN** la consola mantiene su UI y API de control disponibles
- **AND** informa consultas degradadas y permite solicitar el inicio del servicio detenido

### Requirement: Dependency-aware readiness
La aplicación SHALL exponer liveness independiente y readiness dependiente de
PostgreSQL y Kafka con timeout acotado, sin exponer credenciales.

#### Scenario: Dependency outage and recovery
- **WHEN** PostgreSQL o Kafka deja de estar disponible
- **THEN** readiness devuelve 503 y liveness permanece saludable
- **WHEN** la dependencia vuelve a estar disponible
- **THEN** readiness vuelve a devolver 200

### Requirement: Persistent infrastructure
El entorno SHALL conservar datos SQL y mensajes Kafka al recrear contenedores
manteniendo sus volúmenes.

#### Scenario: Recreate containers
- **GIVEN** datos SQL y Kafka aislados de prueba
- **WHEN** se recrean contenedores sin eliminar volúmenes
- **THEN** los datos siguen siendo consultables

### Requirement: Real infrastructure verification
La suite SHALL verificar conectividad SQL, esquema, topics y transacciones Kafka
con Testcontainers y un consumidor read_committed.

#### Scenario: Transaction visibility
- **WHEN** se confirman y abortan publicaciones de infraestructura
- **THEN** read_committed sólo observa los mensajes confirmados

#### Scenario: Docker unavailable
- **WHEN** se ejecuta verify sin acceso a Docker
- **THEN** la suite falla explícitamente sin omitir las pruebas
