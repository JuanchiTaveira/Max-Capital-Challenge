# Spec Delta

## MODIFIED Requirements

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
