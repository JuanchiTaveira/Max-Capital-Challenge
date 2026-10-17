# Design

## Context

Repositorio inicialmente vacío. Java 25 y OpenSpec 1.14.1 disponibles; el motor
Linux de Docker debe estar operativo para verificar integración y Compose.

## Goals / Non-Goals

Base ejecutable con dos instancias, formato uniforme y evidencia reproducible.
El dominio de órdenes y las garantías de entrega pertenecen a features posteriores.

## Decisions

- Un módulo `com.maxcapital.challenge:max-capital-challenge:0.1.0-SNAPSHOT`.
- Java 25, Spring Boot 4.1.1 y Maven Wrapper 3.9.11; dependencias con BOM.
- JUnit Jupiter 6.0.3 y Testcontainers 2.0.5. Failsafe ejecuta `*IT` en verify.
- Spotless 3.10.4 con google-java-format 1.36.1 GOOGLE en validate. La versión
  1.37.0 del plan cambia Style.valueOf y es incompatible con el adaptador de Spotless.
- Imágenes oficiales por tag y digest; Kafka 4.2.1, PostgreSQL 17 y Temurin 25.
- KRaft de un broker, dos particiones y retención sin expiración en tres topics.
- PostgreSQL y Kafka con volúmenes nombrados. Init de topics idempotente.
- Servicios app-1/app-2 comparten imagen, con puertos 8081/8082 e identidad propia.
- Readiness incluye DB y AdminClient Kafka con timeout; liveness sólo aplicación.
- Flyway administra el esquema app; las migraciones de negocio comienzan en F-02.
- Smoke PowerShell aísla y limpia sus datos, prueba caídas y recreación con volúmenes.

## Risks / Trade-offs

Un broker no protege de pérdida física de su volumen. Salud de infraestructura no
demuestra consumo de ER ni settlement. Tests requieren Docker y fallan si falta.
El build Docker omite pruebas y mantiene el check de formato obligatorio.

## Migration Plan

Inicialización local, sin remoto ni datos existentes. Las verificaciones usan datos
aislados y conservan los volúmenes del entorno. Archivar sólo con evidencia completa.

## Open Questions

Ninguna para F-01. La arquitectura transaccional propuesta se validará en F-04/F-05.
