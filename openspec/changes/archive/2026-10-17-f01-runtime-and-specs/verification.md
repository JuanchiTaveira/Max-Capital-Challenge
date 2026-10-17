# Evidencia de F-01

Verificaciones observadas el 17 de octubre de 2026 en Windows con JDK 25.0.2,
PowerShell 7 y Docker Desktop con motor Linux. Código ejecutable probado en el
commit `edbd22e`; el cierre posterior actualiza documentación y specs.

## Resultados observados

| Verificación | Resultado | Evidencia |
| --- | --- | --- |
| Build Java 25 con Wrapper | PASS | JAR ejecutable construido con Maven 3.9.11 |
| Spotless check en validate | PASS | Fuentes de producción y pruebas conformes |
| Desvío intencional en copia descartable | PASS | check y validate rechazaron; apply corrigió; segunda aplicación conservó el hash |
| Maven verify con Testcontainers | PASS | 3 tests, 0 fallas, 0 errores y 0 omitidos |
| HTTP técnico | PASS | Liveness/readiness 200, identidades y versión correctas, salud sin datos internos |
| PostgreSQL | PASS | SELECT 1, esquema app e historial Flyway presentes |
| Kafka | PASS | Topics de dos particiones y retención configurada; mensaje confirmado visible y abortado invisible en read_committed |
| Compose config | PASS | Configuración válida |
| Smoke Compose | PASS | Seis pasos completados; código de salida 0 |
| Clon limpio de Git | PASS | verify volvió a pasar con 3 tests; imagen Linux construida desde los archivos versionados |
| OpenSpec del change | PASS | Validación estricta de las tres capacidades |
| Cierre OpenSpec | PASS | 8/8 tareas completas; change archivado, 8 requisitos sincronizados y 3 specs principales válidas |

Comandos ejecutados: `mvnw.cmd verify`, `mvnw.cmd spotless:check` / `validate`,
`docker compose config --quiet`, `pwsh -File scripts/verify-f01.ps1 -SkipBuild`,
`openspec validate f01-runtime-and-specs --strict`. El smoke omitió reconstrucción
porque el build Compose se verificó por separado. También se construyó la imagen
desde el clon limpio con `docker compose -f target/clean-checkout/compose.yaml build app-1`.

## Smoke de Compose

1. Ambas instancias saludables, identificadas como app-1/app-2.
2. Init repetido sin errores y tres topics con configuración esperada.
3. Marcadores SQL y Kafka aislados creados y consultados.
4. PostgreSQL detenido: readiness 503 en ambas instancias y liveness 200;
   reiniciado: readiness 200.
5. Kafka detenido: readiness 503 y liveness 200; reiniciado: readiness 200.
6. Contenedores eliminados y recreados sin eliminar volúmenes: ambos marcadores
   persistieron. Datos aislados eliminados al finalizar; entorno saludable.

## Ajustes descubiertos al ejecutar

- google-java-format 1.37.0 cambia Style.valueOf y no es compatible con el adaptador
  de Spotless 3.10.4. Se fijó 1.36.1 y se repitieron las verificaciones de formato.
- Se usó Wrapper bin con JAR oficial para validar el mismo ZIP y SHA-256 en
  Windows/Linux. only-script elegía tar.gz en Linux sin unzip.
- Testcontainers declara compatibilidad de las imágenes oficiales por digest y
  lee las mismas referencias de Compose para evitar divergencias de versiones.

## Límites

Estos resultados demuestran infraestructura, formato y reproducibilidad de F-01.
No demuestran secuencia de ER, idempotencia SQL de negocio, ledger ni settlement.
F-02 a F-08 siguen pendientes. La extensión Testcontainers no está configurada
para omitir integración cuando Docker falta; no se detuvo el daemon para simularlo.
