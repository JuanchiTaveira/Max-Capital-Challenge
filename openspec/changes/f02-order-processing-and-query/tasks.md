# Tasks

## 1. Contrato y dominio

- [x] 1.1 Implementar el modelo y parser JSON/key para `fixId`, `numericOrderId`, status, `nominalAmounts`, `leavesNominalAmount`, `accumulativeNominalAmount` y `transactionTime`, conservando los demás campos; verificar con pruebas de ejemplo del enunciado, faltantes, key distinta, tiempo local y cantidades fuera de contrato.
- [x] 1.2 Implementar las transiciones puras de la orden con snapshots de acumulado y restante; verificar con pruebas de `NEW`, parciales sucesivos, `FILLED`, `CANCELLED`, total inconsistente y duplicado terminal evaluado antes de la transición.
- [x] 1.3 Documentar en README el contrato JSON/key y las reglas de transición sin cambiar F-02 de Pendiente; agregar en AGENTS.md y README.md el enlace a `challenge-arch-max-capital..pdf` en la raíz; verificar enlaces y ejemplos con el parser y registrar en DECISIONS los supuestos sobre `fixId`, cancelación y contador.
- [x] 1.4 Aplicar Spotless y crear un commit Conventional Commit del dominio; verificar `.\mvnw.cmd spotless:check` y las pruebas unitarias.

## 2. Estado y ledger SQL

- [x] 2.1 Agregar la migración Flyway aditiva de órdenes y ledger con ER completo en JSONB, restricciones e índices del diseño; verificar en PostgreSQL Testcontainers PK, FK, unicidad e identidad autoincremental.
- [x] 2.2 Implementar la transacción de aplicación y el adaptador JDBC para comparar `(numericOrderId, fixId)`, bloquear orden, aplicar transición y escribir ledger/contador juntos; verificar con integración SQL secuencia, snapshots, duplicado idéntico, conflicto de contenido y rollback ante error de escritura.
- [x] 2.3 Crear un commit Conventional Commit del bloque SQL tras ejecutar las pruebas de integración y `.\mvnw.cmd spotless:check`.

## 3. Ingesta Kafka

- [x] 3.1 Incorporar el listener secuencial por partición con key validada, `read_committed` y confirmación manual posterior al commit SQL; verificar con Kafka/PostgreSQL Testcontainers órdenes intercaladas, dos particiones, reentrega idéntica y transacción productora abortada.
- [x] 3.2 Configurar la falla visible sin recuperación que salte registros; verificar con integración que un ER inválido y una falla SQL dejan el offset sin confirmar, no aplican el siguiente ER de la partición y permiten reprocesar al reiniciar.
- [ ] 3.3 Documentar en README el diagnóstico y límite de recuperación de F-02, y crear un commit Conventional Commit tras `.\mvnw.cmd spotless:check` y las pruebas Kafka/SQL.

## 4. Consulta HTTP

- [ ] 4.1 Implementar `GET /orders/{numericOrderId}` sobre SQL confirmado con estado y ledger en una respuesta, 200/400/404 y `ledgerId` ascendente; verificar con integración HTTP campos, orden, ausencia y que el duplicado no aparece dos veces.
- [ ] 4.2 Documentar la ruta, campos y respuesta de ejemplo en README; verificar el ejemplo contra las pruebas HTTP y crear un commit Conventional Commit tras `.\mvnw.cmd spotless:check`.

## 5. Verificación integral y cierre de F-02

- [ ] 5.1 Agregar un smoke aislado de Compose con dos instancias que publique ER del formato del enunciado en ambas particiones, consulte 8081/8082 y compruebe secuencia, duplicado y datos tras reiniciar sin borrar volúmenes; verificar ejecutando el script y conservar su salida como evidencia.
- [ ] 5.2 Ejecutar `.\mvnw.cmd verify`, `.\mvnw.cmd spotless:check`, el smoke y `openspec validate --all --strict`; registrar resultados observados y limitaciones, marcar sólo entonces F-02 como Implementado en README y crear el commit Conventional Commit de cierre.

## Workflow follow-up

- Sincronizar las delta specs y archivar este change después de verificar todos sus criterios, según AGENTS.md.
