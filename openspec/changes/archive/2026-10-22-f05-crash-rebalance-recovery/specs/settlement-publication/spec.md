# Spec Delta

## MODIFIED Requirements

### Requirement: Recovery after process failure and uncertain commit
Tras una caída o commit Kafka incierto, el sistema SHALL resolver el progreso
desde offsets estables del grupo antes de volver a publicar. Si el intento confirmó,
SHALL continuar después de su posición; si abortó, SHALL recuperar el resultado SQL
y completar la salida. SHALL conservar diagnóstico de incertidumbre y no tratar
un timeout como evidencia de aborto ni reenviar desde la posición local anterior.

#### Scenario: Process dies between SQL commit and publication
- **GIVEN** un proceso con FILLED y resultado APPLIED confirmados en SQL, sin settlement confirmado
- **WHEN** el proceso termina abruptamente en esa frontera y la otra instancia recupera la partición
- **THEN** recupera el mismo resultado y confirma un settlement junto al progreso Kafka
- **AND** al volver la instancia original no se repite ni el efecto SQL ni el settlement

#### Scenario: Broker commits but response is lost
- **GIVEN** settlement y offset confirmados realmente por Kafka
- **WHEN** la aplicación recibe un timeout en lugar de la respuesta y reinicia su sesión de consumo
- **THEN** recupera el progreso confirmado sin volver a publicar esa finalización
- **AND** queda un único settlement visible y el sucesor puede procesarse

#### Scenario: Process dies before terminal SQL commit
- **GIVEN** un FILLED con escrituras SQL todavía sin confirmar
- **WHEN** el proceso termina abruptamente y la partición se recupera
- **THEN** el intento incompleto no conserva efectos SQL ni settlement confirmado
- **AND** recuperar el ER deja una sola aplicación y exactamente un settlement confirmado con los datos del resultado original exitoso

#### Scenario: Process dies after acknowledged settlement send
- **GIVEN** un FILLED confirmado en SQL y un settlement cuyo envío fue aceptado dentro de una transacción Kafka todavía sin confirmar
- **WHEN** el proceso termina abruptamente antes del commit Kafka y otro propietario recupera la partición
- **THEN** el intento abierto se resuelve sin hacerse visible como una segunda salida read_committed
- **AND** después de resolver los intentos y recuperar el progreso existe exactamente un settlement confirmado y el resultado SQL original permanece igual

#### Scenario: Both applications restart with a pending completion
- **GIVEN** un FILLED con resultado SQL confirmado, Kafka pendiente y sucesores publicados
- **WHEN** ambas aplicaciones terminan abruptamente y vuelven a consumir el mismo grupo y datos
- **THEN** recuperan el resultado original, completan el progreso y procesan los sucesores sin repetir efectos
- **AND** después de resolver las transacciones pendientes se observa exactamente un settlement para FILLED y cero para órdenes CANCELLED del recorrido
