# Spec Delta

## ADDED Requirements

### Requirement: Guided evaluation workspace
La vista Escenarios SHALL ofrecer selector y explicación, pasos agrupados por
escenario y comparación Verificación/Esperado/Observado/Resultado sin abrir JSON.
SHALL mantener otras vistas, recuperación por URL y controles de ejecución;
la evidencia técnica SHALL quedar desplegable con enlaces a registros y órdenes.

#### Scenario: Individual guided progression
- **WHEN** se ejecuta un escenario y termina
- **THEN** se ve una conclusión legible y puede seleccionarse el siguiente sin iniciarlo
- **AND** en modo manual se explica el próximo paso antes de continuar

#### Scenario: Batch navigation and reload
- **WHEN** se ejecutan todos o se recarga una ejecución en curso
- **THEN** se conserva el avance y pueden consultarse resultados por escenario sin mezclar pasos
- **AND** no se interpreta el título de un paso ni su error para deducir qué verificó

#### Scenario: Accessible narrow viewport
- **WHEN** se usa teclado o una ventana de 375 px
- **THEN** selector, controles y resultados siguen accesibles, con foco visible y estados textuales
- **AND** la selección no pierde el foco por el refresco y el contenido técnico se presenta como texto inerte
