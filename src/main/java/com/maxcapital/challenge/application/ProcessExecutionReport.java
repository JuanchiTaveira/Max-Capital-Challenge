package com.maxcapital.challenge.application;

import com.maxcapital.challenge.application.ProcessingResult.Outcome;
import com.maxcapital.challenge.domain.ExecutionReport;
import com.maxcapital.challenge.domain.InvalidReport;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProcessExecutionReport {
  private final OrderRepository repository;
  private final ProcessingResults results;
  private final QuarantineStore quarantine;

  public ProcessExecutionReport(
      OrderRepository repository, ProcessingResults results, QuarantineStore quarantine) {
    this.repository = repository;
    this.results = results;
    this.quarantine = quarantine;
  }

  // Estado, contador, ledger y resultado comparten el commit SQL; Kafka se confirma aparte.
  @Transactional("orderTransactionManager")
  public ProcessingResult process(InputPosition position, ExecutionReport report, String payload) {
    return process(position, report, payload, null);
  }

  @Transactional("orderTransactionManager")
  public ProcessingResult process(
      InputPosition position, ExecutionReport report, String payload, String correctionId) {
    var captured = quarantine.find(position, true);
    if (captured.isPresent()
        && (captured.get().state().equals("OPEN")
            || correctionId == null
            || !correctionId.equals(captured.get().correction().requestId())))
      throw new IllegalStateException("La corrección seleccionada ya no está vigente");
    // Una reentrega recupera el resultado y snapshot originales, aunque la orden haya avanzado.
    var stored = results.find(position, payload);
    if (stored.isPresent()) {
      return recover(report, stored.get());
    }
    repository.initialize(report);
    // El lock serializa las modificaciones de esta orden hasta completar la transacción SQL.
    var previous = repository.lock(report.numericOrderId());
    // Otra transacción pudo confirmar esta posición mientras esperábamos el lock.
    stored = results.find(position, payload);
    if (stored.isPresent()) {
      return recover(report, stored.get());
    }
    // Deduplicamos antes de validar transiciones para aceptar también terminales ya aplicados.
    var match = repository.match(report.numericOrderId(), report.fixId(), payload);
    ProcessingResult result;
    if (match.isPresent()) {
      // Reutilizar fixId con otro contenido es un conflicto de identidad, no un duplicado.
      if (!match.get().identical()) {
        throw new InvalidReport(
            "IDENTITY_CONFLICT", "contenido distinto para fixId " + report.fixId());
      }
      result =
          new ProcessingResult(
              position,
              Outcome.DUPLICATE,
              report.numericOrderId(),
              report.fixId(),
              match.get().ledgerId(),
              previous);
    } else {
      // Sólo un ER nuevo aplica la transición, incrementa el contador y agrega ledger.
      var next = previous.apply(report);
      repository.save(next);
      long ledgerId = repository.append(report, payload);
      result =
          new ProcessingResult(
              position, Outcome.APPLIED, report.numericOrderId(), report.fixId(), ledgerId, next);
    }
    // Cada posición nueva conserva su resultado y snapshot en la misma transacción SQL.
    results.insert(result);
    if (captured.isPresent()) quarantine.applied(position, result.outcome().name());
    return result;
  }

  private static ProcessingResult recover(
      ExecutionReport report, ProcessingResults.StoredResult stored) {
    var result = stored.result();
    // La posición sólo puede recuperar un resultado del mismo ER y con contenido equivalente.
    if (!stored.identicalReport()
        || result.numericOrderId() != report.numericOrderId()
        || result.fixId() != report.fixId()) {
      throw new IllegalArgumentException("contenido distinto para posición " + result.position());
    }
    return result;
  }
}
