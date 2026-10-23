const $ = id => document.getElementById(id);
const views = ['scenarios', 'messages', 'orders', 'system'];
const state = {
  view: 'scenarios', runId: null, run: null, runReadFailed: false, active: null, actionBusy: false,
  refreshing: false, paused: false, catalog: null, system: null,
  messageCursor: '0', messageNext: null, messageEnd: '0', records: [], selected: null,
  orderCursor: null, orderNext: null, orderId: null, order: null, highlight: null,
  scenarioId: 'interleaved-orders', explicitScenario: false,
  evidenceSelection: null, snapshotSelection: {}, currentOrderReads: {},
};
const labels = { RUNNING: 'En ejecución', WAITING: 'Esperando continuación', PASSED: 'Aprobado', FAILED: 'Falló', CANCELLED: 'Cancelado', PENDING: 'Pendiente' };
const signatures = new Map();
const pendingReads = new Map();

function element(tag, text = '', cls = '') {
  const item = document.createElement(tag);
  if (text !== '') item.textContent = String(text);
  if (cls) item.className = cls;
  return item;
}
function button(text, action, cls = '') {
  const item = element('button', text, cls); item.type = 'button';
  item.addEventListener('click', action); return item;
}
function notice(id, text, error = false) {
  const item = $(id); item.textContent = text; item.classList.toggle('error', error);
}
function time(value) { return value ? new Date(value).toLocaleTimeString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour12: false }) + ' ART' : ''; }
function api(path, body) {
  if (body === undefined && pendingReads.has(path)) return pendingReads.get(path);
  const request = requestApi(path, body);
  if (body === undefined) {
    pendingReads.set(path, request);
    request.then(() => pendingReads.delete(path), () => pendingReads.delete(path));
  }
  return request;
}
async function requestApi(path, body) {
  const options = { signal: AbortSignal.timeout(body === undefined ? 20000 : 40000) };
  if (body !== undefined) {
    options.method = 'POST'; options.headers = { 'Content-Type': 'application/json', 'X-Demo-Console': '1' };
    options.body = JSON.stringify(body);
  }
  const response = await fetch('/demo/api' + path, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || data.detail || `La consulta devolvió HTTP ${response.status}`);
  return data;
}
function table(headers, rows) {
  const result = element('table'), head = element('thead'), tr = element('tr');
  for (const title of headers) { const th = element('th', title); th.scope = 'col'; tr.append(th); }
  head.append(tr); result.append(head); const body = element('tbody');
  for (const row of rows) {
    const line = element('tr');
    for (const [index, value] of row.entries()) { const td = element('td'); td.dataset.label = headers[index]; td.append(value instanceof Node ? value : document.createTextNode(String(value ?? '—'))); line.append(td); }
    body.append(line);
  }
  result.append(body); return result;
}
function replaceStable(id, signature, content) {
  if (signatures.get(id) === signature) return;
  const openDetails = new Map([...$(id).querySelectorAll('details')].map((item, index) => [item.dataset.preserveKey || `index:${index}`, item.open]));
  const focused = $(id).contains(document.activeElement) ? document.activeElement : null;
  const identity = focused ? { id: focused.id, label: focused.getAttribute('aria-label'), text: focused.textContent } : null;
  signatures.set(id, signature); $(id).replaceChildren(content);
  [...$(id).querySelectorAll('details')].forEach((item, index) => {
    const key = item.dataset.preserveKey || `index:${index}`;
    if (openDetails.has(key)) item.open = openDetails.get(key);
  });
  if (identity) {
    const replacement = [...$(id).querySelectorAll('button,input,select,a,summary')].find(item =>
      (identity.id && item.id === identity.id) || (identity.label && item.getAttribute('aria-label') === identity.label) || (!identity.id && !identity.label && item.textContent === identity.text));
    replacement?.focus({ preventScroll: true });
  }
}
function show(view, updateHash = true) {
  if (!views.includes(view)) view = 'scenarios';
  const changed = state.view !== view; state.view = view;
  for (const name of views) {
    $(name).hidden = name !== view;
    $('tab-' + name).setAttribute('aria-current', String(name === view));
  }
  $('advanced-tools').open = view !== 'scenarios';
  if (updateHash) history.replaceState(null, '', view === 'scenarios' ? scenarioHash() : '#' + view);
  if (changed) window.scrollTo({ top: 0, behavior: 'instant' });
  refresh(true);
}
function hasActiveRun() {
  return ['RUNNING', 'WAITING'].includes(state.run?.status) || Boolean(state.active &&
    (state.run?.id !== state.active || !['PASSED', 'FAILED', 'CANCELLED'].includes(state.run?.status)));
}
function disableMutations() {
  const active = hasActiveRun();
  $('run-mode').disabled = active || state.actionBusy;
  if (['RUNNING', 'WAITING'].includes(state.run?.status)) $('run-mode').value = state.run.mode;
  document.querySelectorAll('[data-manual-mutation]').forEach(item => {
    item.disabled = state.actionBusy || active || item.dataset.unavailable === 'true';
  });
  $('next-step').disabled = state.actionBusy || state.run?.status !== 'WAITING';
  $('cancel-run').disabled = state.actionBusy || !['RUNNING', 'WAITING'].includes(state.run?.status);
}
async function mutate(path, body, target, success) {
  if (state.actionBusy) return;
  state.actionBusy = true; disableMutations(); notice(target, 'Operación en curso…');
  try { const result = await api(path, body); notice(target, 'Operación confirmada.'); await success?.(result); }
  catch (error) { notice(target, error.message, true); }
  finally { state.actionBusy = false; disableMutations(); refresh(true); }
}

async function loadCatalog() {
  try {
    state.catalog = await api('/catalog');
    const pending = state.catalog.filter(entry => !entry.available).map(entry => {
      const section = element('div', '', 'pending-entry');
      section.append(element('strong', entry.title), element('span', 'Pendiente de ' + entry.dependency, 'pending-label'), element('p', entry.objective));
      return section;
    });
    $('pending-scenarios').replaceChildren(...pending);
    if (state.run) renderRun(state.run); else renderGuided();
  } catch (error) { notice('scenario-notice', error.message, true); }
}
const checkLabels = { PASSED: 'Verificado', FAILED: 'Falló', WAITING: 'Esperando condición', NOT_OBSERVED: 'Sin verificar', INCOMPLETE: 'Sin completar' };
const scenarioLabels = { ...labels, PENDING: 'Sin iniciar', NOT_RUN: 'No ejecutado' };
function scenarioHash() { return '#scenarios/' + (state.runId || '') + '/' + state.scenarioId; }
function selectScenario(id) {
  state.scenarioId = id; state.explicitScenario = true;
  state.evidenceSelection = null; state.snapshotSelection = {}; state.currentOrderReads = {};
  history.replaceState(null, '', scenarioHash()); renderGuided();
}
function scenarioTitle(id) { return state.catalog?.find(entry => entry.id === id)?.title || 'Escenario'; }
function startRun(scenario) {
  mutate('/runs', { scenario, mode: $('run-mode').value }, 'scenario-notice', result => {
    state.runId = result.id; state.active = result.id; state.run = result;
    state.evidenceSelection = null; state.snapshotSelection = {}; state.currentOrderReads = {};
    state.scenarioId = result.scenarioIds[0]; state.explicitScenario = true;
    show('scenarios'); renderRun(result);
    notice('scenario-notice', 'La prueba continúa en el servidor. Las observaciones se completan a medida que avanza.');
  });
}
function recordTable(records) {
  if (!records.length) return element('p', 'Sin registros en el rango leído. Podés publicar un ER o ejecutar un escenario.', 'help');
  return table(['Topic', 'Partición / offset', 'Key', 'Transacción', 'Detalle'], records.map(record => [
    record.topic, `${record.partition} / ${record.offset}`, element('span', record.key ?? '(sin key)', 'mono'),
    record.transaction === 'ABORTED' ? 'Abortada; no visible read_committed' : record.transaction === 'COMMITTED' ? 'Confirmada' : 'Registro visible',
    button('Inspeccionar', () => selectRecord(record)),
  ]));
}
function renderRun(run) {
  if (state.run?.id !== run.id && !state.explicitScenario) state.scenarioId = run.scenarioIds[0];
  state.run = run; $('run-overview').hidden = run.status === 'PASSED' && run.scenarioIds.length === 1;
  if (['PASSED', 'FAILED', 'CANCELLED'].includes(run.status) && !state.actionBusy && !state.runReadFailed) notice('scenario-notice', '');
  $('run-heading').textContent = run.scenarioIds.length > 1 ? `Recorrido de ${run.scenarioIds.length} pruebas` : 'Prueba: ' + scenarioTitle(run.scenarioIds[0]);
  $('run-status').textContent = run.scenarioIds.length > 1
    ? `${labels[run.status] || run.status}. ${run.passedScenarios} de ${run.scenarioIds.length} pruebas aprobadas. ${run.excludedPending} pendientes no ejecutadas.`
    : `${labels[run.status] || run.status}.`;
  $('run-status').className = run.status === 'PASSED' ? 'passed status' : run.status === 'FAILED' ? 'failed status' : 'status';
  $('run-error').textContent = run.error || '';
  const next = run.steps.find(step => ['PENDING', 'RUNNING'].includes(step.status));
  $('next-step-purpose').textContent = next ? `${run.status === 'WAITING' ? 'Al continuar' : 'Ahora'}, en «${scenarioTitle(next.scenarioId)}»: ${next.title}. ${next.expected}.` : '';
  $('next-step').hidden = run.mode !== 'step';
  $('cancel-run').hidden = !['RUNNING', 'WAITING'].includes(run.status);
  const overview = element('div', '', 'batch-buttons');
  if (run.scenarioIds.length > 1) for (const result of run.scenarioResults) {
    const item = button(scenarioTitle(result.scenarioId) + ': ' + scenarioLabels[result.status], () => selectScenario(result.scenarioId));
    item.id = 'batch-' + result.scenarioId;
    item.setAttribute('aria-label', 'Ver resultado de ' + scenarioTitle(result.scenarioId) + ': ' + scenarioLabels[result.status]);
    overview.append(item);
  }
  replaceStable('batch-results', JSON.stringify(run.scenarioResults.map(result => [result.scenarioId, result.status])), overview);
  renderGuided();
}
function renderGuided() {
  if (!state.catalog) return;
  const entries = state.catalog.filter(entry => entry.available);
  let entry = entries.find(item => item.id === state.scenarioId);
  if (!entry) { entry = entries[0]; state.scenarioId = entry.id; }
  const results = state.run?.scenarioResults || [];
  const selected = results.find(item => item.scenarioId === entry.id);
  const picker = element('div', '', 'scenario-options');
  entries.forEach((item, index) => {
    const result = results.find(value => value.scenarioId === item.id);
    const action = button('', () => selectScenario(item.id), 'scenario-option');
    action.setAttribute('aria-pressed', String(item.id === entry.id));
    action.setAttribute('aria-label', 'Seleccionar ' + item.title);
    const status = element('span', result ? scenarioLabels[result.status] : 'Por explorar', 'scenario-option-status');
    status.id = 'status-' + item.id; action.setAttribute('aria-describedby', status.id);
    action.append(element('span', String(index + 1), 'scenario-number'), element('span', item.title, 'scenario-option-title'), status);
    picker.append(action);
  });
  replaceStable('catalog', JSON.stringify([entry.id, results.map(item => [item.scenarioId, item.status])]), picker);
  $('scenario-position').textContent = 'Escenario ' + (entries.indexOf(entry) + 1) + ' de ' + entries.length;
  $('scenario-title').textContent = entry.title;
  const brief = element('div');
  brief.append(element('h4', 'Qué demuestra'), element('p', entry.explanation), element('h4', 'Qué vamos a hacer'));
  const actions = element('ol', '', 'planned-actions');
  entry.actions.forEach(text => actions.append(element('li', text)));
  brief.append(actions, element('h4', 'Qué debería pasar'), element('p', entry.expected, 'expected-outcome'));
  const conditions = element('details'); conditions.append(element('summary', 'Precondiciones técnicas'), element('p', entry.preconditions.join('. ') + '.'));
  brief.append(conditions); replaceStable('scenario-brief', entry.id, brief);
  const attempted = selected && !['PENDING', 'NOT_RUN'].includes(selected.status);
  const guideKey = entry.id + ':' + Boolean(attempted);
  if ($('scenario-guide').dataset.state !== guideKey) {
    $('scenario-guide').open = !attempted; $('scenario-guide').dataset.state = guideKey;
  }
  $('run-panel').hidden = !selected;
  const nextIndex = entries.indexOf(entry) + 1;
  $('next-scenario').hidden = !selected || !['PASSED', 'FAILED', 'CANCELLED'].includes(selected.status) || nextIndex >= entries.length;
  $('next-scenario').disabled = state.actionBusy || hasActiveRun();
  if (selected) renderScenarioResult(selected);
  disableMutations();
}
const sqlLabels = {
  NOT_QUERIED: 'Sin consulta SQL', NO_RESULT: 'Sin resultado registrado',
  RESULT_FOUND: 'Resultado SQL observado', QUERY_FAILED: 'Falló la consulta SQL',
};
function displayQuantity(value) {
  const text = String(value ?? '—');
  if (!/^[-]?\d+(\.\d+)?$/.test(text)) return text;
  return text.replace(/(\.\d*?[1-9])0+$/, '$1').replace(/\.0+$/, '');
}
function decimalDifference(total, executed) {
  const left = String(total), right = String(executed);
  const scale = Math.max(left.split('.')[1]?.length || 0, right.split('.')[1]?.length || 0);
  const units = value => {
    const [whole, fraction = ''] = value.split('.');
    return BigInt(whole + fraction.padEnd(scale, '0'));
  };
  const difference = units(left) - units(right);
  const negative = difference < 0n;
  const digits = (negative ? -difference : difference).toString().padStart(scale + 1, '0');
  const formatted = scale ? `${digits.slice(0, -scale)}.${digits.slice(-scale)}` : digits;
  return displayQuantity((negative ? '-' : '') + formatted);
}
function latestSql(message) {
  return message.sqlObservations?.at(-1) || null;
}
function messageRow(message) {
  const report = message.report || {};
  const observation = latestSql(message);
  const sqlResult = observation?.result;
  const id = message.id;
  const selected = state.evidenceSelection?.messageId === id;
  const status = message.transaction === 'ABORTED' ? 'Envío abortado' : (sqlResult?.outcome || sqlLabels[message.sqlObservationStatus] || 'Sin observación');
  const choose = button(selected ? 'Seleccionado' : 'Ver mensaje', () => {
    const ledgerId = sqlResult?.ledgerId || null;
    state.evidenceSelection = { messageId: id, orderId: report.numericOrderId || message.key, ledgerId };
    renderScenarioResult(state.run.scenarioResults.find(item => item.scenarioId === state.scenarioId));
  });
  choose.setAttribute('aria-label', `Inspeccionar ${id}`);
  choose.setAttribute('aria-pressed', String(selected));
  return [report.numericOrderId || message.key || '—', report.fixId || '—', report.status || 'ER inválido', `${message.source.partition} / ${message.source.offset}`, status, sqlResult?.ledgerId || '—', choose];
}
function quantitySummary(snapshot) {
  const list = element('dl', '', 'quantity-summary');
  for (const [term, value] of [
    ['Solicitada', snapshot.nominalAmounts],
    ['Ejecutada', snapshot.accumulativeNominalAmount],
    ['Pendiente de ejecutar', snapshot.leavesNominalAmount],
  ]) {
    list.append(element('dt', term), element('dd', displayQuantity(value)));
  }
  return list;
}
function renderOrderCards(result) {
  const evidence = result.evidence || {};
  const snapshots = evidence.orderSnapshots || [];
  const cards = element('div', '', 'order-snapshots');
  for (const order of result.orders) {
    const history = snapshots.filter(item => item.numericOrderId === order.numericOrderId);
    const card = element('article', '', 'order-card');
    const heading = element('div', '', 'order-card-heading');
    heading.append(element('h5', `${order.label} · ${order.numericOrderId}`));
    const selectedId = state.snapshotSelection[order.numericOrderId] || history.at(-1)?.observedAt;
    let captured = history.find(item => item.observedAt === selectedId) || history.at(-1);
    const snapshot = captured?.snapshot;
    if (snapshot) {
      heading.append(element('span', snapshot.status, `status-pill ${snapshot.status === 'FILLED' ? 'passed' : snapshot.status === 'CANCELLED' ? 'cancelled' : ''}`));
      card.append(heading);
      card.append(element('p', `${captured.checkId} · Capturado ${time(captured.observedAt)} · PostgreSQL`, 'help'));
      card.append(quantitySummary(snapshot));
      const count = element('p', `ER aplicados: ${snapshot.appliedExecutionCount} · Entradas de ledger: ${snapshot.ledger.length}`, 'order-counts');
      card.append(count);
      if (snapshot.status === 'CANCELLED') {
        card.append(element('p', `Cancelada con ${displayQuantity(decimalDifference(snapshot.nominalAmounts, snapshot.accumulativeNominalAmount))} unidades sin ejecutar.`, 'cancelled-explanation'));
      }
      const versions = history.length > 1 ? element('details', '', 'snapshot-history') : null;
      if (versions) {
        versions.dataset.preserveKey = `snapshots:${order.numericOrderId}`;
        versions.append(element('summary', `Ver otras observaciones (${history.length})`));
        for (const version of history) {
          const label = `${version.checkId}: ${version.snapshot.status}, ejecutada ${displayQuantity(version.snapshot.accumulativeNominalAmount)} · ${time(version.observedAt)}`;
          const select = button(label, () => {
            state.snapshotSelection[order.numericOrderId] = version.observedAt;
            renderScenarioResult(state.run.scenarioResults.find(item => item.scenarioId === state.scenarioId));
          }, 'snapshot-choice');
          select.setAttribute('aria-pressed', String(version.observedAt === captured.observedAt));
          versions.append(select);
        }
        card.append(versions);
      }
      const current = state.currentOrderReads[order.numericOrderId];
      card.append(button('Consultar estado actual', () => readCurrentOrder(order.numericOrderId), 'secondary-action'));
      if (current) {
        const currentSection = element('section', '', 'current-order-state');
        currentSection.append(element('h6', 'Estado actual, consultado por separado'));
        currentSection.append(element('p', `${current.status} · PostgreSQL · ${time(current.observedAt)}`, 'help'));
        currentSection.append(quantitySummary(current));
        card.append(currentSection);
      }
      const selectedLedger = state.evidenceSelection?.orderId === order.numericOrderId ? state.evidenceSelection.ledgerId : null;
      const ledger = snapshot.ledger || [];
      if (ledger.length) {
        card.append(element('h6', 'Ledger de esta observación'));
        const rows = ledger.map(entry => {
          const select = button(selectedLedger === entry.ledgerId ? 'Seleccionado' : entry.ledgerId, () => {
            const related = (evidence.messages || []).find(message => message.sqlObservations?.some(observation => observation.result?.ledgerId === entry.ledgerId));
            state.evidenceSelection = { messageId: related?.id || null, orderId: order.numericOrderId, ledgerId: entry.ledgerId };
            renderScenarioResult(state.run.scenarioResults.find(item => item.scenarioId === state.scenarioId));
          }, 'link mono');
          select.setAttribute('aria-label', `Seleccionar ledger ${entry.ledgerId} de ${order.numericOrderId}`);
          select.setAttribute('aria-pressed', String(selectedLedger === entry.ledgerId));
          const detail = element('details'); detail.dataset.preserveKey = `ledger:${order.numericOrderId}:${entry.ledgerId}`;
          detail.append(element('summary', 'Ver ER original'));
          detail.append(element('pre', entry.payload));
          return [select, entry.fixId || '—', entry.status || '—', displayQuantity(entry.accumulativeNominalAmount), detail];
        });
        const ledgerTable = element('div', '', 'table-wrap');
        ledgerTable.append(table(['ledgerId', 'fixId', 'Estado del ER', 'Acumulado informado', 'Detalle'], rows));
        card.append(ledgerTable);
      }
    } else {
      card.append(heading, element('p', 'Todavía no hay un snapshot de esta orden.', 'help'));
    }
    cards.append(card);
  }
  return cards;
}
async function readCurrentOrder(id) {
  const runId = state.runId, scenarioId = state.scenarioId;
  try {
    const current = await api('/orders/' + encodeURIComponent(id));
    if (runId !== state.runId || scenarioId !== state.scenarioId) return;
    state.currentOrderReads[id] = current;
    renderScenarioResult(state.run.scenarioResults.find(item => item.scenarioId === scenarioId));
  } catch (error) {
    if (runId === state.runId && scenarioId === state.scenarioId) notice('scenario-notice', `No se pudo consultar el estado actual: ${error.message}. Las observaciones capturadas siguen disponibles.`, true);
  }
}
function renderMessageInspector(message) {
  const panel = $('run-message-detail');
  panel.hidden = !message;
  if (!message) return;
  const report = message.report || {}, observation = latestSql(message), sqlResult = observation?.result;
  const content = element('div', '', 'message-inspector-grid');
  const facts = table(['Dato', 'Valor'], [
    ['Posición de entrada', `${message.source.topic} / ${message.source.partition} / ${message.source.offset}`],
    ['Paso', message.stepId], ['Envío del productor', message.transaction === 'ABORTED' ? 'Abortado por el productor' : message.transaction === 'COMMITTED' ? 'Confirmado en transacción' : 'Confirmado por Kafka'],
    ['Estado informado por el ER', report.status || 'No disponible'],
    ['Resultado SQL de la posición', sqlLabels[message.sqlObservationStatus] || 'Sin observación'],
    ['Resultado de procesamiento SQL', sqlResult?.outcome || 'Sin resultado'],
    ['Snapshot histórico', sqlResult?.status || 'Sin resultado registrado'],
    ['Estado actual de la orden', state.currentOrderReads[report.numericOrderId]?.status || 'No consultado'],
    ['ledgerId relacionado', sqlResult?.ledgerId || 'Sin referencia'],
    ['Observado en', time(observation?.observedAt || message.observedAt)],
  ]);
  content.append(facts);
  if (message.transaction === 'ABORTED') content.append(element('p', 'El productor abortó esta transacción. No es un registro visible read_committed y no se cuenta como ER aplicado.', 'notice'));
  const details = element('details'); details.dataset.preserveKey = `message-payload:${message.id}`; details.append(element('summary', 'Ver payload original del ER'), element('pre', message.payload ?? '(payload nulo)'));
  content.append(details);
  if (observation?.result) {
    const resultDetails = element('details'); resultDetails.dataset.preserveKey = `sql-result:${message.id}`; resultDetails.append(element('summary', 'Ver snapshot SQL completo'), element('pre', JSON.stringify(observation.result, null, 2)));
    content.append(resultDetails);
  }
  replaceStable('run-message-inspector', JSON.stringify([message.id, message.sqlObservations]), content);
}
function renderOutputs(result) {
  const outputs = result.evidence?.outputs || [];
  const root = element('div');
  if (!outputs.length) root.append(element('p', result.status === 'RUNNING' ? 'Las salidas se mostrarán cuando el escenario las observe.' : 'Este escenario no registró salidas observadas.', 'help'));
  else {
    const rows = outputs.map(item => {
      const payload = element('details'); payload.append(element('summary', 'Ver mensaje completo'), element('pre', item.payload));
      payload.dataset.preserveKey = `output:${item.id}`;
      const start = item.startCursors?.[item.source.partition] ?? '—';
      const end = item.endOffsets?.[item.source.partition] ?? '—';
      return [item.type === 'REJECTION' ? 'Aviso de rechazo DLQ' : 'Settlement', `${item.source.topic} / ${item.source.partition} / ${item.source.offset}`, item.inputPosition || '—', `${item.readSource}: ${start}–${end}; ${time(item.observedAt)}`, payload];
    });
    root.append(table(['Tipo de salida', 'Posición propia', 'Posición de entrada', 'Lectura', 'Detalle'], rows));
  }
  const checkKeysByScenario = {
    'quantity-snapshots': { clock: ['orderId', 'transactionTimes', 'amounts'] },
    'restart-persistence': { preserved: ['before', 'after'] },
    'terminal-duplicates': { history: ['original', 'lateDuplicate'] },
    'committed-input-only': { visibility: ['visibleOffsets', 'messages'], 'no-result': ['position', 'successor'] },
    'settlement-once': { settlement: ['messages', 'result', 'startCursors', 'visibility'], recovery: ['barrier', 'target', 'sqlBefore', 'result', 'successor', 'output', 'order', 'recoveredGroup'] },
    'commit-boundary-recovery': {
      'sql-open': ['barrier', 'target', 'sqlBefore', 'takeover', 'obsoleteAttempt', 'result', 'successor', 'output', 'order', 'recoveredGroup'],
      'sql-committed': ['barrier', 'target', 'sqlBefore', 'takeover', 'obsoleteAttempt', 'result', 'successor', 'output', 'order', 'recoveredGroup'],
      'send-ack': ['barrier', 'target', 'sqlBefore', 'kill', 'takeover', 'result', 'successor', 'output', 'order', 'recoveredGroup'],
      rebalance: ['barrier', 'target', 'sqlBefore', 'takeover', 'obsoleteAttempt', 'result', 'successor', 'output', 'order', 'recoveredGroup'],
      'both-down': ['barrier', 'target', 'sqlBefore', 'kill', 'killOther', 'result', 'successor', 'output', 'order', 'recoveredGroup'],
      restored: ['group', 'instances'],
    },
    'poison-recovery': {
      paused: ['original', 'quarantineState', 'dlqNotified', 'order'],
      recovered: ['order', 'correction', 'original'],
      outputs: ['settlements', 'source'],
      restored: ['group', 'instances'],
    },
  };
  const referenceLabels = {
    barrier: 'Barrera de recuperación', target: 'ER de entrada', sqlBefore: 'Resultado SQL antes de recuperar',
    takeover: 'Toma de partición', obsoleteAttempt: 'Intento anterior rechazado', result: 'Resultado SQL recuperado',
    successor: 'Mensaje sucesor', output: 'Salida verificada', order: 'Snapshot de la orden',
    recoveredGroup: 'Grupo de consumidores recuperado', original: 'ER original', correction: 'Auditoría de corrección',
    quarantineState: 'Estado de cuarentena', dlqNotified: 'Marcador de aviso en PostgreSQL',
    settlements: 'Settlements observados', source: 'Fuente de la salida', group: 'Grupo Kafka', instances: 'Instancias',
    before: 'Capturas anteriores al reinicio', after: 'Capturas posteriores al reinicio',
    transactionTimes: 'Horas de origen', amounts: 'Acumulados observados', visibleOffsets: 'Offsets visibles read_committed',
    position: 'Posición sin resultado', messages: 'Mensajes relacionados', lateDuplicate: 'NEW republicado',
  };
  const checkKeys = checkKeysByScenario[result.scenarioId] || {};
  const evidenceChecks = result.checks.filter(check => checkKeys[check.id] && check.references);
  if (evidenceChecks.length) {
    root.append(element('h5', 'Incidentes y recuperación observados'));
    for (const check of evidenceChecks) {
      const detail = element('details'); detail.dataset.checkId = check.id;
      detail.dataset.preserveKey = `check:${check.id}`;
      detail.append(element('summary', `${check.label}: ${checkLabels[check.status] || check.status}`));
      detail.append(element('p', check.observed || 'Sin observación completa.', 'help'));
      const rows = checkKeys[check.id]
        .filter(key => Object.hasOwn(check.references, key))
        .map(key => {
          const value = check.references[key];
          if (value && typeof value === 'object') {
            const structured = element('div');
            if (!Array.isArray(value)) {
              const fields = Object.entries(value).filter(([field, item]) => item != null && typeof item !== 'object' && !['payload', 'effectiveReport'].includes(field));
              if (fields.length) structured.append(table(['Campo', 'Valor'], fields));
              if (value.source?.topic) structured.append(element('p', `Posición original: ${value.source.topic} / ${value.source.partition} / ${value.source.offset}`));
              for (const field of ['payload', 'effectiveReport']) {
                if (typeof value[field] === 'string') {
                  const payload = element('details');
                  payload.append(element('summary', field === 'payload' ? 'Payload original' : 'ER efectivo de la corrección'), element('pre', value[field]));
                  structured.append(payload);
                }
              }
            }
            const raw = element('details'); raw.dataset.preserveKey = `reference:${check.id}:${key}`;
            raw.append(element('summary', 'Abrir dato completo'), element('pre', JSON.stringify(value, null, 2)));
            structured.append(raw);
            return [referenceLabels[key] || key, structured];
          }
          return [referenceLabels[key] || key, String(value)];
        });
      detail.append(table(['Dato observado', 'Valor'], rows));
      root.append(detail);
    }
  }
  replaceStable('run-outputs', JSON.stringify([outputs, evidenceChecks]), root);
}
function renderScenarioResult(result) {
  if (!result || !state.run) return;
  const steps = state.run.steps.filter(step => step.scenarioId === result.scenarioId);
  const evidence = result.evidence || { messages: [], orderSnapshots: [], outputs: [] };
  const messages = evidence.messages || [];
  $('scenario-outcome').className = 'scenario-outcome ' + (result.status === 'PASSED' ? 'passed' : result.status === 'FAILED' ? 'failed' : '');
  const outcome = element('div'); outcome.append(element('h4', 'Resultado: ' + scenarioLabels[result.status]), element('p', result.conclusion));
  replaceStable('scenario-outcome', JSON.stringify([result.status, result.conclusion]), outcome);
  const rows = result.checks.map(check => {
    const observed = element('div'); observed.append(element('p', check.observed ?? 'Todavía no hay una observación para esta comprobación.'));
    if (check.diagnostic && check.status !== 'PASSED') observed.append(element('p', 'Diagnóstico: ' + check.diagnostic, 'error'));
    if (check.observedAt) observed.append(element('small', time(check.observedAt), 'help'));
    const references = check.references && Object.keys(check.references).length ? element('details') : null;
    if (references) { references.append(element('summary', 'Datos estructurados de esta comprobación'), element('pre', JSON.stringify(check.references, null, 2))); observed.append(references); }
    return [check.label, check.expected, observed, element('span', checkLabels[check.status], 'check-status ' + (check.status === 'PASSED' ? 'passed' : check.status === 'FAILED' ? 'failed' : ''))];
  });
  replaceStable('scenario-checks', JSON.stringify(result.checks), table(['Verificación', 'Esperado', 'Observado', 'Resultado'], rows));
  $('run-message-count').textContent = `${messages.length} envíos capturados`;
  const messageTable = messages.length ? table(
    ['Orden', 'fixId', 'Estado del ER', 'Partición / offset', 'Resultado SQL', 'ledgerId', 'Acción'],
    messages.map(messageRow)) : element('p', result.status === 'RUNNING' ? 'Todavía no se enviaron mensajes.' : 'Esta ejecución terminó sin mensajes capturados.', 'help');
  replaceStable('run-messages', JSON.stringify([messages, result.status, state.evidenceSelection?.messageId]), messageTable);
  const selectedMessage = messages.find(message => message.id === state.evidenceSelection?.messageId);
  renderMessageInspector(selectedMessage);
  replaceStable('run-orders', JSON.stringify([evidence.orderSnapshots, state.snapshotSelection, state.evidenceSelection, state.currentOrderReads]), renderOrderCards(result));
  renderOutputs(result);
  const open = new Set([...$('run-steps').querySelectorAll('details[open]')].map(item => item.dataset.stepId));
  const stepList = document.createDocumentFragment();
  for (const step of steps) {
    const li = element('li'), details = element('details'); details.dataset.stepId = step.id; details.dataset.status = step.status;
    details.open = open.has(step.id) || ['RUNNING', 'PENDING', 'FAILED'].includes(step.status);
    const heading = element('summary', `${step.title}: ${labels[step.status] || step.status}`); heading.id = step.id + '-summary';
    details.append(heading, element('p', step.expected));
    if (step.observedAt) details.append(element('p', 'Comprobado a las ' + time(step.observedAt), 'help'));
    if (step.error) details.append(element('p', step.error, 'error'));
    li.append(details); stepList.append(li);
  }
  replaceStable('run-steps', JSON.stringify([result.scenarioId, steps]), stepList);
  $('run-id').textContent = 'Ejecución: ' + state.run.id;
  $('run-json').textContent = JSON.stringify({ scenario: result, steps, messages }, null, 2);
}
async function refreshRun() {
  if (!state.runId) return;
  const requestedId = state.runId;
  try {
    const run = await api('/runs/' + encodeURIComponent(requestedId));
    if (requestedId !== state.runId) return;
    renderRun(run);
    if (state.runReadFailed) notice('scenario-notice', '');
    state.runReadFailed = false;
  } catch (error) {
    if (requestedId !== state.runId) return;
    state.runReadFailed = true; notice('scenario-notice', error.message, true);
  }
}

async function loadMessages() {
  const topic = $('topic').value, partition = $('partition').value, cursor = state.messageCursor;
  try {
    const page = await api(`/messages?topic=${encodeURIComponent(topic)}&partition=${partition}&cursor=${cursor}&limit=50`);
    if (topic !== $('topic').value || partition !== $('partition').value || cursor !== state.messageCursor) return;
    state.records = page.records; state.messageNext = page.hasMore ? page.nextCursor : null; state.messageEnd = page.endOffset;
    replaceStable('message-table', JSON.stringify(page.records), recordTable(page.records));
    $('message-next').disabled = !state.messageNext;
    notice('message-notice', `Leído ${topic}, partición ${partition}, desde ${page.startCursor} hasta ${page.nextCursor}. ${page.records.length} registros visibles. ${time(page.observedAt)}.`);
  } catch (error) { notice('message-notice', `${error.message}. Los registros anteriores, si hay, están desactualizados.`, true); }
}
function selectRecord(record) {
  state.selected = record; $('topic').value = record.topic; $('partition').value = record.partition;
  state.messageCursor = record.offset; show('messages'); $('message-detail').hidden = false;
  $('record-position').textContent = `${record.topic} / partición ${record.partition} / offset ${record.offset} / key ${record.key ?? '(sin key)'}`;
  $('record-payload').textContent = record.payload ?? '(payload nulo)';
  $('record-order').disabled = !/^[1-9][0-9]*$/.test(record.key ?? '');
  $('record-republish').dataset.unavailable = String(record.topic !== 'execution-reports' || record.payload === null || record.transaction === 'ABORTED');
  $('record-result').replaceChildren(); signatures.delete('record-result'); notice('result-notice', 'Consultando resultado SQL…'); loadResult(); disableMutations();
  requestAnimationFrame(() => $('message-detail').scrollIntoView({ block: 'start', behavior: 'instant' }));
}
async function loadResult() {
  const record = state.selected; if (!record) return;
  try {
    const data = await api(`/results?topic=${encodeURIComponent(record.topic)}&partition=${record.partition}&offset=${record.offset}`);
    if (record !== state.selected) return;
    if (!data.result) { notice('result-notice', 'Sin resultado registrado. La ausencia de fila no distingue entre no intentado y fallo.'); $('record-result').replaceChildren(); return; }
    const result = data.result;
    notice('result-notice', `${result.outcome}. Snapshot histórico de esta posición; puede diferir del estado actual. ${time(data.observedAt)}.`);
    const link = button(`Ver ledger ${result.ledgerId} de la orden ${result.numericOrderId}`, () => viewOrder(result.numericOrderId, result.ledgerId), 'link');
    const content = element('div'); content.append(link, table(['Campo', 'Valor'], ['outcome', 'status', 'appliedExecutionCount', 'accumulativeNominalAmount', 'leavesNominalAmount', 'fixId', 'ledgerId'].map(field => [field, result[field]])));
    replaceStable('record-result', JSON.stringify(result), content);
  } catch (error) { notice('result-notice', `${error.message}. El resultado anterior, si hay, está desactualizado.`, true); }
}

async function loadOrders() {
  const cursor = state.orderCursor;
  try {
    const data = await api('/orders?limit=50' + (cursor ? '&cursor=' + cursor : ''));
    if (cursor !== state.orderCursor) return;
    state.orderNext = data.nextCursor; $('order-next').disabled = !state.orderNext;
    const rows = data.orders.map(order => [button(order.numericOrderId, () => viewOrder(order.numericOrderId), 'link mono'), order.status, order.appliedExecutionCount, order.accumulativeNominalAmount, order.leavesNominalAmount]);
    replaceStable('order-table', JSON.stringify(data.orders), rows.length ? table(['numericOrderId', 'Estado', 'ER aplicados', 'Acumulado', 'Restante'], rows) : element('p', 'Todavía no hay órdenes. Ejecutá un escenario desde la primera vista.', 'help'));
    if (!state.orderId) notice('order-notice', `PostgreSQL observado a las ${time(data.observedAt)}.`);
  } catch (error) { notice('order-notice', `${error.message}. El listado anterior está desactualizado.`, true); }
}
function viewOrder(id, ledger = null) {
  if (state.orderId !== id) { state.order = null; $('order-detail').hidden = true; }
  state.orderId = id; state.highlight = ledger; $('order-id').value = id; show('orders'); loadOrder();
}
async function loadOrder() {
  const id = state.orderId; if (!id) return;
  try {
    const data = await api('/orders/' + encodeURIComponent(id)); if (id !== state.orderId) return;
    state.order = data; $('order-detail').hidden = false; $('order-title').textContent = 'Orden ' + id;
    notice('order-notice', `Estado actual confirmado en PostgreSQL a las ${time(data.observedAt)}.`);
    const signature = JSON.stringify([data.status, data.appliedExecutionCount, data.accumulativeNominalAmount, data.leavesNominalAmount, data.ledger, state.highlight]);
    if (signatures.get('order-detail') !== signature) {
      $('order-state').replaceChildren(table(['Estado', 'ER aplicados', 'Total', 'Acumulado', 'Restante'], [[data.status, data.appliedExecutionCount, data.nominalAmounts, data.accumulativeNominalAmount, data.leavesNominalAmount]]));
      $('order-ledger').replaceChildren(...data.ledger.map(entry => {
        const detail = element('details'); detail.append(element('summary', 'Ledger ' + entry.ledgerId), element('pre', entry.payload));
        if (state.highlight === entry.ledgerId) detail.open = true; return detail;
      })); signatures.set('order-detail', signature);
    }
  } catch (error) { notice('order-notice', `${error.message}${state.order ? '. El detalle anterior está desactualizado.' : ''}`, true); }
}

function currentConsumer(instance) {
  const source = instance?.consumer;
  if (!source?.available || source.data?.statusCode !== 200) return 'Desconocido' + (source?.stale ? ' (última observación conservada)' : '');
  return source.data.data.running ? 'En ejecución' : 'Detenido';
}
function currentHealth(source) {
  return source?.available ? (source.data?.data?.status || 'Sin dato HTTP') : source?.stale ? 'Desconocido (dato anterior)' : 'Desconocido';
}
async function loadSystem() {
  try {
    const data = await api('/system'); state.system = data; state.active = data.activeRunId;
    if (state.active && !state.runId) {
      state.runId = state.active;
      if (state.view === 'scenarios') history.replaceState(null, '', scenarioHash());
      await refreshRun();
    }
    $('summary').textContent = `${data.instances.map(instance => `${instance.service}: HTTP ${currentHealth(instance.liveness)}, consumidor ${currentConsumer(instance)}`).join(' | ')} | SQL: ${data.database.available ? 'disponible' : 'sin observación'} | Kafka: ${data.kafka.available ? 'grupo observado' : 'sin observación'} | ${time(data.observedAt)}${state.paused ? ' · Refresco pausado' : ''}`;
    if (state.view === 'system') renderSystem(data); disableMutations();
  } catch (error) { $('summary').textContent = `${error.message}. Última observación desactualizada.`; notice('system-notice', error.message, true); }
}
function renderSystem(data) {
  const rows = ['app-1', 'app-2', 'postgres', 'kafka'].map(service => {
    const instance = data.instances.find(item => item.service === service), actions = element('div', '', 'actions');
    for (const [action, title] of [['start', 'Iniciar'], ['stop', 'Detener'], ['restart', 'Reiniciar'], ...(service.startsWith('app-') ? [['kill', 'Terminar abruptamente']] : [])]) {
      const control = button(title, () => mutate(`/services/${service}/${action}`, {}, 'control-result', result => notice('control-result', `${service}: ${title}. Estado Docker observado: ${result.containerState}.`)), action === 'kill' ? 'danger' : '');
      control.setAttribute('aria-label', title + ' ' + service); control.dataset.manualMutation = 'true'; actions.append(control);
    }
    const consumer = instance?.consumer?.available ? instance.consumer.data?.data : null;
    return [service, instance ? currentHealth(instance.liveness) : (service === 'postgres' ? (data.database.available ? 'SQL disponible' : 'Sin observación SQL') : (data.kafka.available ? 'Grupo disponible' : 'Sin observación del grupo')), instance ? currentHealth(instance.readiness) : '—', instance ? currentConsumer(instance) : '—', consumer ? `${consumer.clientId || ''}: ${(consumer.assignments || []).map(p => p.topic + '/' + p.partition).join(', ') || 'sin asignación'}` : '—', actions];
  });
  const signature = JSON.stringify([data.instances.map(i => [i.service, currentHealth(i.liveness), currentHealth(i.readiness), currentConsumer(i), i.consumer?.data?.data]), data.database.available, data.kafka.available]);
  replaceStable('service-table', signature, table(['Servicio', 'Proceso / conectividad', 'Readiness HTTP', 'Consumidor', 'Asignación actual', 'Controles locales'], rows));
  const group = data.kafka.data;
  if (group) {
    replaceStable('group-table', JSON.stringify(group.members), table(['ClientId', 'ConsumerId actual', 'Particiones'], group.members.map(member => [member.clientId, member.consumerId, member.assignments.map(p => `${p.topic}/${p.partition}`).join(', ')])));
    replaceStable('offset-table', JSON.stringify(group.partitions), table(['Topic / partición', 'Offset confirmado', 'Fin del log', 'Lag de posiciones'], [...group.partitions].sort((a, b) => Number(a.partition) - Number(b.partition)).map(p => [`${p.topic}/${p.partition}`, p.committedOffset, p.endOffset, p.lag])));
  }
  notice('system-notice', `${data.kafka.stale ? 'Grupo Kafka desactualizado. ' : ''}${data.database.stale ? 'Consulta SQL desactualizada. ' : ''}Observación ${time(data.observedAt)}. Los controles operan sólo sobre este proyecto Compose.`);
  disableMutations();
}

async function refresh(force = false) {
  if (state.refreshing || (!force && (state.paused || document.hidden))) return;
  state.refreshing = true;
  try {
    const reads = [loadSystem()];
    if (!state.catalog) reads.push(loadCatalog());
    if (state.view === 'messages') reads.push(loadMessages(), loadResult());
    if (state.view === 'orders') reads.push(loadOrders(), loadOrder());
    if (state.runId) reads.push(refreshRun());
    await Promise.allSettled(reads);
  } finally { state.refreshing = false; }
}
function example(status) {
  if (status === 'NEW') $('report-key').value = (BigInt(Date.now()) * 1000000n + BigInt(crypto.getRandomValues(new Uint32Array(1))[0] % 1000000)).toString();
  const key = $('report-key').value.trim();
  if (!/^[1-9][0-9]*$/.test(key) || BigInt(key) > 9223372036854775807n) { notice('publish-notice', 'Ingresá una key positiva de 64 bits.', true); return; }
  $('report-payload').value = `{
  "numericOrderId": ${key},
  "fixId": ${status === 'NEW' ? '1' : '2'},
  "status": "${status}",
  "nominalAmounts": 10,
  "accumulativeNominalAmount": ${status === 'NEW' ? '0' : '10'},
  "leavesNominalAmount": ${status === 'NEW' ? '10' : '0'},
  "transactionTime": "2026-07-20T18:08:52.129",
  "ticker": "VSCPC"
}`;
}

document.querySelectorAll('[data-view]').forEach(item => item.addEventListener('click', () => show(item.dataset.view)));
$('refresh').addEventListener('click', () => refresh(true));
$('pause').addEventListener('click', () => { state.paused = !state.paused; $('pause').textContent = state.paused ? 'Reanudar refresco' : 'Pausar refresco'; $('pause').setAttribute('aria-pressed', String(state.paused)); if (!state.paused) refresh(true); });
$('run-all').addEventListener('click', () => startRun('available'));
$('run-selected').addEventListener('click', () => startRun(state.scenarioId));
$('next-scenario').addEventListener('click', () => {
  const entries = state.catalog.filter(entry => entry.available);
  const next = entries[entries.findIndex(entry => entry.id === state.scenarioId) + 1];
  if (next) { selectScenario(next.id); $('scenario-title').tabIndex = -1; $('scenario-title').focus({preventScroll:true}); }
});
$('next-step').addEventListener('click', () => mutate(`/runs/${state.runId}/next`, {}, 'scenario-notice', renderRun));
$('cancel-run').addEventListener('click', () => mutate(`/runs/${state.runId}/cancel`, {}, 'scenario-notice', renderRun));
for (const id of ['topic', 'partition']) $(id).addEventListener('change', () => { state.messageCursor = '0'; state.selected = null; $('message-detail').hidden = true; refresh(true); });
$('message-first').addEventListener('click', () => { state.messageCursor = '0'; loadMessages(); });
$('message-next').addEventListener('click', () => { if (state.messageNext) { state.messageCursor = state.messageNext; loadMessages(); } });
$('message-last').addEventListener('click', () => { const end = BigInt(state.messageEnd); state.messageCursor = (end > 50n ? end - 50n : 0n).toString(); loadMessages(); });
$('record-order').addEventListener('click', () => { if (state.selected) viewOrder(state.selected.key); });
$('record-republish').addEventListener('click', () => {
  const record = state.selected; if (!record) return;
  mutate('/messages/republication', { topic: record.topic, partition: Number(record.partition), offset: record.offset }, 'publish-notice', result => { selectRecord(result); notice('publish-notice', `Republicación confirmada por Kafka: ${result.partition}/${result.offset}. Consultando el resultado SQL.`); });
});
$('example-new').addEventListener('click', () => example('NEW')); $('example-filled').addEventListener('click', () => example('FILLED'));
$('publish-form').addEventListener('submit', event => { event.preventDefault(); mutate('/reports', { key: $('report-key').value.trim(), payload: $('report-payload').value }, 'publish-notice', record => { selectRecord(record); notice('publish-notice', `Kafka confirmó ${record.topic}/${record.partition}/${record.offset}. La aplicación SQL se consulta por separado.`); }); });
$('order-form').addEventListener('submit', event => { event.preventDefault(); viewOrder($('order-id').value.trim()); });
$('order-first').addEventListener('click', () => { state.orderCursor = null; loadOrders(); });
$('order-next').addEventListener('click', () => { state.orderCursor = state.orderNext; loadOrders(); });
$('load-logs').addEventListener('click', async () => {
  notice('log-notice', 'Consultando diagnóstico…');
  try { const data = await api('/instances/' + $('log-service').value + '/logs'); $('logs').textContent = data.text; notice('log-notice', `${data.source}; ${data.limit}; ${time(data.observedAt)}.`); }
  catch (error) { notice('log-notice', `${error.message}. El diagnóstico anterior está desactualizado.`, true); }
});
function readHash() {
  const [view, id, scenario] = location.hash.slice(1).split('/');
  if (view === 'content') return;
  if ((!view || view === 'scenarios') && !id) {
    state.runId = null; state.run = null; state.runReadFailed = false;
    state.evidenceSelection = null; state.snapshotSelection = {}; state.currentOrderReads = {};
    $('run-overview').hidden = true; notice('scenario-notice', '');
  }
  if (view === 'scenarios') {
    if (id && state.runId !== id) {
      state.runId = id; state.run = null; state.evidenceSelection = null; state.snapshotSelection = {}; state.currentOrderReads = {};
      $('run-overview').hidden = true;
    }
    if (scenario) {
      if (state.scenarioId !== scenario) { state.evidenceSelection = null; state.snapshotSelection = {}; state.currentOrderReads = {}; }
      state.scenarioId = scenario; state.explicitScenario = true;
    }
    else state.explicitScenario = false;
    renderGuided();
  }
  show(views.includes(view) ? view : 'scenarios', false);
}
window.addEventListener('hashchange', readHash); document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(true); });
example('NEW'); readHash(); loadCatalog(); setInterval(() => refresh(), 2000);
