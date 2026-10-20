const $ = id => document.getElementById(id);
const views = ['scenarios', 'messages', 'orders', 'system'];
const state = {
  view: 'scenarios', runId: null, run: null, runReadFailed: false, active: null, actionBusy: false,
  refreshing: false, paused: false, catalog: null, system: null,
  messageCursor: '0', messageNext: null, messageEnd: '0', records: [], selected: null,
  orderCursor: null, orderNext: null, orderId: null, order: null, highlight: null,
  scenarioId: 'interleaved-orders', explicitScenario: false,
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
  const focused = $(id).contains(document.activeElement) ? document.activeElement : null;
  const identity = focused ? { id: focused.id, label: focused.getAttribute('aria-label'), text: focused.textContent } : null;
  signatures.set(id, signature); $(id).replaceChildren(content);
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
    $(name).hidden = name !== view; $('tab-' + name).setAttribute('aria-selected', String(name === view));
    $('tab-' + name).tabIndex = name === view ? 0 : -1;
  }
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
  history.replaceState(null, '', scenarioHash()); renderGuided();
}
function scenarioTitle(id) { return state.catalog?.find(entry => entry.id === id)?.title || 'Escenario'; }
function startRun(scenario) {
  mutate('/runs', { scenario, mode: $('run-mode').value }, 'scenario-notice', result => {
    state.runId = result.id; state.active = result.id; state.run = result;
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
  $('run-heading').textContent = run.scenarioIds.length > 1 ? 'Recorrido de las cinco pruebas' : 'Prueba: ' + scenarioTitle(run.scenarioIds[0]);
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
function renderScenarioResult(result) {
  const steps = state.run.steps.filter(step => step.scenarioId === result.scenarioId);
  const messages = state.run.messages.filter(message => message.scenarioId === result.scenarioId);
  $('scenario-outcome').className = 'scenario-outcome ' + (result.status === 'PASSED' ? 'passed' : result.status === 'FAILED' ? 'failed' : '');
  const outcome = element('div'); outcome.append(element('h4', 'Resultado: ' + scenarioLabels[result.status]), element('p', result.conclusion));
  replaceStable('scenario-outcome', JSON.stringify([result.status, result.conclusion]), outcome);
  const rows = result.checks.map(check => {
    const observed = element('div'); observed.append(element('p', check.observed ?? 'Todavía no hay una observación para esta comprobación.'));
    if (check.diagnostic && check.status !== 'PASSED') observed.append(element('p', 'Diagnóstico: ' + check.diagnostic, 'error'));
    if (check.observedAt) observed.append(element('small', time(check.observedAt), 'help'));
    return [check.label, check.expected, observed, element('span', checkLabels[check.status], 'check-status ' + (check.status === 'PASSED' ? 'passed' : check.status === 'FAILED' ? 'failed' : ''))];
  });
  replaceStable('scenario-checks', JSON.stringify(result.checks), table(['Verificación', 'Esperado', 'Observado', 'Resultado'], rows));
  const stepList = document.createDocumentFragment();
  const open = new Set([...$('run-steps').querySelectorAll('details[open]')].filter(item => item.dataset.status === 'PASSED').map(item => item.dataset.stepId));
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
  const links = element('div', '', 'actions');
  for (const order of result.orders) links.append(button('Ver ' + order.label + ' · ' + order.numericOrderId, () => viewOrder(order.numericOrderId), 'link mono'));
  replaceStable('run-orders', JSON.stringify(result.orders), links);
  replaceStable('run-messages', JSON.stringify(messages), recordTable(messages));
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
document.querySelector('nav').addEventListener('keydown', event => {
  if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
  event.preventDefault(); const index = views.indexOf(state.view);
  const next = event.key === 'Home' ? 0 : event.key === 'End' ? 3 : (index + (event.key === 'ArrowRight' ? 1 : 3)) % 4;
  show(views[next]); $('tab-' + views[next]).focus();
});
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
    $('run-overview').hidden = true; notice('scenario-notice', '');
  }
  if (view === 'scenarios') {
    if (id && state.runId !== id) { state.runId = id; state.run = null; $('run-overview').hidden = true; }
    if (scenario) { state.scenarioId = scenario; state.explicitScenario = true; }
    else state.explicitScenario = false;
    renderGuided();
  }
  show(views.includes(view) ? view : 'scenarios', false);
}
window.addEventListener('hashchange', readHash); document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(true); });
example('NEW'); readHash(); loadCatalog(); setInterval(() => refresh(), 2000);
