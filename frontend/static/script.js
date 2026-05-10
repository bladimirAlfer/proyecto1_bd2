let editor = null;
let lastHistory = [];

document.addEventListener("DOMContentLoaded", () => {
  setupEditor();
  setupEvents();
  setupTabs();
  refreshTables();
  drawEmptySpatial();
});

function setupEditor() {
  const textarea = document.getElementById("sql-editor");
  if (window.CodeMirror) {
    editor = CodeMirror.fromTextArea(textarea, {
      mode: "text/x-sql",
      theme: "material-darker",
      lineNumbers: true,
      indentUnit: 4,
      tabSize: 4,
      lineWrapping: true,
      smartIndent: true,
      extraKeys: {
        "Ctrl-Enter": runQuery,
        "Cmd-Enter": runQuery
      }
    });
  }
}

function setupEvents() {
  document.getElementById("btn-run").addEventListener("click", runQuery);
  document.getElementById("btn-clear").addEventListener("click", clearEditor);
  document.getElementById("btn-copy-log").addEventListener("click", copyLog);
  document.getElementById("csv-upload-form").addEventListener("submit", uploadCsv);
  document.getElementById("csv-file").addEventListener("change", updateCsvFileLabel);

}

function setupTabs() {
  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((tab) => tab.classList.remove("active"));
      document.querySelectorAll(".tab-pane").forEach((pane) => pane.classList.remove("active"));
      button.classList.add("active");
      document.getElementById(`tab-${button.dataset.tab}`).classList.add("active");
    });
  });
}

function getEditorValue() {
  return editor ? editor.getValue() : document.getElementById("sql-editor").value;
}

function getSelectedEditorValue() {
  // Caso CodeMirror 5, que es el que estás usando
  if (editor && typeof editor.getSelection === "function") {
    const selected = editor.getSelection();
    return selected || "";
  }

  // Caso textarea normal
  const textarea = document.getElementById("sql-editor");

  if (textarea && textarea.selectionStart !== textarea.selectionEnd) {
    return textarea.value.substring(textarea.selectionStart, textarea.selectionEnd);
  }

  return "";
}


function setEditorValue(value) {
  if (editor) {
    editor.setValue(value);
    editor.focus();
  } else {
    const textarea = document.getElementById("sql-editor");
    textarea.value = value;
    textarea.focus();
  }
}

function clearEditor() {
  setEditorValue("");
}

async function runQuery() {
  const selectedSql = getSelectedEditorValue().trim();
  const sql = selectedSql || getEditorValue().trim();

  if (!sql) {
    showStatus("Ingresa una consulta SQL.", false);
    return;
  }

  setRunning(true);
  try {
    const response = await fetch("/api/execute", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql })
    });

    const payload = await response.json();
    const result = payload.last_result || {};

    renderResult(result);
    renderStats(result);
    renderHistory(payload.history || []);
    renderTables(payload.tables || { tables: [] });

    if (result.spatial) {
      renderSpatial(result.spatial);
      activateTab("spatial");
    } else {
      drawEmptySpatial();
      activateTab("results");
    }
  } catch (error) {
    showStatus(`Error de frontend: ${error.message}`, false);
  } finally {
    setRunning(false);
  }
}


async function refreshTables() {
  try {
    const response = await fetch("/api/tables");
    const payload = await response.json();
    renderTables(payload);
  } catch (error) {
    document.getElementById("tables-list").innerHTML = `<div class="tree-empty">${escapeHtml(error.message)}</div>`;
  }
}

function updateCsvFileLabel() {
  const input = document.getElementById("csv-file");
  const label = document.getElementById("csv-file-label");
  label.textContent = input.files && input.files.length ? input.files[0].name : "Seleccionar archivo .csv";
}

async function uploadCsv(event) {
  event.preventDefault();
  const fileInput = document.getElementById("csv-file");
  const tableInput = document.getElementById("csv-table-name");
  const status = document.getElementById("csv-upload-status");

  if (!fileInput.files || !fileInput.files.length) {
    status.textContent = "Selecciona un archivo CSV primero.";
    status.className = "hint error-text";
    return;
  }

  const formData = new FormData();
  formData.append("file", fileInput.files[0]);
  formData.append("table_name", tableInput.value || "");

  status.textContent = "Subiendo CSV...";
  status.className = "hint";

  try {
    const response = await fetch("/api/upload_csv", {
      method: "POST",
      body: formData
    });
    const payload = await response.json();

    if (!payload.success) {
      const message = payload.error || "No se pudo subir el CSV.";
      status.textContent = message;
      status.className = "hint error-text";
      showStatus(message, false);
      return;
    }

    setEditorValue(payload.suggested_sql || "");
    status.textContent = `${payload.file_name} listo. Revisa el SQL generado y ejecútalo.`;
    status.className = "hint ok-text";
    renderHistory(payload.history || lastHistory);
    showStatus(payload.message || "CSV subido. Ejecuta el CREATE TABLE generado.", true);
    activateTab("results");
  } catch (error) {
    status.textContent = `Error al subir CSV: ${error.message}`;
    status.className = "hint error-text";
    showStatus(`Error al subir CSV: ${error.message}`, false);
  }
}


function setRunning(isRunning) {
  const runButton = document.getElementById("btn-run");
  runButton.disabled = isRunning;
  runButton.textContent = isRunning ? "Ejecutando..." : "Ejecutar ▶";
}

function activateTab(tabName) {
  const button = document.querySelector(`.tab[data-tab="${tabName}"]`);
  if (button) button.click();
}

function renderResult(result) {
  showStatus(result.message || result.error || "Consulta ejecutada.", Boolean(result.success));
  const columns = result.columns || [];
  const rows = result.rows || [];
  const thead = document.querySelector("#result-table thead");
  const tbody = document.querySelector("#result-table tbody");

  if (!columns.length) {
    thead.innerHTML = "";
    tbody.innerHTML = `<tr><td>Sin filas para mostrar.</td></tr>`;
    return;
  }

  thead.innerHTML = `<tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>`;
  tbody.innerHTML = rows.length
    ? rows.map((row) => `<tr>${columns.map((column) => `<td>${formatCell(row[column])}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${columns.length}">La consulta no devolvió filas.</td></tr>`;
}

function renderStats(result) {
  document.getElementById("stat-reads").textContent = result.disk_reads ?? 0;
  document.getElementById("stat-writes").textContent = result.disk_writes ?? 0;
  document.getElementById("stat-accesses").textContent = result.disk_accesses ?? 0;
  document.getElementById("stat-time").textContent = `${Number(result.time_ms || 0).toFixed(3)} ms`;
  document.getElementById("stat-index").textContent = result.used_index || "-";
  document.getElementById("stat-message").textContent = result.message || result.error || "-";
}

function renderHistory(history) {
  lastHistory = history || [];
  const log = document.getElementById("execution-log");
  if (!lastHistory.length) {
    log.textContent = "Sin ejecuciones todavía.";
    return;
  }

  log.textContent = lastHistory
    .slice()
    .reverse()
    .map((item) => {
      return `[${item.timestamp}] ${item.status} | ${item.command_type}\n` +
        `  Mensaje : ${item.message}\n` +
        `  Filas   : ${item.rows}\n` +
        `  I/O     : reads=${item.disk_reads}, writes=${item.disk_writes}, total=${item.disk_accesses}\n` +
        `  Tiempo  : ${Number(item.time_ms || 0).toFixed(3)} ms\n` +
        `  Índice  : ${item.used_index || "-"}\n` +
        `  SQL     : ${item.sql.replace(/\s+/g, " ").trim()}\n`;
    })
    .join("\n");
}

function renderTables(payload) {
  const container = document.getElementById("tables-list");
  const tables = payload.tables || [];
  if (!tables.length) {
    container.innerHTML = `<div class="tree-empty">Sin tablas cargadas</div>`;
    return;
  }

  container.innerHTML = tables.map((table) => {
    const columns = table.columns || [];
    const indexes = table.indexes || {};
    const columnText = columns.map((column) => `${column.name}:${column.type}`).join(", ");
    const indexPills = Object.values(indexes).map((idx) => {
      return `<span class="index-pill">${escapeHtml(idx.column_name || idx.column || "?")} · ${escapeHtml(idx.index_type || idx.type || "index")}</span>`;
    }).join("");

    return `<div class="table-node">
      <strong>${escapeHtml(table.name || "tabla")}</strong>
      <small>${escapeHtml(columnText || "sin columnas")}</small>
      <small>${Number(table.row_count || 0)} fila(s)</small>
      <div>${indexPills || `<span class="index-pill">sin índices</span>`}</div>
    </div>`;
  }).join("");
}

function renderSpatial(spatial) {
  const canvas = document.getElementById("spatial-canvas");
  const ctx = canvas.getContext("2d");
  resizeCanvasForDisplay(canvas);
  clearCanvas(ctx, canvas);

  const points = (spatial.points || []).map((point) => ({
    x: Number(point.x),
    y: Number(point.y),
    match: Boolean(point.match)
  })).filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));

  const queryPoint = spatial.query_point
    ? { x: Number(spatial.query_point.x), y: Number(spatial.query_point.y) }
    : null;

  const allPoints = queryPoint ? [...points, queryPoint] : points;
  if (!allPoints.length) {
    drawEmptySpatial("La consulta espacial no devolvió puntos.");
    return;
  }

  const bounds = computeBounds(allPoints);
  const map = buildMapper(bounds, canvas.width, canvas.height);

  drawGrid(ctx, canvas);

  if (queryPoint && spatial.radius) {
    const center = map(queryPoint.x, queryPoint.y);
    const edge = map(queryPoint.x + Number(spatial.radius), queryPoint.y);
    const radiusPx = Math.abs(edge.x - center.x);
    ctx.beginPath();
    ctx.arc(center.x, center.y, radiusPx, 0, Math.PI * 2);
    ctx.strokeStyle = "rgba(251, 191, 36, 0.55)";
    ctx.lineWidth = 2;
    ctx.setLineDash([7, 7]);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  points.forEach((point) => {
    const mapped = map(point.x, point.y);
    ctx.beginPath();
    ctx.arc(mapped.x, mapped.y, 6, 0, Math.PI * 2);
    ctx.fillStyle = point.match ? "#22c55e" : "#38bdf8";
    ctx.fill();
    ctx.strokeStyle = "#0f172a";
    ctx.lineWidth = 2;
    ctx.stroke();
  });

  if (queryPoint) {
    const mapped = map(queryPoint.x, queryPoint.y);
    ctx.beginPath();
    ctx.arc(mapped.x, mapped.y, 8, 0, Math.PI * 2);
    ctx.fillStyle = "#fbbf24";
    ctx.fill();
    ctx.strokeStyle = "#fef3c7";
    ctx.lineWidth = 2;
    ctx.stroke();
  }

  const note = document.getElementById("spatial-note");
  const queryLabel = queryPoint ? `POINT(${queryPoint.x}, ${queryPoint.y})` : "consulta espacial";
  const extra = spatial.radius ? `RADIUS ${spatial.radius}` : spatial.k ? `K ${spatial.k}` : "";
  note.textContent = `${queryLabel} ${extra} · ${points.length} punto(s) graficado(s).`;
}

function drawEmptySpatial(message = "Ejecuta una consulta POINT/RADIUS o POINT/K para graficar los puntos.") {
  const canvas = document.getElementById("spatial-canvas");
  const ctx = canvas.getContext("2d");
  resizeCanvasForDisplay(canvas);
  clearCanvas(ctx, canvas);
  drawGrid(ctx, canvas);
  ctx.fillStyle = "#94a3b8";
  ctx.font = "16px system-ui";
  ctx.fillText(message, 24, 40);
  document.getElementById("spatial-note").textContent = message;
}

function resizeCanvasForDisplay(canvas) {
  const rect = canvas.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  const width = Math.max(600, Math.floor(rect.width * ratio));
  const height = Math.max(360, Math.floor(420 * ratio));
  if (canvas.width !== width || canvas.height !== height) {
    canvas.width = width;
    canvas.height = height;
  }
}

function clearCanvas(ctx, canvas) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#020617";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
}

function drawGrid(ctx, canvas) {
  ctx.strokeStyle = "rgba(148, 163, 184, 0.12)";
  ctx.lineWidth = 1;
  const step = 48;
  for (let x = 0; x <= canvas.width; x += step) {
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, canvas.height);
    ctx.stroke();
  }
  for (let y = 0; y <= canvas.height; y += step) {
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(canvas.width, y);
    ctx.stroke();
  }
}

function computeBounds(points) {
  const xs = points.map((point) => point.x);
  const ys = points.map((point) => point.y);
  let minX = Math.min(...xs);
  let maxX = Math.max(...xs);
  let minY = Math.min(...ys);
  let maxY = Math.max(...ys);

  if (minX === maxX) {
    minX -= 0.01;
    maxX += 0.01;
  }
  if (minY === maxY) {
    minY -= 0.01;
    maxY += 0.01;
  }

  const padX = (maxX - minX) * 0.15;
  const padY = (maxY - minY) * 0.15;
  return {
    minX: minX - padX,
    maxX: maxX + padX,
    minY: minY - padY,
    maxY: maxY + padY
  };
}

function buildMapper(bounds, width, height) {
  const margin = 34;
  return (x, y) => {
    const normalizedX = (x - bounds.minX) / (bounds.maxX - bounds.minX);
    const normalizedY = (y - bounds.minY) / (bounds.maxY - bounds.minY);
    return {
      x: margin + normalizedX * (width - margin * 2),
      y: height - margin - normalizedY * (height - margin * 2)
    };
  };
}

function showStatus(message, ok) {
  const banner = document.getElementById("status-banner");
  banner.textContent = message;
  banner.className = `status ${ok ? "ok" : "error"}`;
}

function copyLog() {
  const text = document.getElementById("execution-log").textContent || "";
  navigator.clipboard?.writeText(text);
}

function formatCell(value) {
  if (value === null || value === undefined) return "<span style='color:#94a3b8'>NULL</span>";
  if (typeof value === "object") return `<code>${escapeHtml(JSON.stringify(value))}</code>`;
  return escapeHtml(String(value));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
