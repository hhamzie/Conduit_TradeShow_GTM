function parseFilenameFromDisposition(disposition) {
  if (!disposition) {
    return "";
  }

  const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch (_error) {
      return utf8Match[1];
    }
  }

  const plainMatch = disposition.match(/filename="?([^"]+)"?/i);
  return plainMatch ? plainMatch[1] : "";
}

function setFormBusyState(form, isBusy, loadingLabel) {
  const submitButtons = form.querySelectorAll('button[type="submit"], input[type="submit"]');
  submitButtons.forEach((button) => {
    if (!button.dataset.defaultLabel) {
      button.dataset.defaultLabel = button.tagName === "INPUT" ? button.value : button.textContent;
    }
    button.disabled = isBusy;
    if (button.tagName === "INPUT") {
      button.value = isBusy ? loadingLabel : button.dataset.defaultLabel;
    } else {
      button.textContent = isBusy ? loadingLabel : button.dataset.defaultLabel;
    }
  });
}

function setProgressMessage(target, message, tone) {
  if (!target) {
    return;
  }
  target.hidden = !message;
  target.textContent = message || "";
  target.dataset.tone = tone || "info";
}

function updateBulkProgress(ui, { completed = 0, total = 1, currentShow = "", detail = "", tone = "info", active = true }) {
  if (!ui.panel) {
    return;
  }

  const safeCompleted = Math.max(completed || 0, 0);
  const safeTotal = Math.max(total || 0, 1);
  const percent = Math.max(0, Math.min(100, Math.round((safeCompleted / safeTotal) * 100)));

  ui.panel.hidden = false;
  ui.panel.dataset.tone = tone;
  ui.panel.dataset.active = active ? "true" : "false";

  if (ui.fill) {
    ui.fill.style.width = `${percent}%`;
  }
  if (ui.percent) {
    ui.percent.textContent = `${percent}%`;
  }
  if (ui.count) {
    ui.count.textContent = `${Math.min(safeCompleted, safeTotal)} of ${safeTotal} complete`;
  }
  if (ui.currentShow) {
    ui.currentShow.textContent = currentShow || (active ? "Preparing bulk scrape" : "Bulk scrape complete");
  }
  if (ui.detail) {
    ui.detail.textContent = detail || "";
  }
}

async function downloadResponse(response) {
  const disposition = response.headers.get("content-disposition") || "";
  const blob = await response.blob();
  const downloadUrl = window.URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = downloadUrl;
  anchor.download = parseFilenameFromDisposition(disposition) || "scrape-output";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.URL.revokeObjectURL(downloadUrl);
}

async function handleDirectDownloadFormSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const progressTargetId = form.dataset.progressTarget || "";
  const progressTarget = progressTargetId ? document.getElementById(progressTargetId) : null;
  const loadingLabel = form.dataset.loadingLabel || "Working...";

  setFormBusyState(form, true, loadingLabel);
  setProgressMessage(progressTarget, "Working on it. This can take a minute for some directories.", "info");

  try {
    const response = await fetch(form.action, {
      method: form.method || "POST",
      body: new FormData(form),
      credentials: "same-origin",
    });

    const contentType = response.headers.get("content-type") || "";
    const disposition = response.headers.get("content-disposition") || "";
    const looksLikeDownload =
      disposition.toLowerCase().includes("attachment") ||
      contentType.includes("text/csv") ||
      contentType.includes("application/zip");

    if (!response.ok) {
      let errorDetail = "";
      try {
        errorDetail = (await response.text()).trim();
      } catch (_error) {
        errorDetail = "";
      }

      if (errorDetail) {
        errorDetail = errorDetail.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
      }

      const message = errorDetail
        ? `The scraper request failed (${response.status}): ${errorDetail}`
        : `The scraper request failed (${response.status}). Please try again.`;
      throw new Error(message);
    }

    if (!looksLikeDownload) {
      window.location.reload();
      return;
    }

    await downloadResponse(response);
    setProgressMessage(progressTarget, "Download started.", "success");
  } catch (error) {
    const message =
      error instanceof Error && error.message
        ? error.message
        : "Something went wrong while starting the scrape.";
    setProgressMessage(progressTarget, message, "error");
  } finally {
    setFormBusyState(form, false, loadingLabel);
  }
}

function getBulkProgressUI(form) {
  const panelId = form.dataset.progressPanelTarget || "";
  const fillId = form.dataset.progressFillTarget || "";
  const percentId = form.dataset.progressPercentTarget || "";
  const countId = form.dataset.progressCountTarget || "";
  const currentShowId = form.dataset.progressCurrentShowTarget || "";
  const detailId = form.dataset.progressDetailTarget || "";

  return {
    panel: panelId ? document.getElementById(panelId) : null,
    fill: fillId ? document.getElementById(fillId) : null,
    percent: percentId ? document.getElementById(percentId) : null,
    count: countId ? document.getElementById(countId) : null,
    currentShow: currentShowId ? document.getElementById(currentShowId) : null,
    detail: detailId ? document.getElementById(detailId) : null,
  };
}

async function pollBulkScrapeJob(jobId, progressTarget, progressUI) {
  while (true) {
    const response = await fetch(`/scrape/bulk/status/${jobId}`, {
      credentials: "same-origin",
    });
    if (!response.ok) {
      throw new Error(`Unable to fetch bulk scrape status (${response.status}).`);
    }
    const payload = await response.json();
    updateBulkProgress(progressUI, {
      completed: payload.completed || 0,
      total: payload.total || 1,
      currentShow: payload.current_show || "Preparing bulk scrape",
      detail: payload.message || "Working...",
      tone: payload.status === "completed" ? "success" : "info",
      active: payload.status !== "completed",
    });
    setProgressMessage(progressTarget, payload.message || "Working...", "info");

    if (payload.status === "completed") {
      return payload;
    }
    if (payload.status === "failed") {
      throw new Error(payload.error || payload.message || "Bulk scrape failed.");
    }

    await new Promise((resolve) => window.setTimeout(resolve, 1000));
  }
}

async function handleBulkDownloadFormSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const progressTargetId = form.dataset.progressTarget || "";
  const progressTarget = progressTargetId ? document.getElementById(progressTargetId) : null;
  const progressUI = getBulkProgressUI(form);
  const loadingLabel = form.dataset.loadingLabel || "Working...";

  setFormBusyState(form, true, loadingLabel);
  updateBulkProgress(progressUI, {
    completed: 0,
    total: 1,
    currentShow: "Preparing bulk scrape",
    detail: "Queued bulk scrape job.",
    tone: "info",
    active: true,
  });
  setProgressMessage(progressTarget, "Queued bulk scrape job.", "info");

  try {
    const response = await fetch(form.action, {
      method: form.method || "POST",
      body: new FormData(form),
      credentials: "same-origin",
    });
    if (!response.ok) {
      let errorDetail = "";
      try {
        errorDetail = (await response.text()).trim();
      } catch (_error) {
        errorDetail = "";
      }
      const message = errorDetail
        ? `The scraper request failed (${response.status}): ${errorDetail}`
        : `The scraper request failed (${response.status}). Please try again.`;
      throw new Error(message);
    }

    const payload = await response.json();
    const registeredSummary =
      typeof payload.created === "number"
        ? `Added ${payload.created} new show(s), updated ${payload.updated || 0}, skipped ${payload.skipped || 0}.`
        : "Registered shows in the dashboard.";
    updateBulkProgress(progressUI, {
      completed: 0,
      total: 1,
      currentShow: "Queued shows",
      detail: `${registeredSummary} Starting bulk scrape...`,
      tone: "info",
      active: true,
    });
    setProgressMessage(progressTarget, `${registeredSummary} Starting bulk scrape...`, "info");
    const finalStatus = await pollBulkScrapeJob(payload.job_id, progressTarget, progressUI);
    const downloadResponseObject = await fetch(finalStatus.download_url, {
      credentials: "same-origin",
    });
    if (!downloadResponseObject.ok) {
      throw new Error(`The ZIP download failed (${downloadResponseObject.status}).`);
    }
    await downloadResponse(downloadResponseObject);
    updateBulkProgress(progressUI, {
      completed: finalStatus.completed || finalStatus.total || 1,
      total: finalStatus.total || 1,
      currentShow: finalStatus.current_show || "Bulk scrape complete",
      detail: "ZIP download started. The shows are now on the dashboard.",
      tone: "success",
      active: false,
    });
    setProgressMessage(progressTarget, "Bulk scrape finished. ZIP download started. The shows are now on the dashboard.", "success");
  } catch (error) {
    const message =
      error instanceof Error && error.message
        ? error.message
        : "Something went wrong while starting the bulk scrape.";
    updateBulkProgress(progressUI, {
      completed: 0,
      total: 1,
      currentShow: "Bulk scrape failed",
      detail: message,
      tone: "error",
      active: false,
    });
    setProgressMessage(progressTarget, message, "error");
  } finally {
    setFormBusyState(form, false, loadingLabel);
  }
}

function handleAutoSubmitFileFormChange(event) {
  const input = event.currentTarget;
  const form = input.closest("form");
  if (!form || !input.files || input.files.length === 0) {
    return;
  }
  form.requestSubmit();
}

function handleDashboardRowClick(event) {
  const row = event.currentTarget;
  const interactiveTarget = event.target.closest("a, button, input, label, summary, details, form");
  if (interactiveTarget) {
    return;
  }
  const href = row.dataset.rowHref || "";
  if (!href) {
    return;
  }
  window.location.href = href;
}

let activeTooltipTarget = null;
let floatingTooltip = null;

function ensureFloatingTooltip() {
  if (floatingTooltip) {
    return floatingTooltip;
  }
  floatingTooltip = document.createElement("div");
  floatingTooltip.className = "floating-tooltip";
  document.body.appendChild(floatingTooltip);
  return floatingTooltip;
}

function positionFloatingTooltip(target, event) {
  const tooltip = ensureFloatingTooltip();
  const text = target.dataset.tooltip || "";
  if (!text) {
    return;
  }
  tooltip.textContent = text;
  tooltip.classList.add("is-visible");

  const offset = 14;
  const tooltipRect = tooltip.getBoundingClientRect();
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;
  const targetRect = target.getBoundingClientRect();
  const anchorX = typeof event.clientX === "number" ? event.clientX : (targetRect.left + targetRect.right) / 2;
  const anchorY = typeof event.clientY === "number" ? event.clientY : targetRect.top;
  let left = anchorX - (tooltipRect.width / 2);
  let top = anchorY - tooltipRect.height - offset;

  if (left < 8) {
    left = 8;
  }
  if (left + tooltipRect.width > viewportWidth - 8) {
    left = viewportWidth - tooltipRect.width - 8;
  }
  if (top < 8) {
    top = (typeof event.clientY === "number" ? event.clientY : targetRect.bottom) + offset;
  }
  if (top + tooltipRect.height > viewportHeight - 8) {
    top = viewportHeight - tooltipRect.height - 8;
  }

  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function handleTooltipEnter(event) {
  const target = event.currentTarget;
  if (!target.dataset.tooltip) {
    return;
  }
  activeTooltipTarget = target;
  positionFloatingTooltip(target, event);
}

function handleTooltipMove(event) {
  if (activeTooltipTarget !== event.currentTarget) {
    return;
  }
  positionFloatingTooltip(event.currentTarget, event);
}

function handleTooltipLeave(event) {
  if (activeTooltipTarget !== event.currentTarget) {
    return;
  }
  activeTooltipTarget = null;
  if (floatingTooltip) {
    floatingTooltip.classList.remove("is-visible");
  }
}

function closeFlashModal() {
  const flashModal = document.querySelector("[data-flash-modal]");
  if (!flashModal) {
    return;
  }
  flashModal.remove();
}

function handleFlashModalClick(event) {
  if (event.target.matches("[data-flash-close]") || event.target.matches("[data-flash-modal]")) {
    closeFlashModal();
  }
}

function handleFlashModalKeydown(event) {
  if (event.key === "Escape") {
    closeFlashModal();
  }
}

const guideRowAutosaveTimers = new Map();
const guideRowAutosaveControllers = new Map();
const guideRowAutosaveStateTimers = new Map();

function getGuideRowStateTarget(form) {
  return document.querySelector(`[data-guide-row-form-id="${form.id}"]`);
}

function setGuideRowState(form, state) {
  const row = getGuideRowStateTarget(form);
  if (!row) {
    return;
  }
  row.dataset.guideRowState = state;
}

function clearGuideRowStateTimer(formId) {
  const timer = guideRowAutosaveStateTimers.get(formId);
  if (timer) {
    window.clearTimeout(timer);
    guideRowAutosaveStateTimers.delete(formId);
  }
}

function queueGuideRowStateReset(form, delayMs) {
  clearGuideRowStateTimer(form.id);
  guideRowAutosaveStateTimers.set(
    form.id,
    window.setTimeout(() => {
      setGuideRowState(form, "idle");
      guideRowAutosaveStateTimers.delete(form.id);
    }, delayMs),
  );
}

function buildGuideRowFormData(form) {
  const payload = new FormData();
  const fields = document.querySelectorAll(`[form="${form.id}"][name]`);
  fields.forEach((field) => {
    if (field.disabled) {
      return;
    }
    if (field instanceof HTMLInputElement && field.type === "file") {
      if (field.files && field.files.length > 0) {
        payload.set(field.name, field.files[0]);
      }
      return;
    }
    payload.set(field.name, field.value);
  });
  return payload;
}

async function saveGuideRowForm(form) {
  clearGuideRowStateTimer(form.id);
  const existingController = guideRowAutosaveControllers.get(form.id);
  if (existingController) {
    existingController.abort();
  }

  const controller = new AbortController();
  guideRowAutosaveControllers.set(form.id, controller);
  setGuideRowState(form, "saving");

  try {
    const response = await fetch(form.action, {
      method: (form.method || "POST").toUpperCase(),
      body: buildGuideRowFormData(form),
      credentials: "same-origin",
      headers: {
        "X-Guide-Autosave": "1",
      },
      signal: controller.signal,
    });

    if (!response.ok) {
      throw new Error(`Guide row save failed (${response.status}).`);
    }

    setGuideRowState(form, "saved");
    queueGuideRowStateReset(form, 1200);
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      return;
    }
    setGuideRowState(form, "error");
    queueGuideRowStateReset(form, 1800);
  } finally {
    if (guideRowAutosaveControllers.get(form.id) === controller) {
      guideRowAutosaveControllers.delete(form.id);
    }
  }
}

function queueGuideRowAutosave(input, immediate = false) {
  const formId = input.getAttribute("form") || "";
  const form = formId ? document.getElementById(formId) : null;
  if (!(form instanceof HTMLFormElement)) {
    return;
  }

  const existingTimer = guideRowAutosaveTimers.get(form.id);
  if (existingTimer) {
    window.clearTimeout(existingTimer);
  }

  setGuideRowState(form, "dirty");
  const delayMs = immediate ? 80 : 450;
  guideRowAutosaveTimers.set(
    form.id,
    window.setTimeout(() => {
      guideRowAutosaveTimers.delete(form.id);
      void saveGuideRowForm(form);
    }, delayMs),
  );
}

function handleGuideAutosaveInput(event) {
  const target = event.currentTarget;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  queueGuideRowAutosave(target, false);
}

function handleGuideAutosaveCommit(event) {
  const target = event.currentTarget;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  queueGuideRowAutosave(target, true);
}

function activateSheetTab(tabStrip, targetId, updateHash = true) {
  const tabs = tabStrip.querySelectorAll("[data-sheet-tab]");
  const panels = document.querySelectorAll("[data-sheet-panel]");
  let activated = false;

  tabs.forEach((tab) => {
    const isActive = tab.getAttribute("data-sheet-target") === targetId;
    tab.classList.toggle("is-active", isActive);
    tab.setAttribute("aria-selected", isActive ? "true" : "false");
    if (isActive) {
      activated = true;
    }
  });

  panels.forEach((panel) => {
    const isActive = panel.id === targetId;
    panel.classList.toggle("is-active", isActive);
    panel.hidden = !isActive;
  });

  if (activated && updateHash) {
    window.history.replaceState(null, "", `#${targetId}`);
  }
}

function handleSheetTabClick(event) {
  const tab = event.currentTarget;
  const tabStrip = tab.closest("[data-sheet-tabs]");
  const targetId = tab.getAttribute("data-sheet-target") || "";
  if (!tabStrip || !targetId) {
    return;
  }
  activateSheetTab(tabStrip, targetId, true);
}

function initializeSheetTabs() {
  const tabStrip = document.querySelector("[data-sheet-tabs]");
  if (!tabStrip) {
    return;
  }

  const tabs = tabStrip.querySelectorAll("[data-sheet-tab]");
  tabs.forEach((tab) => {
    tab.addEventListener("click", handleSheetTabClick);
  });

  const hashTarget = window.location.hash.replace(/^#/, "");
  const initialTab =
    (hashTarget && tabStrip.querySelector(`[data-sheet-target="${hashTarget}"]`)) ||
    tabStrip.querySelector("[data-sheet-tab]");

  if (initialTab instanceof HTMLElement) {
    activateSheetTab(tabStrip, initialTab.getAttribute("data-sheet-target") || "", false);
  }
}

function updateLeadTable(table) {
  const panel = table.closest(".lead-panel");
  if (!panel) {
    return;
  }

  const searchInput = panel.querySelector("[data-lead-search]");
  const resultsTarget = panel.querySelector("[data-lead-results]");
  const emptyRow = table.querySelector("[data-lead-empty]");
  const rows = Array.from(table.querySelectorAll("[data-lead-row]"));
  const limit = Number.parseInt(table.dataset.leadLimit || "10", 10) || 10;
  const query = searchInput instanceof HTMLInputElement ? searchInput.value.trim().toLowerCase() : "";

  let matchCount = 0;
  let shownCount = 0;

  rows.forEach((row) => {
    const haystack = String(row.getAttribute("data-lead-search-text") || "").toLowerCase();
    const matches = !query || haystack.includes(query);
    if (!matches) {
      row.hidden = true;
      return;
    }

    matchCount += 1;
    if (shownCount < limit) {
      row.hidden = false;
      shownCount += 1;
    } else {
      row.hidden = true;
    }
  });

  if (emptyRow) {
    emptyRow.hidden = matchCount !== 0;
  }

  if (resultsTarget) {
    if (matchCount === 0) {
      resultsTarget.textContent = "No matches";
    } else {
      resultsTarget.textContent = `Showing ${shownCount} of ${matchCount}`;
    }
  }
}

function initializeLeadTables() {
  const leadTables = document.querySelectorAll("[data-lead-table]");
  leadTables.forEach((table) => {
    updateLeadTable(table);

    const panel = table.closest(".lead-panel");
    const searchInput = panel ? panel.querySelector("[data-lead-search]") : null;
    if (searchInput instanceof HTMLInputElement) {
      searchInput.addEventListener("input", () => updateLeadTable(table));
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  const directForms = document.querySelectorAll("[data-direct-download-form]");
  directForms.forEach((form) => {
    form.addEventListener("submit", handleDirectDownloadFormSubmit);
  });

  const bulkForms = document.querySelectorAll("[data-bulk-download-form]");
  bulkForms.forEach((form) => {
    form.addEventListener("submit", handleBulkDownloadFormSubmit);
  });

  const fileForms = document.querySelectorAll("[data-auto-submit-file-form] input[type='file']");
  fileForms.forEach((input) => {
    input.addEventListener("change", handleAutoSubmitFileFormChange);
  });

  const dashboardRows = document.querySelectorAll("[data-row-href]");
  dashboardRows.forEach((row) => {
    row.addEventListener("click", handleDashboardRowClick);
  });

  const tooltipTargets = document.querySelectorAll("[data-tooltip]");
  tooltipTargets.forEach((target) => {
    target.addEventListener("mouseenter", handleTooltipEnter);
    target.addEventListener("mousemove", handleTooltipMove);
    target.addEventListener("mouseleave", handleTooltipLeave);
    target.addEventListener("focus", handleTooltipEnter);
    target.addEventListener("blur", handleTooltipLeave);
  });

  const flashModal = document.querySelector("[data-flash-modal]");
  if (flashModal) {
    flashModal.addEventListener("click", handleFlashModalClick);
    document.addEventListener("keydown", handleFlashModalKeydown);
  }

  const guideAutosaveInputs = document.querySelectorAll("[data-guide-autosave-input]");
  guideAutosaveInputs.forEach((input) => {
    input.addEventListener("input", handleGuideAutosaveInput);
    input.addEventListener("change", handleGuideAutosaveCommit);
    input.addEventListener("blur", handleGuideAutosaveCommit);
  });

  initializeSheetTabs();
  initializeLeadTables();
});
