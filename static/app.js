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
let activeActionMenuDetails = null;
let activeActionMenuPanel = null;

function positionFloatingActionMenu(details, panel) {
  const summary = details.querySelector("summary");
  if (!(summary instanceof HTMLElement)) {
    return;
  }

  panel.classList.add("is-floating");
  panel.style.top = "0px";
  panel.style.left = "0px";
  panel.style.right = "auto";

  const summaryRect = summary.getBoundingClientRect();
  const panelRect = panel.getBoundingClientRect();
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;

  let left = summaryRect.right - panelRect.width;
  let top = summaryRect.bottom + 10;

  if (left < 8) {
    left = 8;
  }
  if (left + panelRect.width > viewportWidth - 8) {
    left = viewportWidth - panelRect.width - 8;
  }
  if (top + panelRect.height > viewportHeight - 8) {
    top = Math.max(8, summaryRect.top - panelRect.height - 10);
  }

  panel.style.left = `${left}px`;
  panel.style.top = `${top}px`;
}

function restoreFloatingActionMenu(details, panel) {
  const mount = details.querySelector("[data-action-menu-mount]");
  if (!(mount instanceof HTMLElement)) {
    return;
  }
  mount.after(panel);
  panel.classList.remove("is-floating");
  panel.style.top = "";
  panel.style.left = "";
  panel.style.right = "";
}

function closeActiveFloatingActionMenu() {
  if (activeActionMenuDetails instanceof HTMLDetailsElement) {
    activeActionMenuDetails.open = false;
  }
}

function handleActionMenuDocumentClick(event) {
  if (!(activeActionMenuDetails instanceof HTMLDetailsElement) || !(activeActionMenuPanel instanceof HTMLElement)) {
    return;
  }

  const summary = activeActionMenuDetails.querySelector("summary");
  const target = event.target;
  if (
    target instanceof Node &&
    (activeActionMenuPanel.contains(target) || (summary instanceof HTMLElement && summary.contains(target)))
  ) {
    return;
  }

  closeActiveFloatingActionMenu();
}

function handleActionMenuViewportChange() {
  if (!(activeActionMenuDetails instanceof HTMLDetailsElement) || !(activeActionMenuPanel instanceof HTMLElement)) {
    return;
  }
  positionFloatingActionMenu(activeActionMenuDetails, activeActionMenuPanel);
}

function initializeActionMenus() {
  const actionMenus = document.querySelectorAll(".action-menu");
  actionMenus.forEach((details) => {
    if (!(details instanceof HTMLDetailsElement)) {
      return;
    }

    const panel = details.querySelector(".action-menu-panel");
    if (!(panel instanceof HTMLElement)) {
      return;
    }

    if (!details.querySelector("[data-action-menu-mount]")) {
      const mount = document.createElement("span");
      mount.hidden = true;
      mount.dataset.actionMenuMount = "true";
      panel.before(mount);
    }

    details.addEventListener("toggle", () => {
      if (details.open) {
        if (activeActionMenuDetails && activeActionMenuDetails !== details) {
          closeActiveFloatingActionMenu();
        }
        document.body.appendChild(panel);
        positionFloatingActionMenu(details, panel);
        activeActionMenuDetails = details;
        activeActionMenuPanel = panel;
      } else {
        restoreFloatingActionMenu(details, panel);
        if (activeActionMenuDetails === details) {
          activeActionMenuDetails = null;
          activeActionMenuPanel = null;
        }
      }
    });
  });

  document.addEventListener("click", handleActionMenuDocumentClick);
  window.addEventListener("resize", handleActionMenuViewportChange);
  window.addEventListener("scroll", handleActionMenuViewportChange, true);
}

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

function openOutboundModal(trigger) {
  const modal = document.querySelector("[data-outbound-modal]");
  const form = modal ? modal.querySelector("[data-outbound-form]") : null;
  const copyTarget = modal ? modal.querySelector("[data-outbound-copy]") : null;
  const capacityTarget = modal ? modal.querySelector("[data-outbound-capacity-copy]") : null;
  const confirmButton = modal ? modal.querySelector("[data-outbound-confirm]") : null;
  if (!(modal instanceof HTMLElement) || !(form instanceof HTMLFormElement) || !(copyTarget instanceof HTMLElement)) {
    return;
  }

  const showId = trigger.dataset.showId || "";
  const showName = trigger.dataset.showName || "this show";
  const emailCount = Number.parseInt(trigger.dataset.emailCount || "0", 10) || 0;
  const linkedinCount = Number.parseInt(trigger.dataset.linkedinCount || "0", 10) || 0;
  const weeks = Number.parseInt(trigger.dataset.weeks || "3", 10) || 3;
  const capacityBlocked = trigger.dataset.capacityBlocked === "true";
  const capacityDetail = trigger.dataset.capacityDetail || "";

  form.action = `/shows/${showId}/outbound/start`;
  copyTarget.textContent =
    `Start outbound for ${showName}? This will send ${emailCount} emails and ${linkedinCount} LinkedIn messages over the next ${weeks} weeks.`;

  if (capacityTarget instanceof HTMLElement) {
    capacityTarget.textContent = capacityDetail;
    capacityTarget.hidden = !capacityDetail;
  }

  if (confirmButton instanceof HTMLButtonElement) {
    confirmButton.disabled = capacityBlocked || emailCount <= 0;
  }

  modal.hidden = false;
  document.body.dataset.modalOpen = "true";
}

function closeOutboundModal() {
  const modal = document.querySelector("[data-outbound-modal]");
  if (!(modal instanceof HTMLElement)) {
    return;
  }
  modal.hidden = true;
  delete document.body.dataset.modalOpen;
}

function initializeOutboundModal() {
  const triggers = document.querySelectorAll("[data-outbound-trigger]");
  triggers.forEach((trigger) => {
    trigger.addEventListener("click", () => openOutboundModal(trigger));
  });

  const closeButtons = document.querySelectorAll("[data-outbound-close]");
  closeButtons.forEach((button) => {
    button.addEventListener("click", closeOutboundModal);
  });

  const modal = document.querySelector("[data-outbound-modal]");
  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        closeOutboundModal();
      }
    });
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeOutboundModal();
    }
  });
}

function renderScanResults(candidates) {
  return candidates
    .map((candidate) => {
      const name = candidate.show_name || "Untitled show";
      const date = candidate.event_date_raw || candidate.event_date || "";
      const place = candidate.place || "";
      const link = candidate.link || "";
      const summary = candidate.summary || "";
      return `
        <article class="scan-result-card">
          <strong>${name}</strong>
          <span class="muted">${date}${place ? ` · ${place}` : ""}</span>
          ${summary ? `<p>${summary}</p>` : ""}
          ${link ? `<a href="${link}" target="_blank" rel="noreferrer">${link}</a>` : ""}
        </article>
      `;
    })
    .join("");
}

function resetScanModalState(modal) {
  const results = modal.querySelector("[data-scan-results]");
  const empty = modal.querySelector("[data-scan-empty]");
  const list = modal.querySelector("[data-scan-list]");
  const payload = modal.querySelector("[data-scan-payload]");
  const message = modal.querySelector("[data-scan-message]");
  const progress = modal.querySelector("[data-scan-progress]");
  const elapsed = modal.querySelector("[data-scan-elapsed]");
  if (results instanceof HTMLElement) {
    results.hidden = true;
  }
  if (empty instanceof HTMLElement) {
    empty.hidden = true;
    empty.textContent = "";
  }
  if (list instanceof HTMLElement) {
    list.innerHTML = "";
  }
  if (payload instanceof HTMLInputElement) {
    payload.value = "";
  }
  if (message instanceof HTMLElement) {
    message.textContent = "";
  }
  if (progress instanceof HTMLElement) {
    progress.hidden = true;
  }
  if (elapsed instanceof HTMLElement) {
    elapsed.textContent = "0s elapsed";
  }
}

function openScanModal() {
  const modal = document.querySelector("[data-scan-modal]");
  if (!(modal instanceof HTMLElement)) {
    return;
  }
  resetScanModalState(modal);
  modal.hidden = false;
  document.body.dataset.modalOpen = "true";
}

function closeScanModal() {
  const modal = document.querySelector("[data-scan-modal]");
  if (!(modal instanceof HTMLElement)) {
    return;
  }
  modal.hidden = true;
  resetScanModalState(modal);
  delete document.body.dataset.modalOpen;
}

async function handleScanSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const modal = form.closest("[data-scan-modal]");
  const results = modal ? modal.querySelector("[data-scan-results]") : null;
  const empty = modal ? modal.querySelector("[data-scan-empty]") : null;
  const list = modal ? modal.querySelector("[data-scan-list]") : null;
  const payloadField = modal ? modal.querySelector("[data-scan-payload]") : null;
  const message = modal ? modal.querySelector("[data-scan-message]") : null;
  const confirmButton = modal ? modal.querySelector("[data-scan-confirm]") : null;
  const progress = modal ? modal.querySelector("[data-scan-progress]") : null;
  const elapsed = modal ? modal.querySelector("[data-scan-elapsed]") : null;
  const startedAt = Date.now();
  let elapsedTimer = 0;

  if (progress instanceof HTMLElement) {
    progress.hidden = false;
  }
  if (elapsed instanceof HTMLElement) {
    elapsed.textContent = "0s elapsed";
    elapsedTimer = window.setInterval(() => {
      const seconds = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
      elapsed.textContent = `${seconds}s elapsed`;
    }, 1000);
  }

  setFormBusyState(form, true, "Scanning...");
  if (results instanceof HTMLElement) {
    results.hidden = true;
  }
  if (empty instanceof HTMLElement) {
    empty.hidden = true;
    empty.textContent = "";
  }

  try {
    const response = await fetch(form.action, {
      method: "POST",
      body: new FormData(form),
      credentials: "same-origin",
    });
    const payload = await response.json();
    if (!response.ok || payload.status === "error") {
      throw new Error(payload.message || `Scan failed (${response.status}).`);
    }

    if (payload.status === "empty") {
      if (empty instanceof HTMLElement) {
        empty.hidden = false;
        empty.textContent = payload.message || "No trade shows were found.";
      }
      return;
    }

    const candidates = Array.isArray(payload.candidates) ? payload.candidates : [];
    if (message instanceof HTMLElement) {
      message.textContent = payload.message || `Found ${candidates.length} shows.`;
    }
    if (list instanceof HTMLElement) {
      list.innerHTML = renderScanResults(candidates);
    }
    if (payloadField instanceof HTMLInputElement) {
      payloadField.value = JSON.stringify(candidates);
    }
    if (confirmButton instanceof HTMLButtonElement) {
      confirmButton.textContent = `Add ${candidates.length} show${candidates.length === 1 ? "" : "s"}`;
      confirmButton.disabled = candidates.length === 0;
    }
    if (results instanceof HTMLElement) {
      results.hidden = false;
    }
  } catch (error) {
    if (empty instanceof HTMLElement) {
      empty.hidden = false;
      empty.textContent =
        error instanceof Error && error.message ? error.message : "Scan failed.";
    }
  } finally {
    if (elapsedTimer) {
      window.clearInterval(elapsedTimer);
    }
    if (progress instanceof HTMLElement) {
      progress.hidden = true;
    }
    setFormBusyState(form, false, "Scanning...");
  }
}

async function handleScanConfirm(event) {
  event.preventDefault();
  const form = event.currentTarget;
  setFormBusyState(form, true, "Adding...");
  try {
    const response = await fetch(form.action, {
      method: "POST",
      body: new FormData(form),
      credentials: "same-origin",
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.detail || payload.message || `Add failed (${response.status}).`);
    }
    window.location.href = payload.redirect || "/shows/dashboard";
  } catch (error) {
    const modal = form.closest("[data-scan-modal]");
    const empty = modal ? modal.querySelector("[data-scan-empty]") : null;
    if (empty instanceof HTMLElement) {
      empty.hidden = false;
      empty.textContent = error instanceof Error && error.message ? error.message : "Could not add scanned shows.";
    }
  } finally {
    setFormBusyState(form, false, "Adding...");
  }
}

function initializeScanModal() {
  const openButton = document.querySelector("[data-scan-open]");
  if (openButton) {
    openButton.addEventListener("click", openScanModal);
  }

  const closeButtons = document.querySelectorAll("[data-scan-close]");
  closeButtons.forEach((button) => {
    button.addEventListener("click", closeScanModal);
  });

  const modal = document.querySelector("[data-scan-modal]");
  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        closeScanModal();
      }
    });

    const shouldAutoOpen = modal.dataset.scanAutoOpen === "true";
    const autoOpenDay = (modal.dataset.scanAutoDay || "sunday").toLowerCase();
    const currentDay = new Intl.DateTimeFormat("en-US", { weekday: "long" }).format(new Date()).toLowerCase();
    if (shouldAutoOpen && currentDay === autoOpenDay) {
      openScanModal();
    }
  }

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    const scanModal = document.querySelector("[data-scan-modal]");
    if (scanModal instanceof HTMLElement && !scanModal.hidden) {
      closeScanModal();
    }
  });

  const form = document.querySelector("[data-scan-form]");
  if (form instanceof HTMLFormElement) {
    form.addEventListener("submit", handleScanSubmit);
  }

  const confirmForm = document.querySelector("[data-scan-confirm-form]");
  if (confirmForm instanceof HTMLFormElement) {
    confirmForm.addEventListener("submit", handleScanConfirm);
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
  const selectAll = table.querySelector("[data-lead-select-all]");
  const selectionItems = Array.from(table.querySelectorAll("[data-lead-select-item]"));
  const rows = Array.from(table.querySelectorAll("[data-lead-row]"));
  const query = searchInput instanceof HTMLInputElement ? searchInput.value.trim().toLowerCase() : "";

  let matchCount = 0;

  rows.forEach((row) => {
    const haystack = String(row.getAttribute("data-lead-search-text") || "").toLowerCase();
    const matches = !query || haystack.includes(query);
    if (!matches) {
      row.hidden = true;
      return;
    }

    matchCount += 1;
    row.hidden = false;
  });

  if (emptyRow) {
    emptyRow.hidden = matchCount !== 0;
  }

  if (resultsTarget) {
    const selectedCount = selectionItems.filter((item) => item.checked).length;
    if (matchCount === 0) {
      resultsTarget.textContent = selectedCount > 0 ? `No matches · ${selectedCount} selected` : "No matches";
    } else {
      resultsTarget.textContent =
        selectedCount > 0
          ? `${matchCount} shown · ${selectedCount} selected`
          : `${matchCount} shown`;
    }
  }

  if (selectAll instanceof HTMLInputElement) {
    const selectedCount = selectionItems.filter((item) => item.checked).length;
    selectAll.checked = selectionItems.length > 0 && selectedCount === selectionItems.length;
    selectAll.indeterminate = selectedCount > 0 && selectedCount < selectionItems.length;
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

    const selectAll = table.querySelector("[data-lead-select-all]");
    if (selectAll instanceof HTMLInputElement) {
      selectAll.addEventListener("change", () => {
        const items = table.querySelectorAll("[data-lead-select-item]");
        items.forEach((item) => {
          item.checked = selectAll.checked;
        });
        updateLeadTable(table);
      });
    }

    const selectionItems = table.querySelectorAll("[data-lead-select-item]");
    selectionItems.forEach((item) => {
      item.addEventListener("change", () => updateLeadTable(table));
    });
  });
}

function initializeMultiSelectForms() {
  const batchForms = document.querySelectorAll("[data-multi-select-form]");
  batchForms.forEach((form) => {
    if (!(form instanceof HTMLFormElement) || !form.id) {
      return;
    }

    const selectAll = form.querySelector("[data-select-all]");
    const countTarget = form.querySelector("[data-select-count]");
    const actionButtons = form.querySelectorAll("[data-requires-selection]");
    const selectionControls = form.querySelector("[data-selection-controls]");

    const getItems = () =>
      Array.from(document.querySelectorAll(`[form="${CSS.escape(form.id)}"][data-select-item]`)).filter(
        (item) => item instanceof HTMLInputElement,
      );

    const syncSelectionState = () => {
      const items = getItems();
      const selectedCount = items.filter((item) => item.checked).length;

      if (countTarget) {
        countTarget.textContent = `${selectedCount} selected`;
      }

      if (selectionControls instanceof HTMLElement) {
        selectionControls.hidden = selectedCount === 0;
      }

      actionButtons.forEach((button) => {
        button.disabled = selectedCount === 0;
      });

      if (selectAll instanceof HTMLInputElement) {
        selectAll.checked = items.length > 0 && selectedCount === items.length;
        selectAll.indeterminate = selectedCount > 0 && selectedCount < items.length;
      }
    };

    if (selectAll instanceof HTMLInputElement) {
      selectAll.addEventListener("change", () => {
        const items = getItems();
        items.forEach((item) => {
          item.checked = selectAll.checked;
        });
        syncSelectionState();
      });
    }

    getItems().forEach((item) => {
      item.addEventListener("change", syncSelectionState);
    });

    form.addEventListener("submit", (event) => {
      const selectedCount = getItems().filter((item) => item.checked).length;
      if (selectedCount === 0) {
        event.preventDefault();
      }
    });

    syncSelectionState();
  });
}

function initializeDismissibleNotices() {
  const dismissButtons = document.querySelectorAll("[data-dismiss-trigger]");
  dismissButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const notice = button.closest("[data-dismissible]");
      if (notice) {
        notice.remove();
      }
    });
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
  initializeActionMenus();
  initializeLeadTables();
  initializeMultiSelectForms();
  initializeDismissibleNotices();
  initializeOutboundModal();
  initializeScanModal();
});
