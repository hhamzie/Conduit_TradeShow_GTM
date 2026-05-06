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

function setProgressBar(bar, completed, total) {
  if (!bar) {
    return;
  }
  const safeTotal = Math.max(total || 0, 1);
  bar.hidden = false;
  bar.max = safeTotal;
  bar.value = Math.min(completed || 0, safeTotal);
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

async function pollBulkScrapeJob(jobId, progressTarget, progressBar) {
  while (true) {
    const response = await fetch(`/scrape/bulk/status/${jobId}`, {
      credentials: "same-origin",
    });
    if (!response.ok) {
      throw new Error(`Unable to fetch bulk scrape status (${response.status}).`);
    }
    const payload = await response.json();
    setProgressBar(progressBar, payload.completed || 0, payload.total || 1);
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
  const progressBarTargetId = form.dataset.progressBarTarget || "";
  const progressTarget = progressTargetId ? document.getElementById(progressTargetId) : null;
  const progressBar = progressBarTargetId ? document.getElementById(progressBarTargetId) : null;
  const loadingLabel = form.dataset.loadingLabel || "Working...";

  setFormBusyState(form, true, loadingLabel);
  setProgressBar(progressBar, 0, 1);
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
    const finalStatus = await pollBulkScrapeJob(payload.job_id, progressTarget, progressBar);
    const downloadResponseObject = await fetch(finalStatus.download_url, {
      credentials: "same-origin",
    });
    if (!downloadResponseObject.ok) {
      throw new Error(`The ZIP download failed (${downloadResponseObject.status}).`);
    }
    await downloadResponse(downloadResponseObject);
    setProgressMessage(progressTarget, "Bulk scrape finished. ZIP download started.", "success");
  } catch (error) {
    const message =
      error instanceof Error && error.message
        ? error.message
        : "Something went wrong while starting the bulk scrape.";
    setProgressMessage(progressTarget, message, "error");
  } finally {
    setFormBusyState(form, false, loadingLabel);
  }
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
});
