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
      throw new Error("The scraper request failed. Please try again.");
    }

    if (!looksLikeDownload) {
      window.location.reload();
      return;
    }

    const blob = await response.blob();
    const downloadUrl = window.URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = downloadUrl;
    anchor.download = parseFilenameFromDisposition(disposition) || "scrape-output";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.URL.revokeObjectURL(downloadUrl);

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

document.addEventListener("DOMContentLoaded", () => {
  const forms = document.querySelectorAll("[data-direct-download-form]");
  forms.forEach((form) => {
    form.addEventListener("submit", handleDirectDownloadFormSubmit);
  });
});
