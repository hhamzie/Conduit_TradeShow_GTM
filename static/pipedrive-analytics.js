(() => {
  "use strict";

  const dataNode = document.querySelector("#pipedrive-analytics-data");
  if (!dataNode) return;

  let dashboard;
  try {
    dashboard = JSON.parse(dataNode.textContent || "{}");
  } catch {
    return;
  }

  const palette = {
    grid: "#2c2c2a",
    muted: "#888681",
    secondary: "#c2c1b8",
    text: "#f5f5f3",
    green: "#4aa030",
    greenFill: "#23322b",
    blue: "#4e85de",
    blueFill: "#20252d",
    empty: "#303032",
  };

  const tooltip = document.querySelector("[data-chart-tooltip]");
  const hitMaps = new WeakMap();
  let resizeFrame = 0;

  function setupCanvas(canvas) {
    const box = canvas.getBoundingClientRect();
    const ratio = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
    const width = Math.max(1, Math.round(box.width));
    const height = Math.max(1, Math.round(box.height));
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
    const context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    context.lineCap = "round";
    context.lineJoin = "round";
    return { context, width, height };
  }

  function rateText(value) {
    return value === null || value === undefined ? "No deals" : `${Math.round(value)}%`;
  }

  function hourLabel(hour) {
    if (hour === 0) return "12am";
    if (hour === 12) return "12pm";
    return hour < 12 ? `${hour}am` : `${hour - 12}pm`;
  }

  function niceMaximum(value) {
    const safe = Math.max(1, value);
    const magnitude = 10 ** Math.floor(Math.log10(safe));
    return Math.ceil(safe / magnitude) * magnitude;
  }

  function drawAxes(context, bounds, options = {}) {
    const { left, top, right, bottom } = bounds;
    const rows = options.rows || 10;
    context.save();
    context.font = "500 11px Inter, system-ui, sans-serif";
    context.fillStyle = palette.muted;
    context.strokeStyle = palette.grid;
    context.lineWidth = 1;
    context.textAlign = "right";
    context.textBaseline = "middle";

    for (let index = 0; index <= rows; index += 1) {
      const ratio = index / rows;
      const y = bottom - ratio * (bottom - top);
      context.beginPath();
      context.moveTo(left, y);
      context.lineTo(right, y);
      context.stroke();
      context.fillText(`${Math.round(ratio * 100)}%`, left - 10, y);
      if (options.rightMaximum !== undefined) {
        context.textAlign = "left";
        context.fillText(
          String(Math.round(ratio * options.rightMaximum)),
          right + 10,
          y,
        );
        context.textAlign = "right";
      }
    }
    context.restore();
  }

  function pointPosition(index, total, left, right) {
    if (total <= 1) return (left + right) / 2;
    return left + (index / (total - 1)) * (right - left);
  }

  function drawLegend(context, items, centerX, y) {
    context.save();
    context.font = "500 12px Inter, system-ui, sans-serif";
    const widths = items.map((item) => 28 + context.measureText(item.label).width);
    const total = widths.reduce((sum, width) => sum + width, 0) + (items.length - 1) * 18;
    let x = centerX - total / 2;
    for (let index = 0; index < items.length; index += 1) {
      const item = items[index];
      context.strokeStyle = item.color;
      context.fillStyle = item.fill;
      context.lineWidth = 2;
      context.fillRect(x, y - 6, 24, 12);
      context.strokeRect(x, y - 6, 24, 12);
      context.fillStyle = palette.secondary;
      context.textAlign = "left";
      context.textBaseline = "middle";
      context.fillText(item.label, x + 30, y);
      x += widths[index] + 18;
    }
    context.restore();
  }

  function drawAreaLine(context, points, bottom, stroke, fill) {
    if (!points.length) return;
    context.save();
    context.fillStyle = fill;
    context.beginPath();
    context.moveTo(points[0].x, bottom);
    for (const point of points) context.lineTo(point.x, point.y);
    context.lineTo(points[points.length - 1].x, bottom);
    context.closePath();
    context.fill();

    context.strokeStyle = stroke;
    context.lineWidth = 2;
    context.beginPath();
    points.forEach((point, index) => {
      if (index === 0) context.moveTo(point.x, point.y);
      else context.lineTo(point.x, point.y);
    });
    context.stroke();

    for (const point of points) {
      context.fillStyle = "#1a1a19";
      context.beginPath();
      context.arc(point.x, point.y, 3.5, 0, Math.PI * 2);
      context.fill();
      context.stroke();
    }
    context.restore();
  }

  function drawWeekdayBlended(canvas) {
    const rows = dashboard.weekday_blended || [];
    const { context, width, height } = setupCanvas(canvas);
    const bounds = { left: 56, top: 52, right: width - 54, bottom: height - 34 };
    const rightMaximum = niceMaximum(
      Math.max(0, ...rows.map((row) => Number(row.avg_deals_per_day) || 0)),
    );
    drawAxes(context, bounds, { rows: 10, rightMaximum });
    drawLegend(
      context,
      [
        { label: "Follow-up %", color: palette.green, fill: "rgba(22, 165, 29, 0.14)" },
        { label: "Avg deals/day", color: palette.blue, fill: "rgba(61, 140, 232, 0.14)" },
      ],
      width / 2,
      22,
    );

    const coveragePoints = [];
    const volumePoints = [];
    const hits = [];
    context.save();
    context.font = "500 11px Inter, system-ui, sans-serif";
    context.fillStyle = palette.muted;
    context.textAlign = "center";
    context.textBaseline = "top";
    rows.forEach((row, index) => {
      const x = pointPosition(index, rows.length, bounds.left, bounds.right);
      const coverage = Number(row.coverage) || 0;
      const volume = Number(row.avg_deals_per_day) || 0;
      coveragePoints.push({
        x,
        y: bounds.bottom - (coverage / 100) * (bounds.bottom - bounds.top),
      });
      volumePoints.push({
        x,
        y: bounds.bottom - (volume / rightMaximum) * (bounds.bottom - bounds.top),
      });
      context.fillText(row.label, x, bounds.bottom + 12);
      hits.push({
        x: x - 22,
        y: bounds.top,
        width: 44,
        height: bounds.bottom - bounds.top,
        text: `${row.label}: ${rateText(row.coverage)} followed up · ${volume.toFixed(1)} avg deals/day · ${row.deals} deals`,
      });
    });
    context.restore();
    drawAreaLine(context, volumePoints, bounds.bottom, palette.blue, palette.blueFill);
    drawAreaLine(context, coveragePoints, bounds.bottom, palette.green, palette.greenFill);
    hitMaps.set(canvas, hits);
  }

  function drawBars(canvas, rows, color, labelForRow) {
    const { context, width, height } = setupCanvas(canvas);
    const bounds = { left: 56, top: 18, right: width - 14, bottom: height - 36 };
    drawAxes(context, bounds, { rows: 10 });
    const slot = (bounds.right - bounds.left) / Math.max(rows.length, 1);
    const barWidth = Math.max(5, Math.min(30, slot * 0.34));
    const hits = [];

    context.save();
    context.font = "500 10.5px Inter, system-ui, sans-serif";
    context.fillStyle = palette.muted;
    context.textAlign = "center";
    context.textBaseline = "top";
    rows.forEach((row, index) => {
      const x = bounds.left + slot * (index + 0.5);
      const coverage = row.coverage === null || row.coverage === undefined
        ? 0
        : Number(row.coverage);
      const y = bounds.bottom - (coverage / 100) * (bounds.bottom - bounds.top);
      context.fillStyle = color;
      context.fillRect(x - barWidth / 2, y, barWidth, bounds.bottom - y);
      context.fillStyle = palette.muted;
      context.fillText(labelForRow(row), x, bounds.bottom + 12);
      hits.push({
        x: x - slot / 2,
        y: bounds.top,
        width: slot,
        height: bounds.bottom - bounds.top,
        text: `${labelForRow(row)}: ${rateText(row.coverage)} followed up · ${row.deals} deals`,
      });
    });
    context.restore();
    hitMaps.set(canvas, hits);
  }

  function heatColor(value) {
    if (value === null || value === undefined) return palette.empty;
    if (value < 20) return "#972b25";
    if (value < 40) return "#9c632d";
    if (value < 60) return "#a4a43c";
    if (value < 80) return "#70a239";
    return "#4fa237";
  }

  function drawHeatmap(canvas) {
    const cells = dashboard.heatmap || [];
    const { context, width, height } = setupCanvas(canvas);
    const left = 54;
    const top = 30;
    const right = width - 2;
    const bottom = height - 4;
    const columns = 7;
    const rows = 24;
    const gapX = 2;
    const gapY = 3;
    const cellWidth = (right - left - gapX * (columns - 1)) / columns;
    const cellHeight = (bottom - top - gapY * (rows - 1)) / rows;
    const weekdayLabels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const byKey = new Map(
      cells.map((cell) => [`${cell.hour}:${cell.day_index}`, cell]),
    );
    const hits = [];

    context.save();
    context.font = "500 10.5px Inter, system-ui, sans-serif";
    context.fillStyle = palette.muted;
    context.textAlign = "center";
    context.textBaseline = "bottom";
    weekdayLabels.forEach((label, index) => {
      const x = left + index * (cellWidth + gapX) + cellWidth / 2;
      context.fillText(label, x, top - 8);
    });
    context.textAlign = "right";
    context.textBaseline = "middle";

    for (let hour = 0; hour < rows; hour += 1) {
      const y = top + hour * (cellHeight + gapY);
      context.fillStyle = palette.muted;
      context.fillText(hourLabel(hour), left - 8, y + cellHeight / 2);
      for (let day = 0; day < columns; day += 1) {
        const x = left + day * (cellWidth + gapX);
        const cell = byKey.get(`${hour}:${day}`) || {
          hour,
          day_index: day,
          deals: 0,
          coverage: null,
        };
        context.fillStyle = heatColor(cell.coverage);
        context.fillRect(x, y, cellWidth, cellHeight);
        hits.push({
          x,
          y,
          width: cellWidth,
          height: cellHeight,
          text: `${weekdayLabels[day]} ${hourLabel(hour)}: ${rateText(cell.coverage)} followed up · ${cell.deals} deals`,
        });
      }
    }
    context.restore();
    hitMaps.set(canvas, hits);
  }

  function drawAll() {
    document.querySelectorAll("[data-analytics-chart]").forEach((canvas) => {
      const type = canvas.dataset.analyticsChart;
      if (type === "weekday-blended") drawWeekdayBlended(canvas);
      if (type === "hourly") {
        drawBars(canvas, dashboard.hourly || [], palette.green, (row) => row.label);
      }
      if (type === "weekday") {
        drawBars(canvas, dashboard.weekdays || [], palette.blue, (row) => row.label);
      }
      if (type === "heatmap") drawHeatmap(canvas);
    });
  }

  function localPointer(event, canvas) {
    const box = canvas.getBoundingClientRect();
    return {
      x: event.clientX - box.left,
      y: event.clientY - box.top,
    };
  }

  function hideTooltip() {
    if (!tooltip) return;
    tooltip.hidden = true;
  }

  function showTooltip(event, text) {
    if (!tooltip) return;
    tooltip.textContent = text;
    tooltip.hidden = false;
    const margin = 12;
    const tooltipBox = tooltip.getBoundingClientRect();
    const left = Math.min(
      window.innerWidth - tooltipBox.width - margin,
      event.clientX + 14,
    );
    const top = Math.min(
      window.innerHeight - tooltipBox.height - margin,
      event.clientY + 14,
    );
    tooltip.style.left = `${Math.max(margin, left)}px`;
    tooltip.style.top = `${Math.max(margin, top)}px`;
  }

  document.querySelectorAll("[data-analytics-chart]").forEach((canvas) => {
    canvas.addEventListener("mousemove", (event) => {
      const point = localPointer(event, canvas);
      const hit = (hitMaps.get(canvas) || []).find(
        (candidate) =>
          point.x >= candidate.x &&
          point.x <= candidate.x + candidate.width &&
          point.y >= candidate.y &&
          point.y <= candidate.y + candidate.height,
      );
      if (hit) showTooltip(event, hit.text);
      else hideTooltip();
    });
    canvas.addEventListener("mouseleave", hideTooltip);
    canvas.addEventListener("focus", hideTooltip);
  });

  window.addEventListener("resize", () => {
    window.cancelAnimationFrame(resizeFrame);
    resizeFrame = window.requestAnimationFrame(drawAll);
  });

  drawAll();
})();
