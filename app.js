async function loadJSON(path){ const r = await fetch(path + '?v=' + Date.now()); return r.json(); }
function badge(label){
  const cls = label === 'UP' ? 'badge up' : label === 'DOWN' ? 'badge down' : 'badge hold';
  return `<span class="${cls}">${label}</span>`;
}
function toLocal(ts){ try { return new Date(ts).toLocaleString('id-ID', {hour12:false}); } catch { return ts; } }

async function main(){
  const prices = await loadJSON('./data/prices.json');
  const pred   = await loadJSON('./data/prediction.json');

  // Prediksi
  const p = pred.next_1h_prediction || {};
  document.getElementById('pred').innerHTML = `
    <h2>Prediksi 1 Jam Berikutnya</h2>
    <div style="display:flex;gap:12px;margin:8px 0 4px 0">
      <div>BTC-USD: ${badge(p['BTC-USD'] || 'HOLD')}</div>
      <div>ETH-USD: ${badge(p['ETH-USD'] || 'HOLD')}</div>
    </div>
    <div class="muted">Dibuat: ${toLocal(pred.generated_at_utc)} · Metode: ${pred.method}</div>
  `;

  // Chart BTC & ETH
  function buildChart(canvasId, series){
    const ctx = document.getElementById(canvasId);
    const labels = series.map(d => new Date(d.ts_utc));
    const data   = series.map(d => d.close);
    new Chart(ctx, {
      type: 'line',
      data: { labels, datasets: [{ label: 'Close', data }] },
      options: {
        parsing: false,
        scales: {
          x: { type: 'time', time: { unit: 'hour', tooltipFormat: 'yyyy-MM-dd HH:mm' } },
          y: { beginAtZero: false }
        },
        plugins: { legend: { display: false } }
      }
    });
  }

  buildChart('chart-btc', prices.series['BTC-USD']);
  buildChart('chart-eth', prices.series['ETH-USD']);

  const lastBadge = document.getElementById('last-badge');
  lastBadge.textContent = "Last update: " + toLocal(prices.generated_at_utc);

  document.getElementById('foot').textContent =
    `Last data: ${toLocal(prices.latest['BTC-USD'].last_ts_utc)} (BTC), ${toLocal(prices.latest['ETH-USD'].last_ts_utc)} (ETH) · Interval ${prices.interval}, Period ${prices.period}`;
}
main().catch(console.error);
