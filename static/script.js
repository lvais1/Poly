// ── State ──────────────────────────────────────────────────────────────────

let traders      = [];
let trades       = [];
let stats        = {};
let seenTxHashes = new Set();
let firstLoad    = true;
let activeWallet = null;
let activeSide   = null;

// ── Sound engine ───────────────────────────────────────────────────────────

let audioCtx = null;

function getAudioCtx() {
  if (!audioCtx || audioCtx.state === 'closed') {
    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  }
  if (audioCtx.state === 'suspended') {
    audioCtx.resume();
  }
  return audioCtx;
}

function getSoundSettings() {
  return {
    enabled:  document.getElementById('sound-enabled').checked,
    volume:   parseInt(document.getElementById('sound-volume').value, 10) / 100,
    buyOn:    document.getElementById('sound-buy').checked,
    sellOn:   document.getElementById('sound-sell').checked,
    buyPitch: parseInt(document.getElementById('sound-buy-pitch').value, 10),
    sellPitch:parseInt(document.getElementById('sound-sell-pitch').value, 10),
    duration: parseInt(document.getElementById('sound-duration').value, 10) / 1000,
    wave:     document.getElementById('sound-wave').value,
  };
}

function playTone(freq, volume, duration, wave) {
  try {
    const ctx  = getAudioCtx();
    const osc  = ctx.createOscillator();
    const gain = ctx.createGain();

    osc.connect(gain);
    gain.connect(ctx.destination);

    osc.type            = wave;
    osc.frequency.value = freq;

    const now = ctx.currentTime;
    gain.gain.setValueAtTime(volume * 0.4, now);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + duration);

    osc.start(now);
    osc.stop(now + duration);
  } catch (e) {
    console.warn('Audio playback failed:', e);
  }
}

function playTradeSound(side) {
  const s = getSoundSettings();
  if (!s.enabled) return;
  if (side === 'BUY'  && !s.buyOn)  return;
  if (side === 'SELL' && !s.sellOn) return;

  const freq = side === 'BUY' ? s.buyPitch : s.sellPitch;
  playTone(freq, s.volume, s.duration, s.wave);
}

function previewSound(side) {
  playTradeSound(side);
}

// ── Sound panel UI ─────────────────────────────────────────────────────────

function initSoundPanel() {
  const btn   = document.getElementById('btn-sound');
  const panel = document.getElementById('sound-panel');

  btn.addEventListener('click', e => {
    e.stopPropagation();
    panel.classList.toggle('open');
    getAudioCtx();
  });

  document.addEventListener('click', e => {
    if (!panel.contains(e.target) && e.target !== btn) {
      panel.classList.remove('open');
    }
  });

  document.getElementById('sound-enabled').addEventListener('change', e => {
    btn.classList.toggle('muted', !e.target.checked);
    btn.textContent = e.target.checked ? '🔔' : '🔕';
  });

  document.getElementById('sound-volume').addEventListener('input', e => {
    document.getElementById('sound-volume-val').textContent = e.target.value + '%';
  });

  document.getElementById('sound-duration').addEventListener('input', e => {
    document.getElementById('sound-duration-val').textContent = e.target.value + 'ms';
  });
}

// ── External URL opener ────────────────────────────────────────────────────

function openExternal(url) {
  if (window.pywebview && window.pywebview.api) {
    window.pywebview.api.open_url(url);
  } else {
    window.open(url, '_blank');
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────

function timeAgo(epochSec) {
  const diff = Math.floor(Date.now() / 1000) - epochSec;
  if (diff < 60)    return `${diff}s ago`;
  if (diff < 3600)  return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function fmtUsd(n) {
  if (isNaN(n) || n === null) return '—';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
  if (abs >= 1_000)     return (n / 1_000).toFixed(1) + 'K';
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtSize(size) {
  const n = parseFloat(size);
  if (isNaN(n)) return '—';
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
  if (n >= 1_000)     return (n / 1_000).toFixed(1) + 'K';
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function fmtPrice(price) {
  const n = parseFloat(price);
  if (isNaN(n)) return '—';
  return '$' + n.toFixed(3);
}

function shortWallet(w) {
  return w.slice(0, 6) + '…' + w.slice(-4);
}

function traderDisplay(t) {
  return t.name || t.pseudonym || shortWallet(t.wallet);
}

const AVATAR_COLORS = ['#7c3aed','#2563eb','#059669','#d97706','#dc2626','#0891b2','#4f46e5','#be185d'];

function walletColor(wallet) {
  let h = 5381;
  for (let i = 0; i < (wallet || '').length; i++) h = ((h << 5) + h + wallet.charCodeAt(i)) >>> 0;
  return AVATAR_COLORS[h % AVATAR_COLORS.length];
}

function avatarSrc(url, name, wallet) {
  if (url) return url;
  const letter = ((name || wallet || '?')[0]).toUpperCase();
  const bg     = wallet ? walletColor(wallet) : '#444';
  const svg    = `<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><rect width="40" height="40" rx="20" fill="${bg}"/><text x="20" y="20" dy=".35em" text-anchor="middle" fill="#fff" font-size="17" font-family="system-ui,sans-serif">${letter}</text></svg>`;
  return 'data:image/svg+xml,' + encodeURIComponent(svg);
}

function imgError(el) {
  el.onerror = null;
  el.src = avatarSrc(null, el.dataset.name || '', el.dataset.wallet || '');
}

function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(showToast._timer);
  showToast._timer = setTimeout(() => el.classList.remove('show'), 3000);
}

function escHtml(str) {
  return String(str || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function polymarketEventUrl(eventSlug) {
  return `https://polymarket.com/event/${eventSlug}`;
}

function polymarketProfileUrl(t) {
  return `https://polymarket.com/profile/${t.pseudonym || t.wallet}`;
}

// ── Activity helpers ────────────────────────────────────────────────────────

function activityLevel(s) {
  if (!s || !s.last_trade_ts) return 'dormant';
  const diff = Math.floor(Date.now() / 1000) - s.last_trade_ts;
  if (diff < 3600)   return 'hot';     // < 1 hour
  if (diff < 86400)  return 'active';  // < 24 hours
  if (diff < 604800) return 'recent';  // < 7 days
  return 'dormant';
}

const ACTIVITY_LABEL = {
  hot:     'Hot',
  active:  'Active',
  recent:  'Recent',
  dormant: 'Dormant',
};

// ── Render ─────────────────────────────────────────────────────────────────

function renderTraders() {
  const list = document.getElementById('traders-list');
  document.getElementById('trader-count').textContent = traders.length;

  if (!traders.length) {
    list.innerHTML = '<p class="empty-msg">No traders tracked yet.<br>Add a wallet address above.</p>';
    return;
  }

  // Sort by most recent trade activity first
  const sorted = [...traders].sort((a, b) => {
    const ta = stats[a.wallet]?.last_trade_ts || 0;
    const tb = stats[b.wallet]?.last_trade_ts || 0;
    return tb - ta;
  });

  list.innerHTML = sorted.map(t => {
    const name       = traderDisplay(t);
    const isSelected = activeWallet === t.wallet;
    const avatar     = avatarSrc(t.profile_image, t.name || t.pseudonym, t.wallet);
    const s          = stats[t.wallet];
    const profileUrl = polymarketProfileUrl(t);
    const level      = activityLevel(s);
    const hasName    = !!(t.name || t.pseudonym);

    // ── Activity badge ──────────────────────────────────────────────────
    const activityBadge = `<span class="activity-badge activity-${level}" title="${
      s && s.last_trade_ts ? 'Last trade: ' + timeAgo(s.last_trade_ts) : 'No trades yet'
    }">${ACTIVITY_LABEL[level]}</span>`;

    // ── Stats row ───────────────────────────────────────────────────────
    let statsHtml = '';
    if (s && s.trade_count > 0) {
      const net      = parseFloat(s.net) || 0;
      const netClass = net > 0 ? 'stat-net-pos' : net < 0 ? 'stat-net-neg' : '';
      const netSign  = net > 0 ? '+' : '';
      statsHtml = `
        <div class="trader-stats-row">
          <span class="stat-item">${s.trade_count.toLocaleString()} trades</span>
          <span class="stat-sep">·</span>
          <span class="stat-item">$${fmtUsd(s.buy_vol + s.sell_vol)} vol</span>
          <span class="stat-sep">·</span>
          <span class="stat-item stat-net ${netClass}">${netSign}$${fmtUsd(net)}</span>
        </div>`;
    }

    // ── Activity row ────────────────────────────────────────────────────
    let activityHtml = '';
    if (s && s.last_trade_ts) {
      activityHtml = `
        <div class="trader-activity-row">
          <span class="act-item act-last" title="Last trade">&#9201; ${timeAgo(s.last_trade_ts)}</span>
          <span class="act-sep">·</span>
          <span class="act-item" title="Trades in last 24h">24h: <b>${s.trades_24h || 0}</b></span>
          <span class="act-sep">·</span>
          <span class="act-item" title="Trades in last 7 days">7d: <b>${s.trades_7d || 0}</b></span>
        </div>`;
    }

    const pseudo = t.pseudonym && t.pseudonym !== t.name ? `@${escHtml(t.pseudonym)}` : '';
    const subLine = [pseudo, t.x_username ? `𝕏 @${escHtml(t.x_username)}` : ''].filter(Boolean).join(' · ');

    return `
      <div class="trader-card${isSelected ? ' active' : ''}" onclick="selectTrader('${t.wallet}')">
        <button class="btn-remove" title="Remove" onclick="removeTrader(event,'${t.wallet}')">&#x2715;</button>

        <div class="trader-top">
          <div class="trader-avatar-wrap"
               onclick="event.stopPropagation(); openExternal('${escHtml(profileUrl)}')"
               title="View on Polymarket">
            <img class="trader-avatar" src="${avatar}" alt=""
                 data-name="${escHtml(t.name || t.pseudonym || '')}" data-wallet="${t.wallet}"
                 onerror="imgError(this)">
            <span class="avatar-dot avatar-dot-${level}"></span>
          </div>

          <div class="trader-info">
            <div class="trader-name-row">
              <span class="trader-name-link"
                    onclick="event.stopPropagation(); openExternal('${escHtml(profileUrl)}')"
                    title="View on Polymarket">${escHtml(name)}</span>
              ${t.verified_badge ? '<span class="verified-badge">&#10003;</span>' : ''}
              ${activityBadge}
            </div>
            ${subLine ? `<div class="trader-pseudo">${subLine}</div>` : ''}
            <div class="trader-wallet">${shortWallet(t.wallet)}</div>
          </div>
        </div>

        ${t.bio ? `<div class="trader-bio">${escHtml(t.bio.slice(0, 110))}${t.bio.length > 110 ? '…' : ''}</div>` : ''}
        ${statsHtml}
        ${activityHtml}
      </div>`;
  }).join('');
}

function renderTrades(newHashes) {
  const list   = document.getElementById('trades-list');
  const wallet = document.getElementById('filter-wallet').value || null;

  let filtered = trades;
  if (wallet)     filtered = filtered.filter(t => t.wallet === wallet);
  if (activeSide) filtered = filtered.filter(t => t.side  === activeSide);

  document.getElementById('trade-count').textContent =
    filtered.length + ' trade' + (filtered.length !== 1 ? 's' : '');

  if (!filtered.length) {
    list.innerHTML = '<p class="empty-msg">No trades to show.</p>';
    return;
  }

  list.innerHTML = filtered.map(t => {
    const trader    = traders.find(x => x.wallet === t.wallet);
    const tName     = trader ? traderDisplay(trader) : shortWallet(t.wallet);
    const tAvatar   = avatarSrc(trader?.profile_image, tName, t.wallet);
    const ago       = t.timestamp ? timeAgo(t.timestamp) : '—';
    const isNew     = newHashes && newHashes.has(t.tx_hash);
    const value     = (parseFloat(t.size) || 0) * (parseFloat(t.price) || 0);
    const eventUrl  = t.event_slug ? polymarketEventUrl(t.event_slug) : null;
    const profileUrl = trader ? polymarketProfileUrl(trader) : null;

    const titleHtml = eventUrl
      ? `<span class="trade-market trade-market-link" onclick="openExternal('${escHtml(eventUrl)}')" title="Open on Polymarket">${escHtml(t.market_title || '—')}</span>`
      : `<span class="trade-market" title="${escHtml(t.market_title || '')}">${escHtml(t.market_title || '—')}</span>`;

    const traderHtml = profileUrl
      ? `<span class="trade-trader trade-trader-link" onclick="openExternal('${escHtml(profileUrl)}')" title="Open profile">
           <img class="trade-trader-avatar" src="${tAvatar}" alt=""
                data-name="${escHtml(tName || '')}" data-wallet="${t.wallet}"
                onerror="imgError(this)">
           ${escHtml(tName)}
         </span>`
      : `<span class="trade-trader">
           <img class="trade-trader-avatar" src="${tAvatar}" alt=""
                data-name="${escHtml(tName || '')}" data-wallet="${t.wallet}"
                onerror="imgError(this)">
           ${escHtml(tName)}
         </span>`;

    return `
      <div class="trade-card ${t.side}${isNew ? ' new' : ''}">
        <div class="trade-header">
          <span class="trade-side ${t.side}">${t.side}</span>
          ${titleHtml}
          <span class="trade-value">$${fmtUsd(value)}</span>
        </div>
        ${t.outcome ? `<div class="trade-outcome">&#9656; ${escHtml(t.outcome)}</div>` : ''}
        <div class="trade-meta">
          <span class="trade-price">${fmtSize(t.size)} shares @ ${fmtPrice(t.price)}</span>
          <span class="trade-dot">·</span>
          <span>${ago}</span>
          <span class="trade-dot">·</span>
          ${traderHtml}
        </div>
      </div>`;
  }).join('');
}

function rebuildWalletFilter() {
  const sel = document.getElementById('filter-wallet');
  const cur = sel.value;
  sel.innerHTML = '<option value="">All Traders</option>' +
    traders.map(t =>
      `<option value="${t.wallet}"${t.wallet === cur ? ' selected' : ''}>${escHtml(traderDisplay(t))}</option>`
    ).join('');
}

// ── Discover / Recommendations ─────────────────────────────────────────────

let recommendations  = [];
let discoverLoading  = false;
let discoverLoaded   = false;

function renderDiscover() {
  const el = document.getElementById('discover-list');
  if (!el) return;

  if (discoverLoading) {
    el.innerHTML = '<p class="loading-msg">Scanning markets for active traders…</p>';
    return;
  }

  if (!recommendations.length) {
    el.innerHTML = `
      <p class="empty-msg">No suggestions yet.<br>Add traders and let trades load first.</p>
      <button class="btn-disc-refresh" onclick="loadDiscover(true)">&#8635; Try Again</button>`;
    return;
  }

  const trackedSet = new Set(traders.map(t => t.wallet));
  const filtered   = recommendations.filter(r => !trackedSet.has(r.wallet));

  const toolbar = `<div class="discover-toolbar">
    <span class="discover-info">${filtered.length} trader${filtered.length !== 1 ? 's' : ''} found</span>
    <button class="btn-disc-refresh" onclick="loadDiscover(true)" title="Shuffle for different results">&#8635; Shuffle</button>
  </div>`;

  const cards = filtered.map(r => {
    const name   = r.name || r.pseudonym || shortWallet(r.wallet);
    const avatar = avatarSrc(r.profileImage, r.name || r.pseudonym, r.wallet);
    const url    = `https://polymarket.com/profile/${r.pseudonym || r.wallet}`;
    const ago    = r.last_ts ? timeAgo(r.last_ts) : '—';

    const level = (() => {
      if (!r.last_ts) return 'dormant';
      const diff = Math.floor(Date.now() / 1000) - r.last_ts;
      if (diff < 3600)   return 'hot';
      if (diff < 86400)  return 'active';
      if (diff < 604800) return 'recent';
      return 'dormant';
    })();

    return `
      <div class="rec-card">
        <div class="rec-top">
          <div class="rec-avatar-wrap" onclick="openExternal('${escHtml(url)}')" title="View on Polymarket">
            <img class="rec-avatar" src="${avatar}" alt=""
                 data-name="${escHtml(r.name || r.pseudonym || '')}" data-wallet="${r.wallet}"
                 onerror="imgError(this)">
            <span class="rec-dot rec-dot-${level}"></span>
          </div>
          <div class="rec-info">
            <div class="rec-name-row">
              <span class="rec-name"
                   onclick="openExternal('${escHtml(url)}')"
                   title="View on Polymarket">${escHtml(name)}</span>
              <span class="activity-badge activity-${level}">${ACTIVITY_LABEL[level]}</span>
            </div>
            ${r.pseudonym && r.pseudonym !== r.name
              ? `<div class="rec-pseudo">@${escHtml(r.pseudonym)}</div>` : ''}
            <div class="rec-wallet-id">${shortWallet(r.wallet)}</div>
            <div class="rec-meta">
              <span title="Markets in common">&#9783; ${r.shared_markets} shared</span>
              <span class="act-sep">·</span>
              <span>${r.trade_count.toLocaleString()} trades</span>
              <span class="act-sep">·</span>
              <span>$${fmtUsd(r.volume)}</span>
              <span class="act-sep">·</span>
              <span title="Last active">${ago}</span>
            </div>
          </div>
          <button class="btn-rec-add" onclick="addFromRec('${r.wallet}')"
                  title="Track this wallet">+</button>
        </div>
        ${r.bio ? `<div class="rec-bio">${escHtml(r.bio.slice(0, 150))}${r.bio.length > 150 ? '…' : ''}</div>` : ''}
      </div>`;
  }).join('');

  el.innerHTML = toolbar + (cards || '<p class="empty-msg">All suggestions already tracked.</p>');
}

async function loadDiscover(shuffle = false) {
  if (discoverLoading) return;
  discoverLoading = true;
  renderDiscover();
  try {
    const url    = shuffle ? '/api/recommendations?shuffle=1' : '/api/recommendations';
    const res    = await fetch(url);
    recommendations = await res.json();
    discoverLoaded  = true;
  } catch (e) {
    recommendations = [];
  } finally {
    discoverLoading = false;
    renderDiscover();
  }
}

async function addFromRec(wallet) {
  const res  = await fetch('/api/traders', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ wallet }),
  });
  const data = await res.json();
  if (data.status === 'added') {
    showToast('Trader added — fetching trades…');
    setTimeout(refresh, 2500);
    setTimeout(refresh, 6000);
    renderDiscover(); // remove from suggestions immediately
  } else {
    showToast('Already tracking this wallet');
  }
  await loadTraders();
}

// ── Positions ──────────────────────────────────────────────────────────────

let positions       = [];
let positionsWallet = null;

function renderPositions() {
  const el = document.getElementById('positions-list');
  if (!el) return;

  if (!positionsWallet) {
    el.innerHTML = '<p class="empty-msg">Select a trader to see their open positions.</p>';
    return;
  }

  if (!positions.length) {
    el.innerHTML = '<p class="empty-msg">No open positions found.</p>';
    return;
  }

  el.innerHTML = positions.map(p => {
    const pnl      = parseFloat(p.cashPnl)  || 0;
    const pnlPct   = parseFloat(p.percentPnl) || 0;
    const pnlClass = pnl > 0 ? 'pos-pnl-pos' : pnl < 0 ? 'pos-pnl-neg' : '';
    const pnlSign  = pnl > 0 ? '+' : '';
    const eventUrl = p.eventSlug ? `https://polymarket.com/event/${p.eventSlug}` : null;

    const avgArrow = p.curPrice > p.avgPrice ? '▲' : p.curPrice < p.avgPrice ? '▼' : '→';
    const arrowCls = p.curPrice > p.avgPrice ? 'arrow-up' : p.curPrice < p.avgPrice ? 'arrow-down' : '';

    return `
      <div class="pos-card">
        <div class="pos-header">
          <div class="pos-title-wrap">
            ${p.icon ? `<img class="pos-icon" src="${escHtml(p.icon)}" alt="" onerror="this.style.display='none'">` : ''}
            <span class="pos-title${eventUrl ? ' pos-title-link' : ''}"
                  ${eventUrl ? `onclick="openExternal('${escHtml(eventUrl)}')" title="Open market"` : ''}
            >${escHtml(p.title || '—')}</span>
          </div>
          <span class="pos-pnl ${pnlClass}">${pnlSign}$${fmtUsd(pnl)} (${pnlSign}${pnlPct.toFixed(1)}%)</span>
        </div>
        <div class="pos-details">
          <span class="pos-outcome">&#9656; ${escHtml(p.outcome || '—')}</span>
          <span class="pos-sep">·</span>
          <span>${fmtSize(p.size)} shares</span>
          <span class="pos-sep">·</span>
          <span>avg $${parseFloat(p.avgPrice).toFixed(3)}</span>
          <span class="pos-sep ${arrowCls}">${avgArrow}</span>
          <span>now $${parseFloat(p.curPrice).toFixed(3)}</span>
          ${p.endDate ? `<span class="pos-sep">·</span><span class="pos-end">ends ${p.endDate.slice(0,10)}</span>` : ''}
        </div>
      </div>`;
  }).join('');
}

async function loadPositions(wallet) {
  positionsWallet = wallet;
  positions = [];
  renderPositions();
  try {
    const res = await fetch(`/api/positions/${wallet}`);
    positions = await res.json();
  } catch (e) {
    positions = [];
  }
  renderPositions();
}

// ── Tab switching ──────────────────────────────────────────────────────────

function switchLeftTab(tab) {
  document.querySelectorAll('.panel-tab').forEach(b => {
    b.classList.toggle('active', b.dataset.tab === tab);
  });
  document.querySelectorAll('.left-pane').forEach(p => {
    p.classList.toggle('active', p.id === (tab === 'tracked' ? 'traders-list' : 'discover-list'));
  });
  if (tab === 'discover' && !discoverLoading) {
    loadDiscover();
  }
}

function switchRightTab(tab) {
  document.querySelectorAll('.right-tab').forEach(b => {
    b.classList.toggle('active', b.dataset.rtab === tab);
  });
  document.querySelectorAll('.right-pane').forEach(p => {
    p.classList.toggle('active',
      p.id === (tab === 'trades' ? 'trades-list' : 'positions-list'));
  });
}

// ── Data fetching ──────────────────────────────────────────────────────────

async function loadTraders() {
  const res = await fetch('/api/traders');
  traders   = await res.json();
  renderTraders();
  rebuildWalletFilter();
}

async function loadStats() {
  const res = await fetch('/api/stats');
  stats     = await res.json();
}

async function loadTrades() {
  const res       = await fetch('/api/trades?limit=500');
  const freshData = await res.json();

  const newHashes = new Set();
  for (const t of freshData) {
    if (t.tx_hash && !seenTxHashes.has(t.tx_hash)) {
      newHashes.add(t.tx_hash);
      seenTxHashes.add(t.tx_hash);
    }
  }

  trades = freshData;

  if (!firstLoad && newHashes.size > 0) {
    const sides = [...new Set(freshData
      .filter(t => newHashes.has(t.tx_hash))
      .map(t => t.side)
    )];
    sides.forEach((side, i) => {
      setTimeout(() => playTradeSound(side), i * 120);
    });
    showToast(`${newHashes.size} new trade${newHashes.size > 1 ? 's' : ''}`);
  }

  renderTrades(firstLoad ? null : newHashes);

  if (firstLoad) firstLoad = false;
}

function updateLastRefreshed() {
  const el = document.getElementById('last-updated');
  if (el) {
    const now = new Date();
    el.textContent = `↺ ${now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}`;
  }
}

async function refresh() {
  await Promise.all([loadTraders(), loadTrades(), loadStats()]);
  renderTraders();
  updateLastRefreshed();
  // Refresh positions if a trader is selected
  if (activeWallet) loadPositions(activeWallet);
  // Refresh discover if open
  if (discoverLoaded) renderDiscover();
}

// ── User actions ───────────────────────────────────────────────────────────

async function addTrader() {
  const input  = document.getElementById('wallet-input');
  const wallet = input.value.trim().toLowerCase();

  if (!wallet.startsWith('0x') || wallet.length !== 42) {
    showToast('Enter a valid Ethereum address (0x + 40 hex chars)');
    input.focus();
    return;
  }

  const btn = document.getElementById('btn-add');
  btn.textContent = 'Adding…';
  btn.disabled = true;

  try {
    const res  = await fetch('/api/traders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ wallet }),
    });
    const data = await res.json();
    if (!res.ok) {
      showToast(data.error || 'Error adding trader');
    } else if (data.status === 'added') {
      showToast('Trader added — fetching profile & trades…');
      input.value = '';
      setTimeout(refresh, 2000);
      setTimeout(refresh, 5000);
      setTimeout(refresh, 12000);
    } else {
      showToast('Already tracking this wallet');
    }
    await loadTraders();
  } finally {
    btn.textContent = '+ Add Trader';
    btn.disabled = false;
  }
}

async function removeTrader(event, wallet) {
  event.stopPropagation();
  const trader = traders.find(t => t.wallet === wallet);
  const name   = trader ? traderDisplay(trader) : shortWallet(wallet);
  if (!confirm(`Remove ${name} and all their trades?`)) return;

  await fetch(`/api/traders/${wallet}`, { method: 'DELETE' });
  for (const t of trades) {
    if (t.wallet === wallet) seenTxHashes.delete(t.tx_hash);
  }
  if (activeWallet === wallet) {
    activeWallet    = null;
    positionsWallet = null;
    document.getElementById('filter-wallet').value = '';
    document.getElementById('btn-positions-tab').style.display = 'none';
    switchRightTab('trades');
  }
  await refresh();
  showToast('Trader removed');
}

function selectTrader(wallet) {
  activeWallet = (activeWallet === wallet) ? null : wallet;
  document.getElementById('filter-wallet').value = activeWallet || '';

  const posTab = document.getElementById('btn-positions-tab');
  if (activeWallet) {
    posTab.style.display = '';
    loadPositions(activeWallet);
  } else {
    posTab.style.display = 'none';
    positionsWallet = null;
    switchRightTab('trades');
  }

  renderTraders();
  renderTrades(null);
}

async function manualFetch() {
  const btn = document.getElementById('btn-fetch');
  btn.textContent = 'Fetching…';
  btn.disabled = true;
  try {
    await fetch('/api/fetch', { method: 'POST' });
    showToast('Fetch triggered — trades updating…');
    setTimeout(refresh, 3000);
    setTimeout(refresh, 8000);
  } finally {
    btn.textContent = '↻ Fetch Now';
    btn.disabled = false;
  }
}

// ── Event wiring ───────────────────────────────────────────────────────────

document.getElementById('btn-add').addEventListener('click', addTrader);
document.getElementById('btn-fetch').addEventListener('click', manualFetch);

document.getElementById('wallet-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') addTrader();
});

document.getElementById('filter-wallet').addEventListener('change', () => renderTrades(null));

document.querySelectorAll('.filter-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    activeSide = btn.dataset.side || null;
    renderTrades(null);
  });
});

// Left panel tabs
document.querySelectorAll('.panel-tab').forEach(btn => {
  btn.addEventListener('click', () => switchLeftTab(btn.dataset.tab));
});

// Right panel tabs
document.querySelectorAll('.right-tab').forEach(btn => {
  btn.addEventListener('click', () => switchRightTab(btn.dataset.rtab));
});

// ── Boot ───────────────────────────────────────────────────────────────────

initSoundPanel();
refresh();
setInterval(refresh, 15_000);
