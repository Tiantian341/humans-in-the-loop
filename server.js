const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

// ─── Configuration (env overridable) ───────────────────────────────────
const CONFIG = {
  port:            parseInt(process.env.PORT, 10) || 3001,
  answerTimeout:   parseInt(process.env.ANSWER_TIMEOUT_MS, 10) || 60_000,
  questionCost:    parseInt(process.env.QUESTION_COST, 10) || 1,
  answerReward:    parseInt(process.env.ANSWER_REWARD, 10) || 1,
  maxViolations:   parseInt(process.env.MAX_VIOLATIONS, 10) || 3,
  initialCredits:  parseInt(process.env.INITIAL_CREDITS, 10) || 2,
  maxMsgLen:       parseInt(process.env.MAX_MESSAGE_LENGTH, 10) || 1000,
  // Rate limiting
  ipRateWindow:    parseInt(process.env.IP_RATE_WINDOW_MS, 10) || 60_000,
  ipRateMax:       parseInt(process.env.IP_RATE_MAX, 10) || 6,
  askRateWindow:   parseInt(process.env.ASK_RATE_WINDOW_MS, 10) || 60_000,
  askRateMax:      parseInt(process.env.ASK_RATE_MAX, 10) || 8,
  answerRateWindow:parseInt(process.env.ANSWER_RATE_WINDOW_MS, 10) || 60_000,
  answerRateMax:   parseInt(process.env.ANSWER_RATE_MAX, 10) || 10,
  // Session management
  sessionTtlMs:    parseInt(process.env.SESSION_TTL_MS, 10) || 30 * 60_000,      // 30 min
  sessionGcInterval:parseInt(process.env.SESSION_GC_INTERVAL_MS, 10) || 5 * 60_000, // 5 min
  persistInterval: parseInt(process.env.PERSIST_INTERVAL_MS, 10) || 30_000,       // 30 sec
  dataDir:         process.env.DATA_DIR || path.join(__dirname, 'data'),
};

// ─── Data directory / restore ──────────────────────────────────────────
if (!fs.existsSync(CONFIG.dataDir)) {
  fs.mkdirSync(CONFIG.dataDir, { recursive: true });
}

const sessionsFile = path.join(CONFIG.dataDir, 'sessions.json');
const creditsFile  = path.join(CONFIG.dataDir, 'credits.json');

// Restore session credits (survive restart); everything else resets
function loadCredits() {
  try {
    if (fs.existsSync(creditsFile)) {
      const raw = fs.readFileSync(creditsFile, 'utf-8');
      return JSON.parse(raw);
    }
  } catch (e) { console.error('[persist] failed to load credits:', e.message); }
  return {};
}
function saveCredits() {
  try {
    const map = {};
    for (const [id, s] of sessions) {
      map[id] = { credits: s.credits, likes: s.likes || 0, dislikes: s.dislikes || 0 };
    }
    fs.writeFileSync(creditsFile + '.tmp', JSON.stringify(map, null, 2));
    fs.renameSync(creditsFile + '.tmp', creditsFile);
  } catch (e) { console.error('[persist] failed to save credits:', e.message); }
}

// In-memory state
const sessions = new Map();          // { id, credits, connectedAt, lastActive, role, ready, likes, dislikes, violations }
const waitingQueue = [];             // { sessionId, question, timestamp, drawMode } — human questions
const readyLarpQueue = [];           // [sessionId] — FIFO queue of ready larpers
const activeMatches = new Map();     // matchId => { questionerId, larpId, question, drawMode, timer }
const ratedMsgs = new Map();        // matchId => { likes:Set, dislikes:Set }
const wsBySession = new Map();

// ─── Rate limiter helpers ──────────────────────────────────────────────
const ipBlacklist = new Map();       // ip => { until: timestamp }
const ipConnections = new Map();     // ip => [{ time }]
const askTimestamps = new Map();     // sessionId => [{ time }]
const answerTimestamps = new Map();  // sessionId => [{ time }]

// Ban an IP for N ms
function banIP(ip, ms = 300_000) {
  ipBlacklist.set(ip, { until: Date.now() + ms });
  console.log(`[rate-limit] banned IP ${ip.slice(0, 12)}... for ${ms / 1000}s`);
}

// Prune old entries from a Map of arrays
function pruneWindow(map, key, windowMs) {
  const entries = map.get(key);
  if (!entries) return;
  const cutoff = Date.now() - windowMs;
  const kept = entries.filter(e => e.time > cutoff);
  if (kept.length === 0) map.delete(key);
  else map.set(key, kept);
}

// Check rate limit. Returns true if rate is exceeded.
function checkRate(map, key, windowMs, max) {
  if (!map.has(key)) map.set(key, []);
  pruneWindow(map, key, windowMs);
  const entries = map.get(key);
  if (!entries) return false;
  return entries.length >= max;
}

// Record a hit for a rate bucket
function hitRate(map, key) {
  if (!map.has(key)) map.set(key, []);
  map.get(key).push({ time: Date.now() });
}

// ─── Session persistence (only credits survive restart) ────────────────
const restoredCredits = loadCredits();

// ─── Content Moderation ────────────────────────────────────────────────
const BANNED_WORDS_ZH = [
  '赌博', '博彩', '赌场', '六合彩', '时时彩',
  '色情', '裸聊', '约炮', '援交', '卖淫', '嫖娼',
  '毒品', '冰毒', '海洛因', '大麻', '吸毒',
  '枪支', '弹药', '炸药', '武器',
  '传销', '洗钱', '诈骗', '套现',
  '翻墙', 'VPN', '科学上网',
  '习近平', '江泽民', '胡锦涛', '温家宝', '李克强',
  '栗战书', '汪洋', '王沪宁', '赵乐际', '韩正',
  '六四', '天安门', '法轮功', '李洪志', '达赖',
  '台独', '港独', '藏独', '疆独', '东突',
  '邪教', '恐怖', '分裂', '颠覆',
];

const BANNED_PATTERNS = [
  /[1１]?[0０]{3,}[8８]{2,}/,
  /[6６][-]?[4４]/g,
  /\b(f[@a]ck|sh[i1]t|n[i1]gg[e3]r|d[@a]mn?)\b/i,
];

function moderate(text, sessionId) {
  if (!text || typeof text !== 'string') return { ok: false, reason: '内容为空' };
  const normalized = text.toLowerCase();
  for (const word of BANNED_WORDS_ZH) {
    if (normalized.includes(word)) {
      const s = sessions.get(sessionId);
      if (s) { s.violations = (s.violations || 0) + 1; }
      return { ok: false, reason: '内容包含违规词语，请修改后重试', banned: true };
    }
  }
  for (const pattern of BANNED_PATTERNS) {
    if (pattern.test(text)) {
      const s = sessions.get(sessionId);
      if (s) { s.violations = (s.violations || 0) + 1; }
      return { ok: false, reason: '内容包含违规内容，请修改后重试', banned: true };
    }
  }
  if (text.length > CONFIG.maxMsgLen) {
    return { ok: false, reason: `内容过长，限制 ${CONFIG.maxMsgLen} 字以内` };
  }
  return { ok: true };
}

function checkViolations(sessionId, ws) {
  const s = sessions.get(sessionId);
  if (!s) return false;
  if ((s.violations || 0) >= CONFIG.maxViolations) {
    send(ws, { type: 'error', msg: `⚠️ 你已被踢出（累计 ${CONFIG.maxViolations} 次违规）。请遵守社区规则。` });
    try { ws.close(); } catch {}
    cleanupSession(sessionId);
    broadcastStats();
    return true;
  }
  return false;
}

function cleanupSession(sessionId) {
  wsBySession.delete(sessionId);
  // Remove from ready queue
  const ri = readyLarpQueue.indexOf(sessionId);
  if (ri >= 0) readyLarpQueue.splice(ri, 1);
  // Cancel any active match where this session is the larp
  for (const [mid, match] of activeMatches) {
    if (match.larpId === sessionId) {
      clearTimeout(match.timer);
      activeMatches.delete(mid);
      // Re-queue the question
      waitingQueue.unshift({ sessionId: match.questionerId, question: match.question, timestamp: Date.now(), drawMode: match.drawMode || false });
      const qWs = wsBySession.get(match.questionerId);
      if (qWs) send(qWs, { type: 'reassigning', matchId: mid, question: match.question });
    }
  }
  for (let i = waitingQueue.length - 1; i >= 0; i--) {
    if (waitingQueue[i].sessionId === sessionId) waitingQueue.splice(i, 1);
  }
  dispatch();
}

function getOrCreate(sessionId) {
  if (!sessions.has(sessionId)) {
    const restored = restoredCredits[sessionId] || {};
    sessions.set(sessionId, {
      id: sessionId,
      credits: typeof restored.credits === 'number' ? restored.credits : CONFIG.initialCredits,
      connectedAt: Date.now(),
      lastActive: Date.now(),
      answersThisHour: 0,
      role: 'human',
      ready: false,
      likes: restored.likes || 0,
      dislikes: restored.dislikes || 0,
      violations: 0,
    });
  }
  return sessions.get(sessionId);
}

// ─── Session GC ────────────────────────────────────────────────────────
setInterval(() => {
  const cutoff = Date.now() - CONFIG.sessionTtlMs;
  let purged = 0;
  for (const [id, s] of sessions) {
    if (s.lastActive <= cutoff && !wsBySession.has(id)) {
      // Not connected and inactive for TTL
      sessions.delete(id);
      // Clean up rate buckets
      askTimestamps.delete(id);
      answerTimestamps.delete(id);
      purged++;
    }
  }
  // Also GC rate limiter maps
  for (const ip of ipConnections.keys()) pruneWindow(ipConnections, ip, CONFIG.ipRateWindow);
  for (const ip of ipBlacklist.keys()) {
    if (ipBlacklist.get(ip).until <= Date.now()) ipBlacklist.delete(ip);
  }
  for (const sid of askTimestamps.keys()) pruneWindow(askTimestamps, sid, CONFIG.askRateWindow);
  for (const sid of answerTimestamps.keys()) pruneWindow(answerTimestamps, sid, CONFIG.answerRateWindow);

  if (purged > 0) {
    console.log(`[gc] purged ${purged} stale sessions (${sessions.size} remaining)`);
    saveCredits();
  }
}, CONFIG.sessionGcInterval);

// ─── Periodic persistence ──────────────────────────────────────────────
setInterval(() => {
  saveCredits();
}, CONFIG.persistInterval);

// ─── Messaging helpers ─────────────────────────────────────────────────
function broadcast(msg) {
  const data = JSON.stringify(msg);
  wss.clients.forEach(ws => {
    if (ws.readyState === 1) ws.send(data);
  });
}

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function broadcastStats() {
  let humanCount = 0, larpCount = 0, readyCount = 0;
  // Count larps currently in an active match
  const busyLarpIds = new Set();
  for (const [, match] of activeMatches) busyLarpIds.add(match.larpId);
  for (const [, s] of sessions) {
    if (wsBySession.has(s.id)) {
      if (s.role === 'larp') {
        larpCount++;
        if (s.ready) readyCount++;
      } else humanCount++;
    }
  }
  // AI waiting = ready larps not in an active match
  const larpWaitingCount = readyLarpQueue.length;
  broadcast({
    type: 'stats',
    queueLength: waitingQueue.length,
    activeMatches: activeMatches.size,
    pendingTotal: waitingQueue.length + activeMatches.size,
    onlineUsers: humanCount + larpCount,
    humanCount, larpCount, readyCount, larpWaitingCount
  });
}

// ─── Scheduling: FIFO question+AI pairing ─────────────────────────────

function dispatch() {
  while (waitingQueue.length > 0 && readyLarpQueue.length > 0) {
    const q = waitingQueue.shift();
    const larpId = readyLarpQueue.shift();

    const matchId = `${q.sessionId}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
    const timer = setTimeout(() => handleTimeout(matchId), CONFIG.answerTimeout);

    activeMatches.set(matchId, {
      matchId, questionerId: q.sessionId, larpId,
      question: q.question, drawMode: q.drawMode || false, timer
    });

    const larpWs = wsBySession.get(larpId);
    if (larpWs && larpWs.readyState === 1) {
      send(larpWs, { type: 'new_task', matchId, question: q.question, timeoutMs: CONFIG.answerTimeout, drawMode: q.drawMode || false });
    }

    const qWs = wsBySession.get(q.sessionId);
    if (qWs) {
      send(qWs, { type: 'matched', matchId, question: q.question, larpCount: 1, drawMode: q.drawMode || false });
    }
  }
  broadcastStats();
}

function handleTimeout(matchId) {
  const match = activeMatches.get(matchId);
  if (!match) return;
  activeMatches.delete(matchId);

  const larpWs = wsBySession.get(match.larpId);
  if (larpWs) send(larpWs, { type: 'timeout', matchId });

  // Return larp to ready queue if still connected and ready
  const s = sessions.get(match.larpId);
  if (s && s.role === 'larp' && s.ready && wsBySession.has(match.larpId)) {
    readyLarpQueue.push(match.larpId);
    if (larpWs) send(larpWs, { type: 'larp_idle' });
  }

  waitingQueue.unshift({
    sessionId: match.questionerId,
    question: match.question,
    timestamp: Date.now(),
    drawMode: match.drawMode || false
  });

  const qWs = wsBySession.get(match.questionerId);
  if (qWs) send(qWs, { type: 'reassigning', matchId, question: match.question });

  dispatch();
}

// ─── /Scheduling ──────────────────────────────────────────────────────

function rateMsg(msgId, raterId, kind) {
  if (!ratedMsgs.has(msgId)) {
    ratedMsgs.set(msgId, { likes: new Set(), dislikes: new Set(), responderId: null });
  }
  const rm = ratedMsgs.get(msgId);
  rm.likes.delete(raterId);
  rm.dislikes.delete(raterId);
  if (kind === 'like') rm.likes.add(raterId);
  else if (kind === 'dislike') rm.dislikes.add(raterId);
  return { likes: rm.likes.size, dislikes: rm.dislikes.size };
}

// ─── HTTP + WS ─────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  // Health check
  if (req.url === '/health' || req.url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, uptime: process.uptime(), sessions: sessions.size }));
  }

  // Admin dashboard
  const adminToken = process.env.ADMIN_TOKEN;
  if (req.url.startsWith('/admin/') && adminToken) {
    const providedToken = new URL(req.url, `http://${req.headers.host}`).searchParams.get('token');
    if (!providedToken || providedToken !== adminToken) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Unauthorized. Use ?token=YOUR_ADMIN_TOKEN' }));
    }

    // Get the base path without query string
    const basePath = req.url.split('?')[0];

    if (basePath === '/admin/stats') {
      const now = Date.now();
      let humanOnline = 0, larpOnline = 0;
      const busyLarpIds = new Set();
      for (const [, match] of activeMatches) busyLarpIds.add(match.larpId);
      for (const [id, s] of sessions) {
        if (wsBySession.has(id)) {
          if (s.role === 'larp') larpOnline++; else humanOnline++;
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({
        ok: true,
        uptime: process.uptime(),
        memoryMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
        sessions: { total: sessions.size, humanOnline, larpOnline },
        queues: {
          waitingQuestions: waitingQueue.length,
          readyLarps: readyLarpQueue.length,
          activeMatches: activeMatches.size
        },
        rateLimiter: { ipBlacklist: ipBlacklist.size, ipConnections: ipConnections.size },
        config: {
          answerTimeout: CONFIG.answerTimeout,
          questionCost: CONFIG.questionCost,
          answerReward: CONFIG.answerReward,
          maxViolations: CONFIG.maxViolations,
          ipRateMax: CONFIG.ipRateMax,
          askRateMax: CONFIG.askRateMax,
          answerRateMax: CONFIG.answerRateMax
        },
        timestamp: new Date().toISOString()
      }, null, 2));
    }

    if (basePath === '/admin/queues') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({
        waitingQueue: waitingQueue.map(q => ({ sessionId: q.sessionId.slice(0, 12), questionPreview: q.question.slice(0, 60), drawMode: q.drawMode })),
        readyLarpQueue: readyLarpQueue.map(id => ({ sessionId: id.slice(0, 12) })),
        activeMatches: [...activeMatches].map(([mid, m]) => ({
          matchId: mid.slice(0, 16),
          questionerId: m.questionerId.slice(0, 12),
          larpId: m.larpId.slice(0, 12),
          questionPreview: m.question.slice(0, 60),
          drawMode: m.drawMode
        }))
      }, null, 2));
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ error: 'Unknown admin route. Try /admin/stats or /admin/queues' }));
  }

  // Security headers
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');

  const url = new URL(req.url, `http://${req.headers.host}`);
  let filePath = url.pathname === '/' ? '/index.html' : url.pathname;

  // Prevent directory traversal
  if (filePath.includes('..')) {
    res.writeHead(403);
    return res.end('403 Forbidden');
  }

  const fullPath = path.join(__dirname, 'public', filePath);
  const mimeMap = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.png': 'image/png',
    '.svg': 'image/svg+xml',
    '.json': 'application/json; charset=utf-8',
  };
  const ext = path.extname(fullPath).toLowerCase();
  const mime = mimeMap[ext];
  if (!mime) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    return res.end('404 Not Found');
  }

  fs.readFile(fullPath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end('<!DOCTYPE html><html><head><meta charset="utf-8"><title>404</title></head><body style="background:#0d0d0d;color:#666;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;"><div style="text-align:center"><h1 style="color:#a78bfa;font-size:3rem;margin:0">404</h1><p>页面不存在</p></div></body></html>');
    }
    res.writeHead(200, { 'Content-Type': mime });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  // ── IP rate limiting ──
  const ip = req.socket.remoteAddress || 'unknown';
  const ban = ipBlacklist.get(ip);
  if (ban && ban.until > Date.now()) {
    const secs = Math.ceil((ban.until - Date.now()) / 1000);
    send(ws, { type: 'error', msg: `⚠️ 连接过于频繁，请 ${secs} 秒后再试` });
    ws.close();
    return;
  }
  if (checkRate(ipConnections, ip, CONFIG.ipRateWindow, CONFIG.ipRateMax)) {
    banIP(ip, 300_000); // 5 min ban
    send(ws, { type: 'error', msg: '⚠️ 连接过于频繁，已被临时限制。请 5 分钟后再试。' });
    ws.close();
    return;
  }
  hitRate(ipConnections, ip);

  let sessionId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'join': {
        sessionId = msg.sessionId || `s${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        const s = getOrCreate(sessionId);
        s.lastActive = Date.now();
        wsBySession.set(sessionId, ws);
        s.connectedAt = Date.now();
        send(ws, { type: 'joined', sessionId, credits: s.credits, queueLength: waitingQueue.length });
        broadcastStats();
        dispatch();
        break;
      }

      case 'ask': {
        if (!sessionId) break;
        const s = getOrCreate(sessionId);
        s.lastActive = Date.now();
        if (checkViolations(sessionId, ws)) break;

        // Rate limit asks
        if (checkRate(askTimestamps, sessionId, CONFIG.askRateWindow, CONFIG.askRateMax)) {
          send(ws, { type: 'error', msg: '提问太频繁，请稍后再试' });
          break;
        }

        const mod = moderate(msg.question, sessionId);
        if (!mod.ok) {
          send(ws, { type: 'error', msg: mod.reason });
          checkViolations(sessionId, ws);
          break;
        }
        if (s.credits < CONFIG.questionCost) {
          send(ws, { type: 'error', msg: 'Credits 不足！去扮演 AI 赚点吧 😉' });
          break;
        }
        hitRate(askTimestamps, sessionId);
        s.credits -= CONFIG.questionCost;
        send(ws, { type: 'credits_updated', credits: s.credits });
        waitingQueue.push({ sessionId, question: msg.question, timestamp: Date.now(), drawMode: msg.drawMode || false });
        send(ws, { type: 'queued', credits: s.credits, position: waitingQueue.length, question: msg.question, drawMode: msg.drawMode || false });
        dispatch();
        broadcastStats();
        break;
      }

      case 'larp_ready': {
        const s = getOrCreate(sessionId);
        s.lastActive = Date.now();
        s.role = 'larp';
        s.ready = true;
        // Add to ready queue if not already in it
        if (!readyLarpQueue.includes(sessionId)) {
          readyLarpQueue.push(sessionId);
        }
        dispatch();
        broadcastStats();
        // If not immediately dispatched, show waiting status
        if (!activeMatches.size || ![...activeMatches.values()].some(m => m.larpId === sessionId)) {
          send(ws, { type: 'larp_waiting', msg: '已就绪，等待问题分配给你...' });
        }
        break;
      }

      case 'larp_unready': {
        const s = getOrCreate(sessionId);
        s.lastActive = Date.now();
        s.ready = false;
        // Remove from ready queue
        const ri = readyLarpQueue.indexOf(sessionId);
        if (ri >= 0) readyLarpQueue.splice(ri, 1);
        // Cancel any active match where this larp is assigned
        for (const [mid, match] of activeMatches) {
          if (match.larpId === sessionId) {
            clearTimeout(match.timer);
            activeMatches.delete(mid);
            waitingQueue.unshift({ sessionId: match.questionerId, question: match.question, timestamp: Date.now(), drawMode: match.drawMode || false });
            const qWs = wsBySession.get(match.questionerId);
            if (qWs) send(qWs, { type: 'reassigning', matchId: mid, question: match.question });
          }
        }
        send(ws, { type: 'larp_idle' });
        broadcastStats();
        dispatch();
        break;
      }

      case 'submit_answer': {
        if (!sessionId) break;
        const s = getOrCreate(sessionId);
        s.lastActive = Date.now();
        if (checkViolations(sessionId, ws)) break;

        if (checkRate(answerTimestamps, sessionId, CONFIG.answerRateWindow, CONFIG.answerRateMax)) {
          send(ws, { type: 'error', msg: '回答太频繁，请稍后再试' });
          break;
        }

        const ansMod = moderate(msg.answer || '', sessionId);
        if (!ansMod.ok) {
          send(ws, { type: 'error', msg: `回答${ansMod.reason}` });
          checkViolations(sessionId, ws);
          break;
        }

        // Find the active match where this session is the larp
        let matchId = null, match = null;
        for (const [mid, m] of activeMatches) {
          if (m.larpId === sessionId) { matchId = mid; match = m; break; }
        }
        if (!match) { send(ws, { type: 'error', msg: '没有活跃的任务' }); break; }

        clearTimeout(match.timer);
        activeMatches.delete(matchId);

        hitRate(answerTimestamps, sessionId);
        s.credits += CONFIG.answerReward;
        s.answersThisHour = (s.answersThisHour || 0) + 1;

        const qWs = wsBySession.get(match.questionerId);
        if (qWs) {
          send(qWs, { type: 'answer', matchId, question: match.question, answer: msg.answer, image: msg.image || null });
        }
        send(ws, { type: 'answer_submitted', credits: s.credits, matchId });

        // Return larp to ready queue
        if (s.ready && wsBySession.has(sessionId)) {
          readyLarpQueue.push(sessionId);
          send(ws, { type: 'larp_idle' });
        }
        broadcastStats();
        dispatch();
        break;
      }

      case 'rate': {
        if (!sessionId) break;
        const s = sessions.get(sessionId);
        if (s) s.lastActive = Date.now();
        const result = rateMsg(msg.matchId, sessionId, msg.kind);
        broadcast({ type: 'rating_update', matchId: msg.matchId, likes: result.likes, dislikes: result.dislikes });
        break;
      }

      case 'skip': {
        if (!sessionId) break;
        const s = sessions.get(sessionId);
        if (s) s.lastActive = Date.now();
        // Find the active match for this larp
        let skipMatchId = null, skipMatch = null;
        for (const [mid, m] of activeMatches) {
          if (m.larpId === sessionId) { skipMatchId = mid; skipMatch = m; break; }
        }
        if (skipMatch) {
          clearTimeout(skipMatch.timer);
          activeMatches.delete(skipMatchId);
          // Re-queue the question
          waitingQueue.unshift({ sessionId: skipMatch.questionerId, question: skipMatch.question, timestamp: Date.now(), drawMode: skipMatch.drawMode || false });
          const qWs = wsBySession.get(skipMatch.questionerId);
          if (qWs) send(qWs, { type: 'reassigning', matchId: skipMatchId, question: skipMatch.question });
          send(ws, { type: 'task_cancelled', matchId: skipMatchId, reason: '你跳过了' });
          // Return larp to ready queue
          if (s && s.ready && wsBySession.has(sessionId)) {
            readyLarpQueue.push(sessionId);
            send(ws, { type: 'larp_idle' });
          }
        }
        broadcastStats();
        dispatch();
        break;
      }

      case 'status': {
        if (!sessionId) break;
        const s = getOrCreate(sessionId);
        s.lastActive = Date.now();
        const pos = waitingQueue.findIndex(q => q.sessionId === sessionId);
        send(ws, { type: 'status', credits: s.credits, position: pos >= 0 ? pos + 1 : 0, queueLength: waitingQueue.length });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (sessionId) {
      const s = sessions.get(sessionId);
      if (s) s.lastActive = Date.now();
      cleanupSession(sessionId);
    }
    broadcastStats();
  });
});

// ─── Graceful shutdown ─────────────────────────────────────────────────
function shutdown() {
  console.log('[server] shutting down, saving credits...');
  saveCredits();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(CONFIG.port, () => {
  console.log(`Humans In The Loop running on port ${CONFIG.port}`);
  console.log(`  Rate limiter: ${CONFIG.ipRateMax} conn/min/IP, ${CONFIG.askRateMax} asks/min, ${CONFIG.answerRateMax} answers/min`);
  console.log(`  Session TTL: ${CONFIG.sessionTtlMs / 1000}s, GC every: ${CONFIG.sessionGcInterval / 1000}s`);
  console.log(`  Persist every: ${CONFIG.persistInterval / 1000}s → ${creditsFile}`);
});
