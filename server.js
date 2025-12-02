// server.js (MySQL-connected)
const express = require('express');
const session = require('express-session');
const path = require('path');
const bodyParser = require('body-parser');
const bcrypt = require('bcryptjs');
const mysql = require('mysql2/promise');
const dotenv = require('dotenv');
const { createProxyMiddleware } = require('http-proxy-middleware');

dotenv.config();

const app = express();
const PUBLIC_DIR = path.join(__dirname, 'public'); // 固定 public 目錄

// ---------- Middlewares ----------
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Session 一定要喺守門員之前
app.use(
  session({
    secret: process.env.SESSION_SECRET || 'change_this_super_secret_key',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 * 12 }, // 12 小時
  })
);

// 簡單 log
app.use((req, _res, next) => {
  console.log('REQ', req.method, req.url, 'user =', req.session?.user?.username);
  next();
});

// 🔐 全局守門員（白名單路徑唔檢查登入）
const PUBLIC_PATHS = new Set([
  '/login',
  '/api/health',
  '/styles.css',
  '/favicon.ico',
]);

app.use((req, res, next) => {
  // 1) 白名單路徑：放行
  if (PUBLIC_PATHS.has(req.path)) return next();

  // 2) API 給 Flask proxy
  if (req.path.startsWith('/flask')) return next();

  // 3) 如果已登入：放行
  if (req.session && req.session.user) return next();

  // 4) 未登入：全部踢去 /login
  return res.redirect('/login');
});

// ---------- 靜態檔案 ----------
// 一定要擺喺守門員之後：咁樣 /index.html /race.html 都會被檢查 session
app.use(express.static(PUBLIC_DIR));


// 健康檢查（Render 用）
app.get('/api/health', (_req, res) => res.json({ ok: true }));

// Flask proxy
if (process.env.FLASK_URL) {
  console.log('🔗 Proxy to Flask API:', process.env.FLASK_URL);
  app.use(
    '/flask',
    createProxyMiddleware({
      target: process.env.FLASK_URL,
      changeOrigin: true,
      pathRewrite: { '^/flask': '' },
    })
  );
}

function requireAuth(req, res, next) {
  if (req.session && req.session.user) return next();
  return res.redirect('/login');
}

// ---------- MySQL Pool ----------
let pool;
(async () => {
  try {
    console.log('DB config =>', {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  database: process.env.DB_NAME,
});

pool = await mysql.createPool({
  host: process.env.DB_HOST || '127.0.0.1',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASS || process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'hkjc_db',
  port: Number(process.env.DB_PORT || 3306),
  waitForConnections: true,
  connectionLimit: 10,
});

    console.log('✅ MySQL connected');
  } catch (e) {
    console.error('❌ MySQL connection failed:', e.message);
  }
})();

// ---------- Page routes ----------
app.get('/', (req, res) => {
  if (req.session?.user) return res.redirect('/app');
  return res.redirect('/login');
});

app.get('/login', (_req, res) => {
  return res.sendFile(path.join(PUBLIC_DIR, 'login.html'));
});

// ✅ Login（唯一一個 /login POST）
app.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body || {};
    console.log('[LOGIN] body =', req.body);

    // 1) 基本檢查
    if (!username || !password) {
      return res.status(400).send('Missing username or password');
    }

    // 2) DB pool 準備好未
    if (!pool) {
      console.error('[LOGIN] pool not ready');
      return res.status(503).send('DB not ready');
    }

    // 3) 喺 DB 搵 user
    const [rows] = await pool.query(
      'SELECT id, username, password_hash, role, is_active FROM users WHERE username = ? LIMIT 1',
      [username]
    );
    console.log('[LOGIN] rows =', rows);

    if (!rows.length) {
      // 用戶名錯
      return res.status(401).send('Invalid credentials');
    }

    const user = rows[0];

    // 4) 帳戶停用
    if (!user.is_active) {
      return res.status(403).send('Account disabled');
    }

    // 5) 用 bcrypt 對比密碼
    const ok = await bcrypt.compare(password, user.password_hash);
    console.log('[LOGIN] password ok?', ok);

    if (!ok) {
      // 密碼錯
      return res.status(401).send('Invalid credentials');
    }

    // 6) 寫入 session
    req.session.user = {
      id: user.id,
      username: user.username,
      role: user.role,
    };

    // 7) 更新最後登入時間（就算失敗都唔影響 login）
    pool.query('UPDATE users SET last_login_at = NOW() WHERE id = ?', [user.id])
      .catch(err => console.error('[LOGIN] update last_login_at error', err));

    // 8) Login OK → 去 /app
    return res.redirect('/app');
  } catch (err) {
    console.error('[LOGIN] error', err);
    return res.status(500).send('Server error (login)');
  }
});


app.post('/logout', (req, res) => req.session.destroy(() => res.redirect('/login')));

app.get('/app', requireAuth, (_req, res) => {
  return res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});



// ---------- API routes (protected) ----------
app.get('/api/jockeys', requireAuth, async (_req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const [rows] = await pool.query(
      'SELECT name_zh AS jockey, country, starts, wins, place_pct FROM jockeys ORDER BY wins DESC LIMIT 500'
    );
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/trainers', requireAuth, async (_req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const [rows] = await pool.query(
      'SELECT name_zh AS trainer, country, IFNULL(stable,"-") AS stable FROM trainers LIMIT 500'
    );
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 取得有排位資料的 race_date 列表（由新到舊）
app.get('/api/racecard/dates', requireAuth, async (_req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const [rows] = await pool.query(
      'SELECT DISTINCT DATE(race_date) AS race_date FROM racecard_races ORDER BY DATE(race_date) DESC LIMIT 365'
    );
    // 只回傳日期字串，例如 ["2025-11-18","2025-11-15", ...]
    res.json(rows.map(r => r.race_date));
  } catch (e) {
    console.error('API /racecard/dates error:', e);
    res.status(500).json({ error: e.message });
  }
});



// 取得某日所有場次（racecard_races）
app.get('/api/racecard/races', requireAuth, async (req, res) => {
  if (!pool) return res.status(503).json({ error: "DB not ready" });

  try {
    const date = req.query.date;
    const venue = req.query.venue; // 'ST' or 'HV'

    if (!date || !venue) {
      return res.status(400).json({ error: "Missing date or venue" });
    }

    const [rows] = await pool.query(
      `SELECT
         race_no, race_time, race_name_zh, distance_m, course, going, class_text
       FROM racecard_races
       WHERE race_date = ? AND venue_code = ?
       ORDER BY race_no`,
      [date, venue]
    );

    res.json(rows);
  } catch (err) {
    console.error("API /racecard/races error:", err);
    res.status(500).json({ error: err.message });
  }
});

// 取得某一場某類型賠率的歷史 snapshot（由新至舊）
app.get('/api/odds/history', requireAuth, async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const date   = req.query.date;
    const venue  = req.query.venue;
    const raceNo = req.query.race_no;
    const type   = (req.query.type || 'WIN').toUpperCase(); // WIN / PLA

    if (!date || !venue || !raceNo) {
      return res.status(400).json({ error: 'Missing date / venue / race_no' });
    }

    const oddsType = type === 'PLA' ? 'PLA' : 'WIN';

    const [rows] = await pool.query(
      `SELECT horse_no, odds, snapshot_ts
       FROM race_odds_snapshots
       WHERE race_date = ? AND venue_code = ? AND race_no = ? AND odds_type = ?
       ORDER BY snapshot_ts DESC, horse_no ASC
       LIMIT 1000`,
      [date, venue, raceNo, oddsType]
    );

    res.json(rows);
  } catch (e) {
    console.error('/api/odds/history error:', e);
    res.status(500).json({ error: e.message });
  }
});


// 取得某一場所有馬（racecard_entries）— 帶出完整欄位
app.get('/api/racecard/entries', requireAuth, async (req, res) => {
  try {
    const date = req.query.date;
    const raceNo = req.query.race_no;

    if (!date || !raceNo) {
      return res.status(400).json({ error: 'Missing race_date or race_no' });
    }

    const [rows] = await pool.query(
      `SELECT
         horse_no,
         horse_name_zh,
         horse_name_en,
         horse_code,
         draw,
         jockey_zh,
         trainer_zh,
         rating,
         rating_pm,
         weight_lb,
         declared_wt,
         declared_wt_pm,
         age,
         sex,
         wfa,
         season_stakes,
         priority,
         days_since,
         owner,
         sire,
         dam,
         import_cat,
         silks,
         brand,
         gear,
         last6,
         scratched,
         win_odds,
         pla_odds,
         last_odds_update
       FROM racecard_entries
       WHERE race_date = ? AND race_no = ?
       ORDER BY horse_no`,
      [date, raceNo]
    );

    res.json(rows);
  } catch (e) {
    console.error('API /racecard/entries error:', e);
    res.status(500).json({ error: e.message });
  }
});

// 🔹 賠率 API：回傳最新賠率 + 最近 10 筆 snapshot
// GET /api/odds?date=2025-11-26&venue=HV&raceNo=1
app.get('/api/odds', requireAuth, async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const { date, venue, raceNo } = req.query;

    if (!date || !venue || !raceNo) {
      return res.status(400).json({ error: 'missing date / venue / raceNo' });
    }

    const conn = await pool.getConnection();

    try {
      // 1) 最新賠率：來自 racecard_entries
      const [latestRows] = await conn.execute(
        `
        SELECT
          horse_no,
          horse_name_zh,
          win_odds,
          pla_odds,
          last_odds_update
        FROM racecard_entries
        WHERE race_date = ?
          AND race_no   = ?
        ORDER BY horse_no
        `,
        [date, raceNo]
      );

      // 2) 最近 10 筆 snapshot：來自 race_odds_snapshots
      const [snapRows] = await conn.execute(
        `
        SELECT
          horse_no,
          odds_type,
          odds,
          snapshot_ts
        FROM race_odds_snapshots
        WHERE race_date = ?
          AND venue_code = ?
          AND race_no    = ?
        ORDER BY snapshot_ts DESC
        LIMIT 10
        `,
        [date, venue, raceNo]
      );

      conn.release();

      return res.json({
        latest: latestRows,
        snapshots: snapRows,
      });
    } catch (e) {
      conn.release();
      throw e;
    }
  } catch (e) {
    console.error('API /api/odds error:', e);
    return res.status(500).json({ error: e.message });
  }
});

// 🆕 馬匹＋檔位統計
app.get('/api/race/horse_stats', async (req, res) => {
  const { date, venue, race_no } = req.query;
  if (!date || !venue || !race_no) {
    return res.status(400).json({ error: 'missing date / venue / race_no' });
  }

  try {
    const [rows] = await pool.query(
      `
      SELECT
        e.horse_no,
        e.horse_name_zh,
        e.draw AS gate_no,

        -- 馬匹統計（來自 race_analysis_scores.horse_*）
        ras.horse_runs,
        ras.win,
        ras.second_place,
        ras.third_place,
        ras.forth_place,
        ras.horse_win_rate,
        ras.horse_q_rate,
        ras.horse_place_rate,
        ras.horse_top4_rate,
        ras.horse_score_raw,
        ras.horse_score_norm,
        ras.horse_score_final,

        -- 檔位統計（draw_*）
        ras.draw_runs,
        ras.draw_win,
        ras.draw_second_place,
        ras.draw_third_place,
        ras.draw_forth_place,
        ras.draw_win_rate,
        ras.draw_q_rate,
        ras.draw_place_rate,
        ras.draw_top4_rate,
        ras.draw_score_raw,
        ras.draw_score_norm,
        ras.draw_score_final
      FROM race_analysis_scores ras
      JOIN racecard_entries e
        ON ras.race_date = e.race_date
       AND ras.race_no   = e.race_no
       AND ras.horse_id  = e.horse_id
      WHERE ras.race_date   = ?
        AND ras.venue_code  = ?
        AND ras.race_no     = ?
      ORDER BY e.horse_no
      `,
      [date, venue, Number(race_no)]
    );

    res.json(rows);
  } catch (err) {
    console.error('[api/race/horse_stats] error', err);
    res.status(500).json({ error: 'internal error' });
  }
});


// 取得馬匹資料（最簡版）
app.get('/api/horses', requireAuth, async (_req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });
  try {
    const [rows] = await pool.query('SELECT * FROM horse_profiles LIMIT 200');
    console.log('Horses rows:', rows.length);
    res.json(rows);
  } catch (e) {
    console.error('🐎 Horses API Error:', e);
    res.status(500).json({ error: e.message });
  }
});

// 只用喺「馬匹 → 歷史」度用：按賽日取出當日出賽馬匹
app.get('/api/horses/by-racedate', requireAuth, async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const date  = req.query.date;        // YYYY-MM-DD
    const venue = req.query.venue || ''; // ST / HV，可選

    if (!date) {
      return res.status(400).json({ error: 'Missing date' });
    }

    let sql = `
      SELECT DISTINCT
        h.horse_id,
        h.name_chi,
        h.name_eng,
        h.sex,
        h.age,
        h.colour,
        h.country,
        h.trainer_id,
        h.owner,
        h.current_rating,
        h.season_rating,
        h.last10_racedays,
        h.updated_at
      FROM horse_profiles h
      JOIN racecard_entries e
        ON e.horse_name_zh = h.name_chi   -- 之後有 horse_id 再改 join condition
      WHERE e.race_date = ?
    `;
    const params = [date];

    if (venue) {
      sql += ' AND e.venue_code = ?';
      params.push(venue);
    }

    sql += ' ORDER BY h.current_rating DESC, h.horse_id';

    const [rows] = await pool.query(sql, params);
    res.json(rows);
  } catch (e) {
    console.error('🐎 /api/horses/by-racedate error:', e);
    res.status(500).json({ error: e.message });
  }
});


// 🔍 搜尋馬匹（支援中英文）
app.get('/api/horses/search', requireAuth, async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });
  try {
    const keyword = req.query.q?.trim();
    if (!keyword) return res.json([]);

    const [rows] = await pool.query(
      `SELECT horse_id, name_chi, name_eng, sex, age, colour, country, trainer_id, owner,
              current_rating, season_rating, season_prize, total_prize, last10_racedays, updated_at
       FROM horses
       WHERE name_chi LIKE ? OR name_eng LIKE ?
       ORDER BY updated_at DESC LIMIT 200`,
      [`%${keyword}%`, `%${keyword}%`]
    );

    console.log(`🔍 Search keyword: ${keyword}, found ${rows.length} horses`);
    res.json(rows);
  } catch (e) {
    console.error('🐎 Horses Search API Error:', e);
    res.status(500).json({ error: e.message });
  }
});

// 取得馬匹清單（支援關鍵字/分頁）
app.get('/api/horses/list', requireAuth, async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const q = (req.query.q || '').trim();
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);

    let sql = `
      SELECT horse_id, name_chi, name_eng, sex, age, colour, country,
             trainer_id, owner, current_rating, season_rating, updated_at
      FROM horses
    `;
    const params = [];
    if (q) {
      sql += ` WHERE name_chi LIKE ? OR name_eng LIKE ? OR horse_id LIKE ? `;
      params.push(`%${q}%`, `%${q}%`, `%${q}%`);
    }
    sql += ` ORDER BY updated_at DESC LIMIT ? OFFSET ? `;
    params.push(limit, offset);

    const [rows] = await pool.query(sql, params);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 批量更新（transaction）
app.post('/api/horses/bulk-update', requireAuth, async (req, res) => {
  try {
    if (req.session?.user?.username !== 'admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.json({ updated: 0 });

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      let updated = 0;
      for (const it of items) {
        const fields = [];
        const values = [];

        if (typeof it.owner === 'string') { fields.push('owner=?'); values.push(it.owner); }
        if (typeof it.trainer_id === 'string') { fields.push('trainer_id=?'); values.push(it.trainer_id); }
        if (it.current_rating !== undefined && it.current_rating !== null) {
          fields.push('current_rating=?'); values.push(parseInt(it.current_rating, 10) || 0);
        }
        if (!fields.length || !it.horse_id) continue;

        const sql = `UPDATE horse_profiles SET ${fields.join(', ')}, updated_at=NOW() WHERE horse_id=?`;
        values.push(it.horse_id);
        const [ret] = await conn.query(sql, values);
        updated += ret.affectedRows;
      }

      await conn.commit();
      conn.release();
      res.json({ updated });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (e) {
    console.error('🐎 bulk-update error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/venues', requireAuth, async (_req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const [rows] = await pool.query('SELECT code, name_zh FROM venues ORDER BY code');
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Races 1–12 (today) runners
app.get('/api/races/:no/runners', requireAuth, async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const raceNo = Number(req.params.no);
    if (raceNo < 1 || raceNo > 12) return res.status(400).json({ error: 'race no 1..12' });
    const [races] = await pool.query(
      'SELECT id, race_day, venue_code, distance_m, going FROM races WHERE race_day=CURDATE() AND race_no=? LIMIT 1',
      [raceNo]
    );
    if (!races.length) return res.json({ meta: null, items: [] });
    const race = races[0];
    const [rows] = await pool.query(
      'SELECT saddle_no, horse_name_zh, jockey_zh, weight_lbs, draw, sp FROM race_runners WHERE race_id=? ORDER BY saddle_no',
      [race.id]
    );
    res.json({ meta: race, items: rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------- Debug ----------
app.get('/debug/db', async (_req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not ready' });

  try {
    const [[db]] = await pool.query('SELECT DATABASE() AS db');
    const [[cnt]] = await pool.query('SELECT COUNT(*) AS total FROM horse_profiles');
    res.json({ db: db.db, horses_count: cnt.total });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---- 兜底：除 /api/*、/flask/* 之外嘅路徑，全部送去 login（或前端 index）----
app.use((req, res, next) => {
  if (req.path.startsWith('/api') || req.path.startsWith('/flask')) return next();
  return res.sendFile(path.join(PUBLIC_DIR, 'login.html'));
});

// ---------- Listen（0.0.0.0 + Render PORT） ----------
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ Racing portal running on http://0.0.0.0:${PORT}`);
});
