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

// 🆕 賽事分析：馬匹統計（來自 race_analysis_scores + racecard_entries）
// 🆕 馬匹＋檔位＋負磅統計
app.get('/api/race/horse_stats', async (req, res) => {
  if (!pool) {
    console.error('[GET /api/race/horse_stats] pool not ready');
    return res.status(503).json({ error: 'DB not ready' });
  }

  try {
    const { date, venue, race_no } = req.query;
    if (!date || !venue || !race_no) {
      return res.status(400).json({ error: 'missing date / venue / race_no' });
    }

    const sql = `
      SELECT
        e.horse_no,
        e.horse_name_zh,
        e.draw                                   AS gate_no,

        -- 🐎 馬匹統計（race_analysis_scores）
        ra.horse_runs                              AS starts,
        ra.win                                     AS win,
        ra.second_place                            AS second,
        ra.third_place                             AS third,
        ra.forth_place                             AS fourth,
        ra.horse_win_rate                          AS win_pct,      -- 0–1 or 0–100 都得，前端會再處理
        ra.horse_q_rate                            AS q_pct,
        ra.horse_place_rate                        AS place_pct,
        ra.horse_top4_rate                         AS top4_pct,
        ra.horse_score_raw                         AS score,
        ra.horse_score_norm                        AS total_pct,
        ra.horse_score_final                       AS green10,

        -- 🧱 檔位統計
        ra.draw_runs,
        ra.draw_win,
        ra.draw_second_place,
        ra.draw_third_place,
        ra.draw_forth_place,
        ra.draw_win_rate                           AS draw_win_pct,
        ra.draw_q_rate                             AS draw_q_pct,
        ra.draw_place_rate                         AS draw_place_pct,
        ra.draw_top4_rate                          AS draw_top4_pct,
        ra.draw_score_raw                          AS draw_score,
        ra.draw_score_norm                         AS draw_total_pct,
        ra.draw_score_final                        AS draw_green10,

        -- ⚖️ 負磅統計（race_combo_scores：metric_code = 'WEIGHT_xxx'）
        w.metric_code                              AS weight_band,
        w.runs                                     AS wt_runs,
        w.win_cnt                                  AS wt_win,
        w.second_cnt                               AS wt_second,
        w.third_cnt                                AS wt_third,
        w.fourth_cnt                               AS wt_fourth,
        w.win_pct                                  AS wt_win_pct,
        w.q_pct                                    AS wt_q_pct,
        w.place_pct                                AS wt_place_pct,
        w.top4_pct                                 AS wt_top4_pct,
        w.score_raw                                AS wt_score,
        w.score_norm                               AS wt_total_pct,
        w.score_final                              AS wt_green10

      FROM race_analysis_scores ra
      JOIN racecard_entries e
        ON ra.race_date = e.race_date
       AND ra.race_no   = e.race_no
       AND ra.horse_id  COLLATE utf8mb4_unicode_ci
           = e.horse_id COLLATE utf8mb4_unicode_ci

      -- 👇 重要：用 metric_code LIKE 'WEIGHT%' 連接負磅統計
      LEFT JOIN race_combo_scores w
        ON w.race_date   = ra.race_date
       AND w.venue_code  = ra.venue_code
       AND w.race_no     = ra.race_no
       AND w.horse_id    = ra.horse_id
       AND w.metric_code LIKE 'WEIGHT%'

      WHERE ra.race_date   = ?
        AND ra.venue_code  = ?
        AND ra.race_no     = ?
      ORDER BY e.horse_no ASC
    `;

    const [rows] = await pool.query(sql, [date, venue, Number(race_no)]);
    return res.json(rows);
  } catch (err) {
    console.error('[GET /api/race/horse_stats] SQL error:', err);
    return res.status(500).json({ error: String(err.message || err) });
  }
});

// 🆕 負磅統計：由 race_combo_scores (metric_code = 'WEIGHT_*') 取得
// GET /api/race/weight_stats?date=YYYY-MM-DD&venue=ST&race_no=1
app.get('/api/race/weight_stats', requireAuth, async (req, res) => {
  if (!pool) {
    console.error('[GET /api/race/weight_stats] pool not ready');
    return res.status(503).json({ error: 'DB not ready' });
  }

  try {
    const { date, venue, race_no } = req.query;
    if (!date || !venue || !race_no) {
      return res.status(400).json({ error: 'missing date / venue / race_no' });
    }

    const sql = `
      SELECT
        e.horse_no,
        e.horse_name_zh,
        rc.metric_code                 AS weight_zone,
        rc.runs,
        rc.win_cnt,
        rc.second_cnt,
        rc.third_cnt,
        rc.fourth_cnt,
        rc.win_pct,
        rc.q_pct,
        rc.place_pct,
        rc.top4_pct,
        rc.score_raw,
        rc.score_norm,
        rc.score_final
      FROM race_combo_scores rc
      JOIN racecard_entries e
        ON rc.race_date = e.race_date
       AND rc.race_no   = e.race_no
       AND rc.horse_id  COLLATE utf8mb4_unicode_ci
           = e.horse_id COLLATE utf8mb4_unicode_ci
      WHERE rc.race_date  = ?
        AND rc.venue_code = ?
        AND rc.race_no    = ?
        AND rc.metric_code LIKE 'WEIGHT_%'
      ORDER BY e.horse_no ASC, rc.metric_code ASC
    `;

    const [rows] = await pool.query(sql, [date, venue, Number(race_no)]);
    return res.json(rows);
  } catch (err) {
    console.error('[GET /api/race/weight_stats] SQL error:', err);
    return res.status(500).json({ error: String(err.message || err) });
  }
});

// 🆕 檔位統計：由 race_combo_scores (metric_code = 'HORSE_DRAW_*') 取得
// GET /api/race/draw_stats?date=YYYY-MM-DD&venue=ST&race_no=1
app.get('/api/race/draw_stats', requireAuth, async (req, res) => {
  if (!pool) {
    console.error('[GET /api/race/draw_stats] pool not ready');
    return res.status(503).json({ error: 'DB not ready' });
  }

  try {
    const { date, venue, race_no } = req.query;
    if (!date || !venue || !race_no) {
      return res.status(400).json({ error: 'missing date / venue / race_no' });
    }

    const sql = `
      SELECT
        e.horse_no,
        e.horse_name_zh,
        e.draw,
        rc.metric_code                 AS draw_band,        -- HORSE_DRAW_1_4 / 5_8 / 9_14
        rc.runs,
        rc.win_cnt,
        rc.second_cnt,
        rc.third_cnt,
        rc.fourth_cnt,
        rc.win_pct,
        rc.q_pct,
        rc.place_pct,
        rc.top4_pct,
        rc.score_raw,
        rc.score_norm,
        rc.score_final
      FROM race_combo_scores rc
      JOIN racecard_entries e
        ON rc.race_date = e.race_date
       AND rc.race_no   = e.race_no
       AND rc.horse_id  COLLATE utf8mb4_unicode_ci
           = e.horse_id COLLATE utf8mb4_unicode_ci
      WHERE rc.race_date  = ?
        AND rc.venue_code = ?
        AND rc.race_no    = ?
        AND rc.metric_code LIKE 'HORSE_DRAW_%'
        AND (e.scratched IS NULL OR e.scratched = 0)
      ORDER BY e.horse_no ASC
    `;

    const [rows] = await pool.query(sql, [date, venue, Number(race_no)]);
    return res.json(rows);
  } catch (err) {
    console.error('[GET /api/race/draw_stats] SQL error:', err);
    return res.status(500).json({ error: String(err.message || err) });
  }
});

// 🆕 騎師路程統計：同一場比賽入面，每個騎師在「同場地＋同途程」嘅歷史表現
// GET /api/race/jockey_dist_stats?date=YYYY-MM-DD&venue=ST&race_no=1
app.get('/api/race/jockey_dist_stats', requireAuth, async (req, res) => {
  const { date, venue, race_no } = req.query;

  if (!date || !venue || !race_no) {
    return res.status(400).json({ error: 'missing date / venue / race_no' });
  }

  let conn;
  try {
    conn = await pool.getConnection();

    // 1) 先搵今場距離
    const [rRows] = await conn.query(
      `SELECT distance_m FROM racecard_races
       WHERE race_date=? AND venue_code=? AND race_no=? LIMIT 1`,
      [date, venue, Number(race_no)]
    );

    if (!rRows.length) {
      return res.json([]);
    }

    const distance_m = rRows[0].distance_m;
    const metricCode = `JOCKEY_DIST_${venue}_${distance_m}`;

    // 2) 查騎師路程統計
    const [rows] = await conn.query(
      `
      SELECT
        e.jockey_zh,
        rc.venue_code AS venue,
        ? AS distance_m,

        MAX(rc.runs)        AS runs,
        MAX(rc.win_cnt)     AS win_cnt,
        MAX(rc.second_cnt)  AS second_cnt,
        MAX(rc.third_cnt)   AS third_cnt,
        MAX(rc.fourth_cnt)  AS fourth_cnt,
        MAX(rc.win_pct)     AS win_pct,
        MAX(rc.q_pct)       AS q_pct,
        MAX(rc.place_pct)   AS place_pct,
        MAX(rc.top4_pct)    AS top4_pct,
        MAX(rc.score_raw)   AS score_raw,
        MAX(rc.score_norm)  AS score_norm,
        MAX(rc.score_final) AS score_final

      FROM race_combo_scores rc
      JOIN racecard_entries e
        ON rc.race_date = e.race_date
       AND rc.race_no   = e.race_no
       AND TRIM(rc.horse_id) COLLATE utf8mb4_unicode_ci =
           TRIM(e.horse_id) COLLATE utf8mb4_unicode_ci

      WHERE rc.race_date   = ?
        AND rc.venue_code  = ?
        AND rc.race_no     = ?
        AND rc.metric_code = ?
        AND (e.scratched IS NULL OR e.scratched = 0)

      GROUP BY e.jockey_zh, rc.venue_code
      ORDER BY e.jockey_zh
      `,
      [
        distance_m,         // 第一個 ?（顯示用）
        date,
        venue,
        Number(race_no),
        metricCode          // JOCKEY_DIST_ST_1650
      ]
    );

    console.log('[jockey_dist_stats] return rows:', rows.length);
    res.json(rows);

  } catch (err) {
    console.error('[api] jockey_dist_stats ERROR:', err);
    res.status(500).json({ error: 'internal error' });
  } finally {
    if (conn) conn.release();
  }
});


// 🆕 馬匹距離統計：只取「同場地 + 同距離」那條 HORSE_DIST_xxx
// GET /api/race/distance_stats?date=YYYY-MM-DD&venue=ST&race_no=1
app.get('/api/race/distance_stats', requireAuth, async (req, res) => {
  if (!pool) {
    console.error('[GET /api/race/distance_stats] pool not ready');
    return res.status(503).json({ error: 'DB not ready' });
  }

  try {
    const { date, venue, race_no } = req.query;
    if (!date || !venue || !race_no) {
      return res.status(400).json({ error: 'missing date / venue / race_no' });
    }

    // 1) 先搵今場距離（例如 1200）
    const [raceRows] = await pool.query(
      `
      SELECT distance_m
      FROM racecard_races
      WHERE race_date  = ?
        AND venue_code = ?
        AND race_no    = ?
      LIMIT 1
      `,
      [date, venue, Number(race_no)]
    );

    if (!raceRows.length) {
      return res.json({ distance_m: null, items: [] });
    }

    const distance_m = raceRows[0].distance_m;
    const metricCode = `HORSE_DIST_${venue}_${distance_m}`;

    // 2) 喺 race_combo_scores 撈「今場、同場地、同距離」嗰條 HORSE_DIST_xxx
    const sql = `
      SELECT
        e.horse_no,
        e.horse_name_zh,
        rc.metric_code                 AS dist_metric,   -- 例如 HORSE_DIST_ST_1200
        rc.runs,
        rc.win_cnt,
        rc.second_cnt,
        rc.third_cnt,
        rc.fourth_cnt,
        rc.win_pct,
        rc.q_pct,
        rc.place_pct,
        rc.top4_pct,
        rc.score_raw,
        rc.score_norm,
        rc.score_final
      FROM race_combo_scores rc
      JOIN racecard_entries e
        ON rc.race_date = e.race_date
       AND rc.race_no   = e.race_no
       AND rc.horse_id  COLLATE utf8mb4_unicode_ci
           = e.horse_id COLLATE utf8mb4_unicode_ci
      WHERE rc.race_date   = ?
        AND rc.venue_code  = ?
        AND rc.race_no     = ?
        AND rc.metric_code = ?
        AND (e.scratched IS NULL OR e.scratched = 0)
      ORDER BY e.horse_no ASC
    `;

    const [rows] = await pool.query(sql, [
      date,
      venue,
      Number(race_no),
      metricCode,
    ]);

    return res.json({
      race_date: date,
      venue_code: venue,
      race_no: Number(race_no),
      distance_m,
      metric_code: metricCode,
      items: rows,   // 👈 前端直接 loop 呢個 items 用
    });
  } catch (err) {
    console.error('[GET /api/race/distance_stats] SQL error:', err);
    return res.status(500).json({ error: String(err.message || err) });
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
