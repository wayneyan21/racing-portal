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
const __dirnameResolved = __dirname;

// ---------- Middlewares ----------
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
// ---------- Proxy Flask API ----------
app.use('/api', createProxyMiddleware({
  target: 'http://127.0.0.1:5000',   // Flask 執行嘅 port（你改過5001就填5001）
  changeOrigin: true,
}));


app.use(
  session({
    secret: process.env.SESSION_SECRET || 'change_this_super_secret_key',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 * 12 },
  })
);

// ---------- Demo user ----------
// username: admin, password: Wayne123!
const USER = { username: 'admin', passwordHash: bcrypt.hashSync('Wayne123!', 10) };

function requireAuth(req, res, next) {
  if (req.session && req.session.user) return next();
  return res.redirect('/login');
}

// ---------- MySQL Pool ----------
let pool;
(async () => {
  try {
    pool = await mysql.createPool({
      host: process.env.DB_HOST || '127.0.0.1',
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASS || '',
      database: process.env.DB_NAME || 'racing_db',
      waitForConnections: true,
      connectionLimit: 10,
    });
    console.log('✅ MySQL connected');
  } catch (e) {
    console.error('❌ MySQL connection failed:', e.message);
  }
})();

// ---------- Page routes ----------
app.get('/', (req, res) => (req.session?.user ? res.redirect('/app') : res.redirect('/login')));
app.get('/login', (req, res) => res.sendFile(path.join(__dirnameResolved, 'public', 'login.html')));
app.post('/login', (req, res) => {
  const { username, password } = req.body || {};
  if (username !== USER.username) return res.status(401).send('Invalid credentials');
  const ok = bcrypt.compareSync(password, USER.passwordHash);
  if (!ok) return res.status(401).send('Invalid credentials');
  req.session.user = { username };
  res.redirect('/app');
});
app.post('/logout', (req, res) => req.session.destroy(() => res.redirect('/login')));
app.get('/app', requireAuth, (req, res) => res.sendFile(path.join(__dirnameResolved, 'public', 'index.html')));

// ---------- API routes (protected) ----------
app.get('/api/jockeys', requireAuth, async (req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT name_zh AS jockey, country, starts, wins, place_pct FROM jockeys ORDER BY wins DESC LIMIT 500'
    );
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/trainers', requireAuth, async (req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT name_zh AS trainer, country, IFNULL(stable,"-") AS stable FROM trainers LIMIT 500'
    );
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------- 取得馬匹資料 ----------
// 取得馬匹資料（最簡版）
app.get('/api/horses', requireAuth, async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM horses LIMIT 200');
    console.log('Horses rows:', rows.length);
    res.json(rows);
  } catch (e) {
    console.error("🐎 Horses API Error:", e);
    res.status(500).json({ error: e.message });
  }
});

// 🔍 搜尋馬匹（支援中英文）
app.get('/api/horses/search', requireAuth, async (req, res) => {
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
    console.error("🐎 Horses Search API Error:", e);
    res.status(500).json({ error: e.message });
  }
});

// 取得馬匹清單（支援關鍵字/分頁），供「多項修改」頁使用
app.get('/api/horses/list', requireAuth, async (req, res) => {
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
    // 可簡單限制只有 admin 可改
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
        // 允許更新的欄位（白名單）
        const fields = [];
        const values = [];

        if (typeof it.owner === 'string') { fields.push('owner=?'); values.push(it.owner); }
        if (typeof it.trainer_id === 'string') { fields.push('trainer_id=?'); values.push(it.trainer_id); }
        if (it.current_rating !== undefined && it.current_rating !== null) {
          fields.push('current_rating=?'); values.push(parseInt(it.current_rating, 10) || 0);
        }
        if (!fields.length || !it.horse_id) continue;

        const sql = `UPDATE horses SET ${fields.join(', ')}, updated_at=NOW() WHERE horse_id=?`;
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



app.get('/api/venues', requireAuth, async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT code, name_zh FROM venues ORDER BY code');
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Races 1–12 (today) runners
app.get('/api/races/:no/runners', requireAuth, async (req, res) => {
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

// ---------- 健檢：確認 DB 及 horses 行數 ----------
app.get('/debug/db', async (req, res) => {
  try {
    const [[db]]  = await pool.query('SELECT DATABASE() AS db');
    const [[cnt]] = await pool.query('SELECT COUNT(*) AS total FROM horses');
    res.json({ db: db.db, horses_count: cnt.total });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


// ---------- Static ----------
app.use(express.static(path.join(__dirnameResolved, 'public')));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`✅ Racing portal running on http://localhost:${port}`));
