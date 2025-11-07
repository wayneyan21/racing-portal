// 放在 app.js / main.js 最頂
const API_BASE = 'http://127.0.0.1:5000';


document.addEventListener('DOMContentLoaded', () => {
  const links = document.querySelectorAll('.menu a[data-panel]');
  const panels = document.querySelectorAll('.panel');

  function activate(id) {
    panels.forEach(p => p.classList.toggle('active', p.id === id));
    links.forEach(a => a.classList.toggle('active', a.getAttribute('data-panel') === id));
  }

  async function renderRace(no) {
    const panel = document.getElementById(`race-${no}`);
    if (!panel) return;
    panel.innerHTML = `<h2>第${no}場</h2><div class="card"><div class="table-wrap"><table>
      <thead><tr><th>馬號</th><th>馬名</th><th>騎師</th><th>負磅</th><th>檔位</th><th>獨贏</th></tr></thead>
      <tbody id="race-${no}-tbody"><tr><td colspan="6">讀取中…</td></tr></tbody></table></div></div>`;
    try {
      const res = await fetch(`${API_BASE}/api/races/${no}/runners`);
      const data = await res.json();
      const tb = document.getElementById(`race-${no}-tbody`);
      if (!data.items || !data.items.length) {
        tb.innerHTML = `<tr><td colspan="6">暫無資料</td></tr>`; return;
      }
      tb.innerHTML = data.items.map(r => `
        <tr>
          <td>${r.saddle_no}</td>
          <td>${r.horse_name_zh}</td>
          <td>${r.jockey_zh ?? '-'}</td>
          <td>${r.weight_lbs ?? '-'}</td>
          <td>${r.draw ?? '-'}</td>
          <td>${r.sp ?? '-'}</td>
        </tr>`).join('');
    } catch (e) {
      document.getElementById(`race-${no}-tbody`).innerHTML = `<tr><td colspan="6">讀取失敗：${e}</td></tr>`;
    }
  }

  async function renderDB(panelId, url, columns) {
  const panel = document.getElementById(panelId);
  if (!panel) return;

  // 建表格骨架
  panel.innerHTML = `
    <h2>${panel.querySelector('h2')?.textContent || '資料'}</h2>
    <div class="card">
      <div class="table-wrap">
        <table>
          <thead><tr>${columns.map(c => `<th>${c.title}</th>`).join('')}</tr></thead>
          <tbody id="${panelId}-tbody">
            <tr><td colspan="${columns.length}">讀取中…</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  `;

  try {
    // 透過 API_BASE 串後端，並加個簡單的 limit
    const apiUrl = `${API_BASE}${url}${url.includes('?') ? '&' : '?'}limit=200`;
    const res  = await fetch(apiUrl);
    const json = await res.json();

    // 兼容 array / {items:[]} / {data:[]}
    const rows = Array.isArray(json) ? json : (json.items || json.data || []);

    const tb = document.getElementById(`${panelId}-tbody`);
    if (!rows.length) {
      tb.innerHTML = `<tr><td colspan="${columns.length}">暫無資料</td></tr>`;
      return;
    }
    tb.innerHTML = rows.map(r => `
      <tr>
        ${columns.map(c => `<td>${(r[c.key] ?? '') || '-'}</td>`).join('')}
      </tr>
    `).join('');
  } catch (e) {
    document.getElementById(`${panelId}-tbody`).innerHTML =
      `<tr><td colspan="${columns.length}">讀取失敗：${e}</td></tr>`;
    console.error(e);
  }
}



  links.forEach(a => {
    a.addEventListener('click', (e) => {
      e.preventDefault();
      const id = a.getAttribute('data-panel');
      activate(id);
      history.replaceState(null, '', `#${id}`);
      if (id.startsWith('race-')) {
        const no = id.split('-')[1]; renderRace(no);
      } else if (id === 'db-jockeys') {
        renderDB('db-jockeys', '/api/jockeys', [
          {key:'jockey', title:'騎師'}, {key:'country', title:'國籍'},
          {key:'starts', title:'出賽'}, {key:'wins', title:'頭馬'}, {key:'place_pct', title:'入Q%'}
        ]);
      } else if (id === 'db-trainers') {
        renderDB('db-trainers', '/api/trainers', [
          {key:'trainer', title:'練馬師'}, {key:'country', title:'國籍'}, {key:'stable', title:'馬房'}
        ]);
      } else if (id === 'db-horses') {
  // 例如喺 menu 點擊時判斷 id === 'db-horses' 就執行：
renderDB('db-horses', '/api/horses', [
  { key: 'horse_id',   title: '馬匹ID' },
  { key: 'name',       title: '名稱' },
  { key: 'horse_code', title: '馬匹編號' },
  { key: 'sex',        title: '性別' },
  { key: 'age',        title: '年齡' },
  { key: 'colour',     title: '毛色' },
  { key: 'country',    title: '出生地' },
  { key: 'trainer',    title: '練馬師' },
  { key: 'owner',      title: '馬主' },
  { key: 'updated_at', title: '更新時間' }
]);

      } else if (id === 'db-venues') {
        renderDB('db-venues', '/api/venues', [
          {key:'code', title:'代碼'}, {key:'name_zh', title:'場地'}
        ]);
      }
    });
  });

  const initial = location.hash?.replace('#', '') || 'db-horses';
activate(document.getElementById(initial) ? initial : 'db-horses');
if (initial.startsWith('race-')) {
  renderRace(initial.split('-')[1]);
} else if (initial === 'db-horses') {
  renderDB('db-horses', '/api/horses', [
    { key: 'horse_id',       title: '馬匹ID' },
    { key: 'name_chi',       title: '中文名' },
    { key: 'name_eng',       title: '英文名' },
    { key: 'sex',            title: '性別' },
    { key: 'age',            title: '年齡' },
    { key: 'colour',         title: '毛色' },
    { key: 'country',        title: '出生地' },
    { key: 'trainer_id',     title: '練馬師ID' },
    { key: 'owner',          title: '馬主' },
    { key: 'current_rating', title: '現評分' },
    { key: 'season_rating',  title: '季內評分' },
    { key: 'season_prize',   title: '季內獎金' },
    { key: 'total_prize',    title: '累計獎金' },
    { key: 'last10_racedays',title: '近10次出賽' },
    { key: 'updated_at',     title: '更新時間' }
  ]);
}

});
