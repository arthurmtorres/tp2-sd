package tp2;

/** Página do painel, embutida como texto para não depender da configuração de resources do Maven. */
final class Dashboard {
    private Dashboard() {}

    static final String HTML = """
<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TP2 · Painel ZooKeeper</title>
<style>
:root{--bg:#edf0f4;--panel:#fff;--ink:#18263b;--muted:#5e6b7e;--line:#d5dbe5;
--accent:#2457d6;--ok:#17825a;--bad:#c62f2a;--warn:#b7791f;--okbg:#e3f4ec;--badbg:#fbe6e5;--warnbg:#fbf1dc;--accbg:#e4ebfb}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 "Segoe UI",system-ui,sans-serif}
header{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:20px 28px;background:var(--ink);color:#fff}
h1{margin:0;font-size:22px;font-weight:650}
header p{margin:4px 0 0;color:#b9c4d6;font-size:14px}
.pill{padding:8px 18px;border-radius:999px;font-weight:650;background:#35465f;white-space:nowrap}
.pill.run{background:var(--accent)}.pill.OK{background:var(--ok)}.pill.VIOLACAO{background:var(--bad)}.pill.INCOMPLETO{background:var(--warn)}
main{max-width:1180px;margin:0 auto;padding:22px 20px 40px;display:grid;gap:18px}
section{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:18px 20px}
h2{margin:0 0 12px;font-size:16px}
.note{margin:10px 0 0;color:var(--muted);font-size:13px}
.zkgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.zk{border:1px solid var(--line);border-left-width:6px;border-radius:8px;padding:10px 14px}
.zk.leader{border-left-color:var(--warn);background:var(--warnbg)}
.zk.follower{border-left-color:var(--accent);background:var(--accbg)}
.zk.down{border-left-color:var(--bad);background:var(--badbg)}
.zk b{display:block;font-size:17px}.zk span{color:var(--muted);font-size:13px}
#rbox{display:flex;align-items:center;gap:22px;border-width:2px;transition:background .2s,border-color .2s}
#rbox.free{border-color:var(--line)}
#rbox.busy{border-color:var(--ok);background:var(--okbg)}
#rbox.clash{border-color:var(--bad);background:var(--badbg)}
#rbox .big{font-size:42px;font-weight:700;min-width:70px}
#rbox .state{font-size:22px;font-weight:650}
#rbox .who{color:var(--muted)}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px}
.stat{border:1px solid var(--line);border-radius:8px;padding:10px 14px}
.stat small{color:var(--muted)}.stat b{display:block;font-size:26px}
.stat.bad{background:var(--badbg);border-color:var(--bad)}
.bar{height:8px;background:var(--line);border-radius:4px;overflow:hidden;margin-top:6px}
.bar i{display:block;height:100%;background:var(--accent);transition:width .3s}
.two{display:grid;grid-template-columns:1fr 1fr;gap:18px}
@media(max-width:800px){.two{grid-template-columns:1fr}header{flex-direction:column;align-items:flex-start}}
table{width:100%;border-collapse:collapse}
td{padding:6px 4px;border-top:1px solid var(--line)}tr:first-child td{border-top:0}
td:first-child{font-weight:650;width:70px}
.row{display:grid;grid-template-columns:50px 1fr 36px;gap:8px;align-items:center;margin:6px 0}
#log{max-height:340px;overflow:auto;font:13px/1.5 ui-monospace,Consolas,monospace}
.ev{display:grid;grid-template-columns:96px 190px 1fr;gap:8px;padding:2px 0;border-top:1px solid #eef1f5}
.ev time{color:var(--muted)}
.ev.BEGIN b{color:var(--accent)}.ev.END b{color:var(--ok)}
.ev.ARRIVE b,.ev.PASS b{color:var(--warn)}
.ev.VIOLATION_OVERLAP,.ev.VIOLATION_BARRIER_ENTRY,.ev.VIOLATION_BARRIER_EXIT{background:var(--badbg);color:var(--bad)}
.ev.SUMMARY{background:var(--accbg);font-weight:650}
</style>
</head>
<body>
<header>
  <div><h1>Sincronização distribuída com ZooKeeper e Curator</h1><p id="sub">Aguardando o observador…</p></div>
  <div id="status" class="pill">conectando…</div>
</header>
<main>
  <section>
    <h2>Ensemble ZooKeeper</h2>
    <div id="zk" class="zkgrid"></div>
    <p class="note" id="quorum"></p>
  </section>
  <section id="rbox" class="free">
    <div class="big" id="rbig">R</div>
    <div><div class="state" id="rstate">Livre</div><div class="who" id="rwho">Nenhum nó está usando o recurso.</div></div>
  </section>
  <section>
    <div class="stats">
      <div class="stat"><small>Acessos concluídos</small><b id="done">0</b><div class="bar"><i id="prog" style="width:0"></i></div></div>
      <div class="stat"><small>Máximo de acessos simultâneos</small><b id="maxa">0</b></div>
      <div class="stat" id="ovbox"><small>Sobreposições detectadas</small><b id="ov">0</b></div>
      <div class="stat" id="bvbox"><small>Violações da barreira</small><b id="bv">0</b></div>
    </div>
  </section>
  <div class="two">
    <section><h2>Nós de sincronização</h2><table id="nodes"></table></section>
    <section><h2>Acessos concluídos por cliente</h2><div id="clients"></div></section>
  </div>
  <section><h2>Eventos recebidos pelo observador</h2><div id="log"></div></section>
</main>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const ROLE = {leader:'líder', follower:'seguidor', standalone:'único servidor'};

function render(s) {
  const pill = $('status');
  pill.textContent = s.finished ? s.status : 'em execução';
  pill.className = 'pill ' + (s.finished ? s.status : 'run');
  $('sub').textContent = 'Cenário ' + s.label + ' · semente ' + s.seed + ' · ' + s.nodes + ' nós · barreira ' + (s.barrier ? 'ligada' : 'desligada');

  const up = s.zk.filter(z => z.up).length;
  $('zk').innerHTML = s.zk.map(z => {
    const cls = !z.up ? 'down' : (z.mode === 'leader' ? 'leader' : 'follower');
    const role = !z.up ? 'fora do ar' : (ROLE[z.mode] || z.mode);
    return '<div class="zk ' + cls + '"><b>' + esc(role) + '</b>' + esc(z.addr) +
      '<br><span>znodes: ' + esc(z.znodes) + ' · conexões: ' + esc(z.conns) + '</span></div>';
  }).join('');
  const need = Math.floor(s.zk.length / 2) + 1;
  $('quorum').textContent = s.zk.length + ' servidores configurados, ' + up + ' ativos. Quórum exigido: ' + need + '. ' +
    (up >= need ? 'O serviço está disponível.' : 'Sem quórum: o serviço não aceita operações.');

  const h = s.holders;
  const box = $('rbox');
  box.className = h.length === 0 ? 'free' : (h.length === 1 ? 'busy' : 'clash');
  $('rbig').textContent = h.length;
  $('rstate').textContent = h.length === 0 ? 'Recurso R livre' : (h.length === 1 ? 'Recurso R em uso' : 'Sobreposição no recurso R');
  $('rwho').textContent = h.length === 0 ? 'Nenhum nó está usando o recurso.' :
    h.map(x => 'nó ' + x.split(' ')[0] + ' atendendo ' + x.split(' ')[1]).join(' · ');

  $('done').textContent = s.completed + ' de ' + (s.expected < 0 ? '?' : s.expected);
  $('prog').style.width = (s.expected > 0 ? Math.min(100, 100 * s.completed / s.expected) : 0) + '%';
  $('maxa').textContent = s.maxActive;
  $('ov').textContent = s.overlaps;
  $('bv').textContent = s.barrierViol;
  $('ovbox').className = 'stat' + (s.overlaps > 0 ? ' bad' : '');
  $('bvbox').className = 'stat' + (s.barrierViol > 0 ? ' bad' : '');

  $('nodes').innerHTML = Object.entries(s.nodeState).map(([k, v]) => '<tr><td>' + esc(k) + '</td><td>' + esc(v) + '</td></tr>').join('');
  const pc = Object.entries(s.perClient);
  const mx = Math.max(1, ...pc.map(e => e[1]));
  $('clients').innerHTML = pc.map(([k, v]) => '<div class="row"><b>' + esc(k) + '</b><div class="bar"><i style="width:' + (100 * v / mx) + '%"></i></div><span>' + v + '</span></div>').join('') || '<p class="note">Nenhum acesso concluído ainda.</p>';

  $('log').innerHTML = s.events.slice().reverse().map(e =>
    '<div class="ev ' + esc(e.type) + '"><time>' + esc(e.t) + '</time><b>' + esc(e.type) + '</b><span>' + esc(e.text) + '</span></div>').join('');
}

async function tick() {
  try {
    const r = await fetch('/api/state', {cache: 'no-store'});
    render(await r.json());
  } catch (e) {
    const p = $('status');
    p.textContent = 'observador desconectado';
    p.className = 'pill';
  }
}
tick();
setInterval(tick, 700);
</script>
</body>
</html>
""";
}