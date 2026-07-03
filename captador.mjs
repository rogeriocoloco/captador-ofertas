// Captador de ofertas — Wasender webhook -> resolve -> taggeia -> envia (multi-oferta)
// Rodar: node captador.mjs            (sobe o servidor do webhook)
//        node captador.mjs --test URL (envia 1 oferta na hora, sem fila/delay)
// Requer Node 18+ (fetch nativo). Zero dependencias externas.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// ---------- .env loader minimalista ----------
const __dir = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dir, '..');
for (const envPath of [path.join(ROOT, '.env'), path.join(__dir, '.env')]) {
  if (fs.existsSync(envPath)) {
    for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
      const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/i);
      if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
    }
  }
}

const CFG = {
  TOKEN:  process.env.WASENDER_TOKEN_BR || '',
  SOURCE: process.env.SOURCE_GROUP_JID  || '5521985322034-1542837195@g.us', // TÁ BUGADO
  TARGET: process.env.TARGET_GROUP_JID  || '120363423312940352@g.us',       // Preço de Banana
  AMZ_TAG: process.env.AMAZON_TAG || 'bananadeals-20',
  ML_COOKIE: process.env.ML_COOKIE || '',
  ML_TAG:    process.env.ML_TAG || '',
  MIN_DELAY: (+process.env.MIN_DELAY_S || 30) * 1000,   // jitter minimo entre envios
  MAX_DELAY: (+process.env.MAX_DELAY_S || 180) * 1000,  // jitter maximo (anti-espelho)
  PORT: +process.env.PORT || 3000,
  UA: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
};
const SEND_URL = 'https://wasenderapi.com/api/send-message'; // SEM www (www = Cloudflare 1010)

// ---------- fallback: puxa segredos que ja existem no projeto ("la tem tudo") ----------
function safeRead(p) { try { return fs.readFileSync(p, 'utf8'); } catch { return ''; } }
(function loadFromProject() {
  // Wasender token BR do arquivo de APIs
  if (!CFG.TOKEN) {
    const apis = safeRead(path.join(ROOT, 'Algumas APIs.txt'));
    const line = apis.split('\n').find(l => /wasender/i.test(l) && /brazil|br\b/i.test(l));
    const t = line && (line.match(/Bearer\s+([A-Za-z0-9]+)/i) || line.match(/([a-f0-9]{64})/i));
    if (t) CFG.TOKEN = t[1];
  }
  // Cookie/tag do ML do fluxo BUSCAR PROMO MERCADO LIVRE
  if (!CFG.ML_COOKIE || !CFG.ML_TAG) {
    const flow = safeRead(path.join(ROOT, 'Fluxos', 'BUSCAR PROMO MERCADO LIVRE - BACKUP REMOTO.json'));
    try {
      for (const n of JSON.parse(flow || '{}').nodes || [])
        for (const a of n.parameters?.assignments?.assignments || []) {
          if (a.name === 'cookie' && a.value && !CFG.ML_COOKIE) CFG.ML_COOKIE = a.value;
          if (a.name === 'tag_afiliado' && a.value && !CFG.ML_TAG) CFG.ML_TAG = a.value;
        }
    } catch {}
  }
  console.log(`creds: wasender=${CFG.TOKEN ? 'ok' : 'FALTA'} ml=${CFG.ML_COOKIE ? 'ok(' + CFG.ML_TAG + ')' : 'off'}`);
})();

// ---------- dedup persistente ----------
const DATA = path.join(ROOT, 'data'); fs.mkdirSync(DATA, { recursive: true });
const DEDUP = path.join(DATA, 'enviados.json');
const sent = new Set(fs.existsSync(DEDUP) ? JSON.parse(fs.readFileSync(DEDUP, 'utf8')) : []);
const remember = (id) => { sent.add(id); fs.writeFileSync(DEDUP, JSON.stringify([...sent], null, 0)); };

const log = (...a) => console.log(new Date().toISOString(), ...a);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const rand = (a, b) => Math.floor(Math.random() * (b - a) + a);

async function getHtml(url, cookie) {
  const headers = { 'User-Agent': CFG.UA, 'Accept-Language': 'pt-BR,pt;q=0.9', Accept: 'text/html' };
  if (cookie) headers.Cookie = cookie; // cookie ML fura o challenge de bot (pagina real -> pega og:image)
  const r = await fetch(url, { headers, redirect: 'follow' });
  return { finalUrl: r.url, html: await r.text() };
}

function pick(html, ...res) { for (const re of res) { const m = html.match(re); if (m) return (m[1] || '').replace(/&amp;/g, '&').trim(); } return ''; }

// ---------- resolvers ----------
async function resolveAmazon(url) {
  const { finalUrl, html } = await getHtml(url);
  const asin = pick(finalUrl + '\n' + html, /\/dp\/([A-Z0-9]{10})/, /"asin"\s*:\s*"([A-Z0-9]{10})"/i);
  if (!asin) return null;
  const image = pick(html, /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)/i, /"hiRes"\s*:\s*"(https:[^"]+)"/i, /data-old-hires=["'](https:[^"']+)/i);
  const title = pick(html, /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)/i, /<title>([^<]+)</i).replace(/\s*[:|-]\s*Amazon.*$/i, '');
  const price = pick(html, /class="a-offscreen">\s*(R\$[\s\d.,]+)/i);
  return { productId: asin, link: `https://www.amazon.com.br/dp/${asin}?tag=${CFG.AMZ_TAG}`, image, title, price, network: 'amazon' };
}

async function resolveML(url) {
  let finalUrl = url, html = '';
  try { ({ finalUrl, html } = await getHtml(url, CFG.ML_COOKIE)); } catch (e) { log('  ML page block', String(e).slice(0, 80)); }
  // usa a URL original (o createLink aceita crua); canonical so se a pagina veio de verdade
  const canonical = pick(html, /rel=["']canonical["']\s+href=["']([^"']+)/i) || url;
  const productId = (canonical.match(/(MLB-?\d+)/) || [,''])[1] || canonical;
  const image = pick(html, /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)/i);
  const title = pick(html, /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)/i, /<title>([^<]+)</i);
  const price = pick(html, /"price"\s*:\s*"?([\d.,]+)"?/i);
  let link = null;
  if (CFG.ML_COOKIE && CFG.ML_TAG) {
    try {
      const r = await fetch('https://www.mercadolivre.com.br/affiliate-program/api/v2/affiliates/createLink', {
        method: 'POST',
        headers: { Cookie: CFG.ML_COOKIE, Origin: 'https://www.mercadolivre.com.br', Referer: 'https://www.mercadolivre.com.br/afiliados/linkbuilder', 'User-Agent': CFG.UA, 'Content-Type': 'application/json' },
        body: JSON.stringify({ urls: [canonical], tag: CFG.ML_TAG }),
      });
      const j = await r.json().catch(() => ({}));
      link = j.shortUrl || j.short_link || j?.data?.[0]?.short_link || j?.urls?.[0]?.shortUrl || null;
    } catch (e) { log('ML createLink erro', String(e).slice(0, 120)); }
  }
  return { productId, link, image, title, price: price ? 'R$ ' + price : '', network: 'ml' };
}

// ---------- montar copy (nao copia literal o autor -> anti-espelho) ----------
const HEADERS = ['🔥 ACHADO', '💥 OFERTA', '🍌 PREÇO DE BANANA', '⚡ PROMO', '🚨 BAIXOU'];
function buildText(o, cupom) {
  const L = [`${HEADERS[rand(0, HEADERS.length)]}${o.title ? ' — ' + o.title : ''}`];
  if (o.price) L.push(`💰 ${o.price}`);
  L.push('', `👉 ${o.link}`);
  if (cupom) L.push('', `🎟️ Cupom: *${cupom}*`);
  return L.join('\n');
}

async function sendOffer(o, cupom) {
  const payload = { to: CFG.TARGET, text: buildText(o, cupom) };
  if (o.image) payload.imageUrl = o.image;
  const r = await fetch(SEND_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + CFG.TOKEN, 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(payload) });
  const t = await r.text();
  log('  send', r.status, t.slice(0, 160));
  return r.ok;
}

async function resolve(url) {
  if (/amzn\.to|amazon\./i.test(url)) return resolveAmazon(url);
  if (/mercadoliv|mercadolibre|\/sec\//i.test(url)) return resolveML(url);
  return null;
}

// ---------- fila global (respeita 1 msg/5s do Wasender + jitter anti-espelho) ----------
const queue = []; let working = false;
async function worker() {
  if (working) return; working = true;
  while (queue.length) {
    const { url, cupom } = queue.shift();
    try {
      const o = await resolve(url);
      if (!o) { log('  ignorado (rede desconhecida)', url); continue; }
      if (!o.link) { log('  SEM link de afiliado (ML sem cookie?) -> pula', o.productId); continue; }
      if (sent.has(o.productId)) { log('  dup, ja enviado', o.productId); continue; }
      const ok = await sendOffer(o, cupom);
      if (ok) remember(o.productId);
    } catch (e) { log('  erro processando', url, String(e).slice(0, 160)); }
    if (queue.length) await sleep(Math.max(6000, rand(CFG.MIN_DELAY, CFG.MAX_DELAY))); // >=6s garante o limite de 5s
  }
  working = false;
}

// ---------- parsing do webhook Wasender ----------
function parseMessage(body) {
  const msg = body?.data?.messages || body?.messages || body?.data || body || {};
  const m = Array.isArray(msg) ? msg[0] : msg;
  const jid = m?.key?.remoteJid || m?.remoteJid || m?.from || m?.chatId || '';
  const fromMe = m?.key?.fromMe === true;
  const text = m?.message?.conversation || m?.message?.extendedTextMessage?.text || m?.message?.text?.body
    || m?.message?.imageMessage?.caption || m?.text || m?.body || '';
  return { jid, fromMe, text };
}

function extractOffers(text) {
  const urls = [...new Set((text.match(/https?:\/\/[^\s)]+/gi) || []))];
  const offers = urls.filter(u => /amzn\.to|amazon\.|mercadoliv|mercadolibre|\/sec\//i.test(u));
  const cupom = (text.match(/cupom[:\s]+([A-Z0-9]{4,})/i) || [,''])[1];
  return { offers, cupom };
}

function handle(body) {
  const { jid, fromMe, text } = parseMessage(body);
  if (fromMe) return { skip: 'fromMe' };
  if (CFG.SOURCE && jid && jid !== CFG.SOURCE) return { skip: `outro grupo (${jid})` };
  const { offers, cupom } = extractOffers(text);
  if (!offers.length) return { skip: 'sem ofertas' };
  for (const url of offers) queue.push({ url, cupom });
  worker();
  return { enfileiradas: offers.length };
}

// ---------- modo teste ----------
if (process.argv.includes('--test')) {
  const url = process.argv[process.argv.indexOf('--test') + 1];
  if (!url) { console.error('uso: node captador.mjs --test <url>'); process.exit(1); }
  const o = await resolve(url);
  log('resolvido:', JSON.stringify(o));
  if (o?.link) await sendOffer(o, '');
  process.exit(0);
}

// ---------- servidor ----------
http.createServer((req, res) => {
  if (req.method === 'POST' && req.url.startsWith('/webhook/captador')) {
    let b = ''; req.on('data', c => b += c);
    req.on('end', () => {
      let out; try { out = handle(JSON.parse(b || '{}')); } catch (e) { out = { erro: String(e) }; }
      log('webhook', JSON.stringify(out));
      res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: true, ...out }));
    });
  } else { res.writeHead(200); res.end('captador up'); }
}).listen(CFG.PORT, () => log(`captador on :${CFG.PORT}  source=${CFG.SOURCE}  target=${CFG.TARGET}  tag=${CFG.AMZ_TAG}`));
