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
  COUPON_REPOST: process.env.COUPON_REPOST === '1', // repostar cupons-sem-produto com link proprio (padrao: off)
  ML_COOKIE: process.env.ML_COOKIE || '',
  ML_TAG:    process.env.ML_TAG || '',
  MIN_DELAY: (+process.env.MIN_DELAY_S || 8) * 1000,    // jitter minimo entre envios (>=6s p/ o limite 1msg/5s)
  MAX_DELAY: (+process.env.MAX_DELAY_S || 25) * 1000,   // jitter maximo (rapido o bastante p/ nao acumular em rajada)
  PORT: +process.env.PORT || 3000,
  UA: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  // UA de crawler p/ scraping: Amazon bloqueia IP de datacenter no browser normal, mas libera og:image/title pro crawler de preview
  SCRAPE_UA: 'WhatsApp/2.23.20.0',
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

// ---------- rotas (multi-grupo): roteia por JID de ORIGEM ----------
// Cada rota: de qual grupo pegar (source) -> pra qual mandar (target) usando qual token/tag/mercado.
// BR: Amazon + Mercado Livre. US: so Amazon (amazon.com). O numero que RECEBE e o que ENVIA podem ser
// sessoes/tokens diferentes do Wasender (ex.: US recebe por um numero e posta por outro).
const ROUTES = [
  { market: 'BR', source: CFG.SOURCE, target: CFG.TARGET, token: CFG.TOKEN, amzTag: CFG.AMZ_TAG, ml: true },
];
if (process.env.SOURCE_GROUP_JID_US && process.env.WASENDER_TOKEN_US) {
  ROUTES.push({
    market: 'US',
    source: process.env.SOURCE_GROUP_JID_US,
    target: process.env.TARGET_GROUP_JID_US || '',
    token: process.env.WASENDER_TOKEN_US,
    amzTag: process.env.AMAZON_TAG_US || 'rogeriocoloco-20',
    ml: false,
  });
}
const routeFor = (jid) => ROUTES.find(r => r.source === jid) || null;
console.log('rotas:', ROUTES.map(r => `${r.market}[${r.source} -> ${r.target} tag=${r.amzTag} ml=${r.ml} tok=${r.token ? 'ok' : 'FALTA'}]`).join('  |  '));

// ---------- dedup persistente ----------
const DATA = path.join(ROOT, 'data'); fs.mkdirSync(DATA, { recursive: true });
const DEDUP = path.join(DATA, 'enviados.json');
const sent = new Set(fs.existsSync(DEDUP) ? JSON.parse(fs.readFileSync(DEDUP, 'utf8')) : []);
const remember = (id) => { sent.add(id); fs.writeFileSync(DEDUP, JSON.stringify([...sent], null, 0)); };

// ---------- fila persistente (sobrevive a restart -> nao perde oferta em espera) ----------
const QUEUE_FILE = path.join(DATA, 'fila.json');
const saveQueue = () => { try { fs.writeFileSync(QUEUE_FILE, JSON.stringify(queue, null, 0)); } catch (e) { console.error('saveQueue', e.message); } };

const log = (...a) => console.log(new Date().toISOString(), ...a);

// ---------- historico em memoria (visibilidade via GET /status) ----------
const BOOT = Date.now();
const rxLog = [];   // ultimas mensagens RECEBIDAS + decisao
const txLog = [];   // ultimos envios feitos pro grupo
const ringPush = (arr, item) => { arr.push({ t: new Date().toISOString(), ...item }); if (arr.length > 60) arr.shift(); };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const rand = (a, b) => Math.floor(Math.random() * (b - a) + a);

// fetch com timeout (evita conexao travada segurar a fila)
async function fetchT(url, opts = {}, ms = 8000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try { return await fetch(url, { ...opts, signal: ac.signal }); }
  finally { clearTimeout(t); }
}

async function getHtml(url, cookie) {
  // UA de crawler: Amazon bloqueia IP de datacenter no browser normal, mas serve og:image/title pro crawler de preview
  const headers = { 'User-Agent': CFG.SCRAPE_UA, 'Accept-Language': 'pt-BR,pt;q=0.9', Accept: 'text/html' };
  if (cookie) headers.Cookie = cookie; // cookie ML fura o challenge de bot (pagina real -> pega og:image)
  const r = await fetchT(url, { headers, redirect: 'follow' });
  return { finalUrl: r.url, html: await r.text() };
}

function pick(html, ...res) { for (const re of res) { const m = html.match(re); if (m) return (m[1] || '').replace(/&amp;/g, '&').trim(); } return ''; }

// valida que a URL e uma imagem real (evita mandar placeholder/quadro preto no WhatsApp)
async function validImg(url) {
  try {
    const r = await fetchT(url, {}, 6000);
    if (!r.ok) return false;
    if (!/image\//i.test(r.headers.get('content-type') || '')) return false;
    const len = +(r.headers.get('content-length') || 0) || (await r.arrayBuffer()).byteLength;
    return len > 2000; // placeholder da Amazon costuma ser um gif de ~43 bytes
  } catch { return false; }
}

// ---------- resolvers ----------
async function resolveAmazon(url, route) {
  const tld = route.market === 'US' ? 'com' : 'com.br';
  const lang = route.market === 'US' ? 'en-US,en;q=0.9' : 'pt-BR,pt;q=0.9';
  // passo 1: ASIN seguindo o redirect com UA normal (a URL final tem /dp/ASIN mesmo se a pagina vier como captcha)
  let canon = url, html1 = '';
  try {
    const r = await fetchT(url, { headers: { 'User-Agent': CFG.UA, 'Accept-Language': lang, Accept: 'text/html' }, redirect: 'follow' });
    canon = r.url; html1 = await r.text();
  } catch (e) { log('  amazon p1', String(e).slice(0, 80)); }
  const asin = pick(canon + '\n' + html1, /\/(?:dp|gp\/product|gp\/aw\/d)\/([A-Z0-9]{10})/, /"asin"\s*:\s*"([A-Z0-9]{10})"/i);
  if (!asin) return null;
  // passo 2: og:image/title/preco com UA de crawler; a Amazon devolve INTERMITENTE, entao tenta ate 3x
  let image = '', title = '', price = '';
  for (let i = 0; i < 3 && !image; i++) {
    try {
      const { html } = await getHtml(`https://www.amazon.${tld}/dp/${asin}`);
      if (!title) title = pick(html, /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)/i).replace(/\s*[:|-]\s*Amazon.*$/i, '');
      if (!price) price = pick(html, /class="a-offscreen">\s*([R$][\s\d.,]*\d)/i); // R$ (BR) ou $ (US)
      const og = pick(html, /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)/i, /"hiRes"\s*:\s*"(https:[^"]+)"/i, /data-old-hires=["'](https:[^"']+)/i);
      if (og && await validImg(og)) image = og;
    } catch (e) { log('  amazon p2', String(e).slice(0, 80)); }
  }
  // fallback: imagem por ASIN, so se for valida (o endpoint devolve gif 43b de placeholder p/ alguns ASIN)
  if (!image) { const det = `https://images-na.ssl-images-amazon.com/images/P/${asin}.jpg`; if (await validImg(det)) image = det; }
  return { productId: asin, link: `https://www.amazon.${tld}/dp/${asin}?tag=${route.amzTag}`, image, title, price, network: 'amazon' };
}

// chama o createLink oficial do ML; devolve o short link meli.la (com nossa tag)
async function mlCreateLink(productUrl) {
  try {
    const r = await fetchT('https://www.mercadolivre.com.br/affiliate-program/api/v2/affiliates/createLink', {
      method: 'POST',
      headers: { Cookie: CFG.ML_COOKIE, Origin: 'https://www.mercadolivre.com.br', Referer: 'https://www.mercadolivre.com.br/afiliados/linkbuilder', 'User-Agent': CFG.UA, 'Content-Type': 'application/json' },
      body: JSON.stringify({ urls: [productUrl], tag: CFG.ML_TAG }),
    }, 12000);
    const j = await r.json().catch(() => ({}));
    const u = j?.urls?.[0] || {};
    if (u.message && /not allowed/i.test(u.message)) { log('  ML createLink recusou', u.message); return null; }
    // short_link direto, ou extrai do texto ("Ou acesse o link: https://meli.la/xxx")
    return u.short_link || u.shortUrl || (u.text && (u.text.match(/https:\/\/meli\.la\/\w+/) || [null])[0]) || j.shortUrl || null;
  } catch (e) { log('  ML createLink erro', String(e).slice(0, 120)); return null; }
}

async function resolveML(url) {
  // meli.la/xxx e /sec/ redirecionam pra pagina /social/<tag> (wrapper do afiliado).
  // A pagina lista o produto DESTACADO (card-featured) + recomendacoes. Precisamos so do destacado.
  let finalUrl = url, html = '';
  try {
    const r = await fetchT(url, { headers: { 'User-Agent': CFG.UA, Cookie: CFG.ML_COOKIE, 'Accept-Language': 'pt-BR,pt;q=0.9', Accept: 'text/html' }, redirect: 'follow' }, 12000);
    finalUrl = r.url; html = (await r.text()).replace(/&amp;/g, '&');
  } catch (e) { log('  ML page block', String(e).slice(0, 80)); }

  // produto compartilhado = card marcado como /home/card-featured/element; pega o item_id real (listing do vendedor)
  let itemId = '';
  const feat = html.match(/https?:\/\/[^"'\s\\]*?\/p\/MLB\d+[^"'\s\\]*?c_id=\/home\/card-featured\/element/i);
  if (feat) itemId = (feat[0].match(/item_id[:%]3?A?(MLB\d+)/i) || feat[0].match(/wid=(MLB\d+)/i) || feat[0].match(/\/p\/(MLB\d+)/i) || [, ''])[1];
  // fallback: primeiro item_id do body, senao MLB da propria URL
  if (!itemId) itemId = (html.match(/pdp_filters=item_id%3A(MLB\d+)/i) || html.match(/item_id[:%]3?A?(MLB\d+)/i) || (finalUrl + url).match(/(MLB-?\d+)/i) || [, ''])[1];
  itemId = (itemId || '').replace('-', '');

  const image = pick(html, /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)/i);
  const title = pick(html, /<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)/i, /<title>([^<]+)</i);
  const price = pick(html, /"price"\s*:\s*"?([\d.,]+)"?/i);

  if (!itemId || !CFG.ML_COOKIE || !CFG.ML_TAG) { log('  ML sem itemId/creds', itemId || '-'); return null; }
  // createLink so aceita o listing do vendedor (produto.mercadolivre.com.br/MLB-<id>); /p/MLB (catalogo) e recusado
  const link = await mlCreateLink('https://produto.mercadolivre.com.br/' + itemId.replace('MLB', 'MLB-'));
  if (!link) return null;
  return { productId: itemId, link, image, title, price: price ? 'R$ ' + price : '', network: 'ml' };
}

// ---------- montar copy (nao copia literal o autor -> anti-espelho) ----------
const HEADERS = ['🔥 ACHADO', '💥 OFERTA', '🍌 PREÇO DE BANANA', '⚡ PROMO', '🚨 BAIXOU'];
const HEADERS_US = ['🔥 HOT DEAL', '💥 PRICE DROP', '⚡ DEAL ALERT', '🚨 PRICE DROP', '📉 DEAL'];
function buildText(o, cupom, route) {
  const us = route.market === 'US';
  const H = us ? HEADERS_US : HEADERS;
  const L = [`${H[rand(0, H.length)]}${o.title ? ' — ' + o.title : ''}`];
  if (o.price) L.push(`💰 ${o.price}`);
  L.push('', `👉 ${o.link}`);
  if (cupom) L.push('', us ? `🎟️ Coupon: *${cupom}*` : `🎟️ Cupom: *${cupom}*`);
  return L.join('\n');
}

// envia usando o token/destino DA ROTA (US recebe por um numero e posta por outro -> token diferente)
async function wasend(route, payload) {
  const r = await fetchT(SEND_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + route.token, 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ to: route.target, ...payload }) }, 15000);
  const t = await r.text();
  ringPush(txLog, { market: route.market, to: route.target, ok: r.ok, status: r.status, txt: (payload.text || payload.imageUrl || '').slice(0, 90) });
  return { ok: r.ok, status: r.status, body: t.slice(0, 160) };
}

async function sendOffer(o, cupom, route) {
  const payload = { text: buildText(o, cupom, route) };
  if (o.image) payload.imageUrl = o.image;
  const r = await wasend(route, payload);
  log(`  send ${route.market}`, r.status, r.body);
  return r.ok;
}

// segue redirects HTTP + meta-refresh ate a Amazon (encurtadores do concorrente: tabuga.do -> amzn.to -> amazon)
async function followToAmazon(start) {
  let url = start;
  for (let i = 0; i < 8; i++) {
    if (/amazon\./i.test(url)) return url; // ja chegou na amazon: nao carrega a pagina (1MB)
    let r;
    try { r = await fetchT(url, { redirect: 'manual', headers: { 'User-Agent': CFG.UA, Accept: 'text/html', 'Accept-Language': 'pt-BR' } }, 8000); } catch { return null; }
    const loc = r.headers.get('location');
    if (loc && r.status >= 300 && r.status < 400) { url = loc.startsWith('http') ? loc : new URL(loc, url).href; continue; }
    let b = ''; try { b = await r.text(); } catch {}
    const mr = (b.match(/http-equiv=["']refresh["'][^>]*url=([^"'>\s]+)/i) || [, ''])[1];
    if (mr) { url = mr.startsWith('http') ? mr : new URL(mr, url).href; continue; }
    break;
  }
  return /amazon\./i.test(url) ? url : null;
}
// reescreve a URL final da Amazon com a NOSSA tag (mantem /dp/ASIN ou pagina de promo tipo /primeday; dropa tracking deles)
function amazonRetag(u, route) { try { const x = new URL(u); return x.origin + x.pathname + '?tag=' + route.amzTag; } catch { return null; } }
// converte o link de resgate do concorrente (tabuga.do/amzn.to/...) na Amazon com a nossa tag
async function resolveResgate(rawUrl, route) {
  if (!rawUrl) return null;
  const amz = await followToAmazon(rawUrl);
  return amz ? amazonRetag(amz, route) : null;
}

// cupom-sem-produto: reposta o codigo + regras com link proprio (dropa link/imagem da origem)
async function sendCoupon(item, route) {
  const us = route.market === 'US';
  const L = [`🎟️ ${us ? 'AMAZON COUPON' : 'CUPOM AMAZON'}${item.cupom ? ' — *' + item.cupom + '*' : ''}`];
  if (item.rules) L.push(item.rules);
  let link = null;
  if (item.resgate) { try { link = await resolveResgate(item.resgate, route); } catch (e) { log('  resgate erro', String(e).slice(0, 80)); } }
  const home = `https://www.amazon.${us ? 'com' : 'com.br'}?tag=${route.amzTag}`;
  L.push('', `👉 ${link || home}`);
  const r = await wasend(route, { text: L.join('\n') });
  log(`  send cupom ${route.market}`, r.status, r.body);
  return r.ok;
}

// aviso de status de cupom (ex.: "CUPOM X ESGOTADO") — reposta o texto limpo, sem link/imagem
function cleanStatus(text) {
  return (text || '').replace(/https?:\/\/[^\s]+/g, ' ').replace(/tabugado[^\s]*/gi, ' ').replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim().slice(0, 220);
}
async function sendStatus(text, route) {
  const r = await wasend(route, { text });
  log(`  send status ${route.market}`, r.status, r.body);
  return r.ok;
}

async function resolve(url, route) {
  if (/amzn\.to|amazon\.|\.amazon\/|a\.co\//i.test(url)) return resolveAmazon(url, route);
  if (route.ml && /mercadoliv|mercadolibre|meli\.la|\/sec\//i.test(url)) return resolveML(url);
  return null;
}

// ---------- fila global (respeita 1 msg/5s do Wasender + jitter anti-espelho) ----------
const queue = (() => { try { return fs.existsSync(QUEUE_FILE) ? JSON.parse(fs.readFileSync(QUEUE_FILE, 'utf8')) : []; } catch { return []; } })();
let working = false;
const seenMsg = new Set(); // dedup por id de mensagem (Wasender manda ~3 eventos por msg)
// mantem o item pra nova tentativa (falha transitoria: bloqueio/captcha/rede). Move pro fim da fila
// pra os outros passarem na frente; desiste (e remove) apos MAX_TRIES. Retorna true se deve remover agora.
const MAX_TRIES = 4;
function keepForRetry(item, why) {
  item.tries = (item.tries || 0) + 1;
  if (item.tries >= MAX_TRIES) { log(`  DESISTIU apos ${item.tries}x:`, why, item.url || item.cupom || ''); return true; }
  log(`  retry ${item.tries}/${MAX_TRIES - 1}:`, why, item.url || item.cupom || '');
  if (queue.length > 1) queue.push(queue.shift()); // manda pro fim; se for o unico, fica e re-tenta apos o sleep
  return false;
}

async function worker() {
  if (working) return; working = true;
  while (queue.length) {
    const item = queue[0]; // PEEK: so remove da fila (disco) depois de enviar OK -> sobrevive a restart/crash sem perder
    let remove = true;
    // rota da mensagem (por JID de origem). Legado: itens antigos sem src caem na rota BR (a primeira).
    const route = routeFor(item.src) || ROUTES.find(r => r.market === 'BR') || ROUTES[0];
    try {
      if (!route || !route.token || !route.target) { log('  SEM rota/token/destino -> descarta', item.src || item.url || ''); }
      else if (item.kind === 'status') {
        await sendStatus(item.text, route); // status e informativo: best-effort, sem retry
      } else if (item.kind === 'coupon') {
        const id = 'cupom:' + item.cupom;
        if (sent.has(id)) log('  dup cupom', item.cupom);
        else if (await sendCoupon(item, route)) remember(id);
        else remove = keepForRetry(item, 'envio de cupom falhou');
      } else {
        const o = await resolve(item.url, route);
        if (!o) remove = keepForRetry(item, 'nao resolveu (bloqueio/link morto)');
        else if (!o.link) remove = keepForRetry(item, 'sem link de afiliado (cookie ML?)');
        else if (sent.has(o.productId)) log('  dup, ja enviado', o.productId);
        else {
          if (item.srcPrice) o.price = item.srcPrice; // preco da origem (ex.: PIX/US$) tem prioridade
          if (!o.title && item.srcTitle) o.title = item.srcTitle; // titulo da origem se a Amazon nao devolve og:title
          if (await sendOffer(o, item.cupom, route)) remember(o.productId);
          else remove = keepForRetry(item, 'envio da oferta falhou');
        }
      }
    } catch (e) { log('  erro processando', item.url || item.cupom || '', String(e).slice(0, 160)); remove = keepForRetry(item, 'excecao'); }

    if (remove) queue.shift(); // sucesso, duplicata ou desistencia: remove da frente (keepForRetry ja reposicionou se necessario)
    saveQueue();
    if (queue.length) await sleep(Math.max(6000, rand(CFG.MIN_DELAY, CFG.MAX_DELAY))); // >=6s respeita o limite de 5s do Wasender
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
  return { jid, fromMe, text, msgId: m?.key?.id || m?.id || '' };
}

function extractOffers(text) {
  const urls = [...new Set((text.match(/https?:\/\/[^\s)]+/gi) || []))];
  const offers = urls.filter(u => /amzn\.to|amazon\.|\.amazon\/|a\.co\/|mercadoliv|mercadolibre|meli\.la|\/sec\//i.test(u));
  // exige ":" (formato real "CUPOM: CODIGO"); fallback sem ":" ignora "CUPOM AMAZON/MERCADO" (titulos)
  const cupom = (text.match(/cupom\s*[:\-]\s*([A-Z0-9]{4,})/i)
              || text.match(/cupom\s+(?!amazon|amzn|mercado|prime)([A-Z0-9]{4,})/i)
              || [, ''])[1];
  const srcPrice = extractPricePhrase(text);
  return { offers, cupom, srcPrice, srcTitle: extractTitle(text) };
}

// nome do produto a partir do texto da oferta (fallback quando a Amazon nao devolve og:title)
function extractTitle(text) {
  const t = (text || '')
    .replace(/https?:\/\/[^\s]+/g, ' ')                      // remove URLs
    .replace(/R\$[\s\S]*$/i, ' ')                            // corta do preco em diante
    .replace(/EXCLUSIVO PARA ASSINANTES PRIME|IMPORTA[ÇC][AÃ]O AMAZON!*|IMPOSTOS J[ÁA] INCLU[ÍI]DOS!*|O DESCONTO APARECE NA TELA DE FINALIZA[ÇC][AÃ]O|NOVO CUPOM AMAZON!*|PAGANDO VIA PIX|VIA PIX|ACABA RAPID[O]+!*|CORRE!*|[ÚU]LTIMAS UNIDADES!*/gi, ' ')
    .replace(/TOP\b/g, ' ')
    .replace(/[^\p{L}\p{N}\s,.\-ºª°²³/()"+]/gu, ' ')          // tira emojis/simbolos, mantem pontuacao util
    .replace(/\s+/g, ' ')
    .replace(/^[\s\-•!.:,]+|[\s\-•!.:,]+$/g, '')
    .trim();
  return t.length > 3 ? t.slice(0, 200) : '';                // descricao ampliada (specs uteis)
}

// bloco de preco da origem completo (ex.: "R$1924,00 PAGANDO VIA PIX OU R$2.299,00 EM 10X") — preserva PIX + parcelado
function extractPricePhrase(text) {
  const m = (text || '').match(/R\$[\s\S]*?(?=\s*(?:👉|USAR\s*CUPOM|CUPOM\b|C[ÓO]DIGO|RESGATE|https?:\/\/|$))/i);
  return m ? m[0].replace(/\s+/g, ' ').trim().slice(0, 120) : '';
}

// regras do cupom (ex.: "10% OFF acima de R$300", "Limitado a R$100"), sem URLs/lixo da origem
function extractCouponRules(text) {
  return text.split('\n').map(l => l.trim())
    .filter(l => l && /%|R\$|acima|limit/i.test(l) && !/https?:|tabuga|resgate|usar\s+cupom/i.test(l))
    .slice(0, 3).join('\n');
}

function handle(body) {
  const { jid, fromMe, text, msgId } = parseMessage(body);
  log('  rx', 'evento=' + (body?.event || body?.type || '?'), 'jid=' + (jid || '-'), 'txt=' + text.slice(0, 45).replace(/\n/g, ' '));
  if (fromMe) return { skip: 'fromMe' };
  const route = routeFor(jid); // roteia pela ORIGEM: BR (Amazon+ML) ou US (so Amazon)
  if (!route) return { skip: `grupo nao mapeado (${jid})` };
  // o Wasender dispara ~3 eventos por mensagem (upsert + received + group.received) -> processa so 1x
  if (msgId && seenMsg.has(msgId)) return { skip: 'evento duplicado' };
  if (msgId) { seenMsg.add(msgId); if (seenMsg.size > 3000) seenMsg.clear(); }
  const { offers, cupom, srcPrice, srcTitle } = extractOffers(text);
  if (!offers.length) {
    // aviso de status de cupom (esgotado/expirou/acabou) -> reposta pro grupo saber
    if (/cupom/i.test(text) && /esgotad|acabou|acabaram|expirou|encerrad|finaliz|indispon|fora do ar|desativ|cupom\s+off/i.test(text)) {
      queue.push({ kind: 'status', text: cleanStatus(text), src: jid });
      saveQueue(); worker();
      return { enfileiradas: 1, tipo: 'status' };
    }
    if (CFG.COUPON_REPOST && cupom) {
      const resgate = (text.match(/https?:\/\/[^\s]+/i) || [])[0] || '';
      queue.push({ kind: 'coupon', cupom, rules: extractCouponRules(text), resgate, src: jid });
      saveQueue(); worker();
      return { enfileiradas: 1, tipo: 'cupom' };
    }
    return { skip: 'sem ofertas' };
  }
  // preco da origem so vale quando ha 1 oferta (1 preco = 1 produto); multi-oferta usa preco real por item
  const single = offers.length === 1;
  let price = single ? srcPrice : '';
  let title = single ? srcTitle : '';
  if (route.market === 'US') { // US manda em USD/ingles: pega "Now: $X" e deixa o titulo vir do og:title real da amazon.com
    const m = text.match(/now[:\s~]*\$?\s*([\d,]+\.\d{2})/i) || text.match(/\$\s*([\d,]+\.\d{2})/);
    price = single && m ? '$' + m[1] : '';
    title = '';
  }
  for (const url of offers) queue.push({ url, cupom, srcPrice: price, srcTitle: title, src: jid });
  saveQueue(); worker();
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
  // aceita QUALQUER POST como webhook (robusto se o Wasender for configurado so com o dominio, sem /webhook/captador)
  if (req.method === 'POST') {
    let b = ''; req.on('data', c => b += c);
    req.on('end', () => {
      let out, body;
      try { body = JSON.parse(b || '{}'); out = handle(body); } catch (e) { out = { erro: String(e) }; }
      try { const p = parseMessage(body || {}); if (p && p.jid) ringPush(rxLog, { jid: p.jid, market: (routeFor(p.jid) || {}).market || '?', txt: (p.text || '').slice(0, 120), out }); } catch {}
      log('webhook', req.url, JSON.stringify(out));
      res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: true, ...out }));
    });
  } else if ((req.url || '').startsWith('/status')) {
    const body = { up: true, uptime_s: Math.round((Date.now() - BOOT) / 1000), fila: queue.length, ja_enviados: sent.size, rotas: ROUTES.map(r => ({ market: r.market, source: r.source, target: r.target })), recebidas: rxLog.slice(-40).reverse(), enviadas: txLog.slice(-40).reverse() };
    res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body, null, 2));
  } else { log('req', req.method, req.url); res.writeHead(200); res.end('captador up'); }
}).listen(CFG.PORT, () => {
  log(`captador on :${CFG.PORT}  source=${CFG.SOURCE}  target=${CFG.TARGET}  tag=${CFG.AMZ_TAG}`);
  if (queue.length) { log(`retomando fila persistida: ${queue.length} item(ns)`); worker(); } // sobrevive a restart
});
