import asyncio
import logging
import math
import re
import time

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from telegraph import Telegraph
from telegraph.exceptions import RetryAfterError
from url_normalize import url_normalize
import os
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from keep_alive import keep_alive
import asyncpg
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlsplit, urlunsplit
import requests as req_http
import gc

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("ferrovie")

LANG_COOKIE = {"name": "lang", "value": "it_IT"}
HTTP_TIMEOUT = 30
TELEGRAPH_TIMEOUT = 15
HTTP_HEADERS = {
    "Accept-Language": "it-IT,it;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    )
}

ITALIAN_MONTHS = {
    "gennaio": "01",
    "febbraio": "02",
    "marzo": "03",
    "aprile": "04",
    "maggio": "05",
    "giugno": "06",
    "luglio": "07",
    "agosto": "08",
    "settembre": "09",
    "ottobre": "10",
    "novembre": "11",
    "dicembre": "12",
}


class TimeoutSession(req_http.Session):
    def __init__(self, timeout):
        super().__init__()
        self.default_timeout = timeout

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", self.default_timeout)
        return super().request(*args, **kwargs)


def create_http_session():
    session = req_http.Session()
    session.headers.update(HTTP_HEADERS)
    session.cookies.set(LANG_COOKIE["name"], LANG_COOKIE["value"])
    return session


def with_source_param(url, source="Bot"):
    parts = urlsplit(url)
    query_params = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "source"]
    query_params.append(("source", source))
    return urlunsplit(parts._replace(query=urlencode(query_params, doseq=True)))


def fetch_page_source_http(session, url):
    response = session.get(with_source_param(url), timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding or "utf-8"
    return response.text


def get_results_container(soup):
    return soup.find("div", {"class": "searchResultsBody"})


def get_result_cards(results_container):
    return results_container.find_all(
        "div",
        class_=lambda value: value and "singleResult" in value
    )


def get_description_node(soup):
    return (
            soup.find("div", {"itemprop": "description"}) or
            soup.find("div", {"class": "descriptionContainer"}) or
            soup.find("div", {"class": "locationList"})
    )


def is_offline_job_page(soup):
    search_titles = soup.find_all("div", {"class": "searchTitle"})
    for node in search_titles:
        text = node.get_text(" ", strip=True).lower()
        if "annuncio di lavoro" in text and "offline" in text:
            return True
        if "job ad" in text and "offline" in text:
            return True
    return False


def parse_jobs_list_html(main_html):
    soup = BeautifulSoup(main_html, "lxml")
    results_container = get_results_container(soup)
    if results_container is None:
        raise ValueError("Contenitore risultati non trovato nell'HTML della lista annunci")

    results = get_result_cards(results_container)
    jobs_data = []
    for result in results:
        try:
            details = result.find("div", {"class": "details"})
            jobUrl = url_normalize(DOMAIN + details.find("a")["href"])
            jobTitle = details.find("h3").text.strip()

            lista_posizione = [
                span.text.strip().title() for span in details.find_next(
                    string="Sede:").find_next("td").find_next("span").find_all("span")
                if span.text.strip()
            ]
            jobZone = ' , '.join(lista_posizione)
            jobSector = details.find_next(string="Settore:").find_next("span").text
            jobRole = details.find_next(string="Ruolo:").find_next("span").text
            jobDate = details.find("span", {"class": "date"}).text

            parsed_url = urlparse(jobUrl)
            params = parse_qs(parsed_url.query)
            jobID = int(params['id'][0])

            jobs_data.append({
                'id': jobID, 'url': jobUrl, 'title': jobTitle,
                'zone': jobZone, 'sector': jobSector, 'role': jobRole,
                'date': jobDate, 'description_html': None
            })
        except Exception as e:
            logger.warning("Errore parsing annuncio dalla lista: %s", e)
            continue

    return jobs_data


def is_valid_detail_html(detail_html):
    soup = BeautifulSoup(detail_html, "lxml")
    return get_description_node(soup) is not None and not is_offline_job_page(soup)


def extract_deadline_from_text(text):
    patterns = [
        r"Candidati\s+entro\s+(?:il\s+giorno\s+|il\s+)?(\d{1,2}/\d{1,2}/\d{4}|\d{1,2}\s+[A-Za-zÀ-ÿ]+\s+\d{4})",
        r"entro\s+(?:il\s+giorno\s+|il\s+)?(\d{1,2}/\d{1,2}/\d{4}|\d{1,2}\s+[A-Za-zÀ-ÿ]+\s+\d{4})"
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            raw_deadline = re.sub(r"\s+", " ", match.group(1).strip())
            numeric_match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw_deadline)
            if numeric_match:
                day, month, year = numeric_match.groups()
                return f"{int(day):02d}/{int(month):02d}/{year}"

            textual_match = re.fullmatch(r"(\d{1,2})\s+([A-Za-zÀ-ÿ]+)\s+(\d{4})", raw_deadline, flags=re.IGNORECASE)
            if textual_match:
                day, month_name, year = textual_match.groups()
                month_number = ITALIAN_MONTHS.get(month_name.lower())
                if month_number:
                    return f"{int(day):02d}/{month_number}/{year}"
    return None


def sanitize_description_html(description_node):
    allowed_tags = {"a", "b", "br", "em", "i", "li", "ol", "p", "strong", "u", "ul"}
    container = BeautifulSoup(str(description_node), "lxml")
    root = container.find(description_node.name)
    if root is None:
        return None, None

    for tag in root.find_all(True):
        if tag.name not in allowed_tags:
            tag.unwrap()
            continue
        if tag.name == "a":
            href = tag.get("href")
            tag.attrs = {"href": href} if href else {}
        else:
            tag.attrs = {}

    description_text = root.get_text(" ", strip=True).replace("\xa0", " ").strip()
    description_html = root.decode_contents().replace("\xa0", " ").strip()
    if not description_text or not description_html:
        return None, None

    return description_text, description_html


def extract_job_detail_data(detail_html):
    soup = BeautifulSoup(detail_html, "lxml")
    if is_offline_job_page(soup):
        return None
    description_node = get_description_node(soup)
    if description_node is None:
        return None

    description_text, description_html = sanitize_description_html(description_node)
    if not description_text or not description_html:
        return None

    page_text = soup.get_text(" ", strip=True).replace("\xa0", " ")
    return {
        "description_text": description_text,
        "description_html": description_html,
        "deadline": extract_deadline_from_text(page_text)
    }


telegraph = Telegraph()
telegraph._telegraph.session = TimeoutSession(TELEGRAPH_TIMEOUT)
try:
    telegraph.create_account("@ConcorsiFerrovie")
except Exception as e:
    logger.warning("Telegraph create_account fallito all'avvio: %s", e)

lock = asyncio.Lock()
db_pool: asyncpg.Pool = None

bot = Bot(token=os.environ['bot_token'], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher()
dp.include_router(router)

ADMIN_ID = 5239432590
CHAT_ID = int(os.environ['chat_id'])
DOMAIN = "https://fscareers.gruppofs.it/"
CHANNEL_SHARE_URL = (
    "https://telegram.me/share/url?url=https://telegram.me/concorsiferrovie"
    "&text=Unisciti%20per%20ricevere%20notifiche%20sulle%20nuove%20posizioni%20"
    "disponibili%20sul%20sito%20delle%20Ferrovie%20Dello%20Stato%20"
)


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        database=os.environ["db_name"],
        host=os.environ["db_host"],
        user=os.environ["db_user"],
        password=os.environ["db_password"],
        port=os.environ["db_port"],
        min_size=1,
        max_size=3,
        statement_cache_size=0
    )


async def notify_admin_error(context, error=None):
    details = f"{type(error).__name__}: {error}" if error is not None else "Nessun dettaglio aggiuntivo."
    try:
        await bot.send_message(chat_id=ADMIN_ID, text=f"Errore bot: {context}\n{details}")
    except Exception as exc:
        logger.warning("Errore invio notifica admin per %s: %s", context, exc)


def build_start_message_buttons():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Canale notifiche \u2757", url="t.me/concorsiferrovie"),
         InlineKeyboardButton(text="Gruppo discussione \U0001F5E3", url="t.me/selezioniconcorsiferrovie")],
        [InlineKeyboardButton(text="Ultime posizioni \U0001F4C5", callback_data="ultime")],
        [InlineKeyboardButton(text="Lista per settore \U0001F477\U0001F3FB\u200d\u2642\ufe0f",
                              callback_data="listasettore"),
         InlineKeyboardButton(text="Lista per regione \U0001F4CD", callback_data="listaregioni")],
        [InlineKeyboardButton(text="Cerca \U0001F50D", callback_data="ricerca")],
        [InlineKeyboardButton(text="Profilo \U0001F464", callback_data="profilo"),
         InlineKeyboardButton(text="Guadagna \U0001F4B0", url="https://t.me/concorsiferrovie/1430")],
        [InlineKeyboardButton(text="Assistenza \u2709", callback_data="assistenza")]
    ])


def build_updated_job_buttons(message_id):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Visualizza messaggio \U0001F517", url=f"t.me/concorsiferrovie/{message_id}"),
        InlineKeyboardButton(text="Guadagna \U0001F4B0", url="https://t.me/concorsiferrovie/1430")
    ]])


def build_channel_job_buttons(description_url, job_id, whatsapp_url=None, whatsapp_title=None):
    buttons = [[
        InlineKeyboardButton(text="Condividi il canale \u2757", url=CHANNEL_SHARE_URL),
        InlineKeyboardButton(text="Gruppo discussione \U0001F5E3", url="t.me/selezioniconcorsiferrovie")
    ], [
        InlineKeyboardButton(text="Descrizione \U0001F4C3", url=description_url)
    ], [
        InlineKeyboardButton(text="Guadagna \U0001F4B0", url="https://t.me/concorsiferrovie/1430"),
        InlineKeyboardButton(text="Aggiungi ai preferiti \U0001F3F7",
                             url=f"t.me/concorsiferroviebot?start=like_{job_id}")
    ]]

    if whatsapp_url:
        share_title = f"{whatsapp_title.replace(' ', '+')}+" if whatsapp_title else ""
        buttons.append([
            InlineKeyboardButton(
                text="Condividi su WhatsApp \U0001F4F1",
                url=url_normalize(
                    "https://api.whatsapp.com/send?text=Guarda+questo+annuncio+di+lavoro+delle+Ferrovie+Dello+Stato:+"
                    + share_title + whatsapp_url
                )
            )
        ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def format_job_date(value):
    return value.strftime("%d/%m/%Y") if hasattr(value, "strftime") else str(value)


def build_channel_message_text(job, deadline=None, header="\U0001F686 <b>Nuovo annuncio!</b>"):
    deadline_line = f"\n\u23F3 <i>Data di scadenza: {deadline}</i>" if deadline else ""
    publication_date = format_job_date(job["date"])
    return f"""{header}

\U0001F517 <a href='{job['url']}'>{job['title']}</a>

\U0001F4CD Sede: <b>{job['zone']}</b>
\U0001F4BC Settore: <b>{job['sector']}</b>
\U0001F4C4 Ruolo: <b>{job['role']}</b>

\U0001F4C5 <i>Data di pubblicazione: {publication_date}</i>{deadline_line}"""


def fetch_latest_job_snapshot():
    jobs_data, list_source = scrape_all_pages()
    if not jobs_data:
        raise ValueError("Nessun annuncio trovato sul sito")

    latest_job = jobs_data[0]
    detail_htmls, _ = scrape_detail_pages([latest_job["url"]])
    detail_html = detail_htmls.get(latest_job["url"])
    detail_data = extract_job_detail_data(detail_html) if detail_html else None

    return latest_job, detail_data, list_source


async def create_telegraph_page_url(title, html_content, retries=3):
    if not html_content or not html_content.strip():
        return None

    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = await asyncio.to_thread(
                telegraph.create_page,
                title,
                html_content=html_content
            )
            return response["url"]
        except RetryAfterError as exc:
            last_error = exc
            logger.warning(
                "Telegraph flood wait per '%s': retry_after=%ss",
                title,
                exc.retry_after
            )
            if attempt < retries and exc.retry_after <= 5:
                await asyncio.sleep(exc.retry_after)
            else:
                break
        except Exception as exc:
            last_error = exc
            logger.warning("Telegraph create_page fallito (%s/%s) per '%s': %s", attempt, retries, title, exc)
            if attempt < retries:
                await asyncio.sleep(min(attempt * 2, 5))

    logger.error("Telegraph non disponibile per '%s'. Ultimo errore: %s", title, last_error)
    return None


start_message = """Ciao! Questo \u00E8 un bot <b>non ufficiale</b> delle <b>Ferrovie Dello Stato</b> \U0001F686
<a href="https://telegra.ph/file/9d5be8ab56b1788848e60.jpg"> </a>
Unisciti al canale per rimanere aggiornato sulle posizioni presenti sul <a href="https://fscareers.gruppofs.it/jobs.php"><b>sito ufficiale</b></a> \u2757

<i>Usa i tasti per visualizzare le posizioni disponibili in base alle tue preferenze, o per cercarne una specifica</i> \U0001F441"""
start_message_buttons = build_start_message_buttons()


# Comando Start
@router.message(CommandStart(), F.chat.type == "private")
async def start_command(message: Message):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", message.chat.id)

        if user is None:
            await conn.execute("INSERT INTO users(id, date) VALUES ($1, $2)", message.chat.id, message.date)

        if message.text == "/start":
            await message.answer(text=start_message,
                                 reply_markup=start_message_buttons,
                                 disable_web_page_preview=False)
        else:
            request = (message.text.split()[1]).split("_")
            azione = request[0]

            bottoni_link = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='Profilo 👤', callback_data="profilo")],
                [InlineKeyboardButton(text='Indietro ↩', callback_data="menu")]
            ])

            job_id = int(request[1])
            job = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)

            if job is None:
                await message.answer(
                    text=f"""⚠ <b>Attenzione!</b>

A quanto pare, questo annuncio è stato <b>cancellato</b> dal nostro database.

Se l'annuncio è disponibile sul sito, ti invitiamo a contattare <b>l'assistenza</b>.""",
                    reply_markup=bottoni_link,
                    disable_web_page_preview=True)
            else:
                jobFavorite = await conn.fetchrow(
                    "SELECT * FROM favorites WHERE idUser = $1 AND idJob = $2",
                    message.chat.id, job_id)
                if azione == "like":
                    if jobFavorite is not None:
                        await message.answer(
                            text=f"""❓ <b>Annuncio già aggiunto!</b>

➤ <a href='{job['url']}'> {job['title']} </a> | 🔗

Digita il pulsante sottostante per accedere al tuo <b>Profilo 👤</b>""",
                            reply_markup=bottoni_link,
                            disable_web_page_preview=True)
                    else:
                        await conn.execute("INSERT INTO favorites(idUser, idJob) values ($1, $2)",
                                           message.chat.id, job_id)
                        await message.answer(
                            text=f"""✔ <b>Annuncio salvato correttamente!</b>

➤ <a href='{job['url']}'> {job['title']} </a> | 🔗

Potrai visualizzare questo e gli altri nel tuo <b>Profilo 👤</b>""",
                            reply_markup=bottoni_link,
                            disable_web_page_preview=True)

                if azione == "unlike":

                    if jobFavorite is not None:
                        await conn.execute("DELETE FROM favorites WHERE idUser = $1 AND idJob = $2",
                                           message.chat.id, job_id)
                        await message.answer(
                            text=f"""❌ <b>Annuncio rimosso!</b>

➤ <a href='{job['url']}'> {job['title']} </a> | 🔗

Questo annuncio non sarà più visualizzabile nel <b>Profilo 👤</b>""",
                            reply_markup=bottoni_link,
                            disable_web_page_preview=True)
                    else:
                        await message.answer(
                            text=f"""❓ <b>Annuncio non presente!</b>

➤ <a href='{job['url']}'> {job['title']} </a> | 🔗

Potrai visualizzare gli altri nel tuo <b>Profilo 👤</b> o aggiungerli dal canale <b> @concorsiferrovie </b>""",
                            reply_markup=bottoni_link,
                            disable_web_page_preview=True)


# Comando Help
@router.message(Command("help"), F.chat.type == "private")
async def help_command(message: Message):
    await message.answer(
        "Vuoi chiedere informazioni? Unisciti al gruppo <b>@selezioniconcorsiferrovie</b>  👥")


# Messaggio di test per l'admin
@router.message(Command("test"), F.chat.type == "private", F.from_user.id == ADMIN_ID)
async def test_command(message: Message):
    try:
        job, detail_data, list_source = await asyncio.to_thread(fetch_latest_job_snapshot)
    except Exception as exc:
        logger.exception("Errore nel test live admin: %s", exc)
        await message.answer("Errore nel recupero dell'ultimo annuncio dal sito.")
        return

    if detail_data is None:
        await message.answer(
            f"Ultimo annuncio trovato via {list_source}, ma la descrizione o la scadenza non sono risultate leggibili."
        )
        return

    telegraph_url = await create_telegraph_page_url(job['title'], detail_data['description_html'])
    if not telegraph_url:
        await message.answer("Ultimo annuncio trovato, ma la pagina Telegraph non \u00E8 stata generata.")
        return

    reply_markup = build_channel_job_buttons(telegraph_url, job['id'])
    text = build_channel_message_text(job, detail_data.get('deadline'),
                                      header="\U0001F9EA <b>Anteprima annuncio live</b>")

    await message.answer(
        text=text,
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )


# Invio file guida alle selezioni
@router.message(Command("selezioni"))
async def selezioni(message: Message):
    await bot.send_document(message.chat.id,
                            document="BQACAgEAAxkBAAOLY3kya8znQHFPlyaWUVh6Kza1rjUAAi0CAAKRc8lHK8mW1THwNpEeBA",
                            caption='Guida per l\'iter di selezione, redatta dal<b><a href="https://t.me/SelezioniConcorsiFerrovie"> gruppo telegram </a></b>👥')


# Invio file guida alla candidatura
@router.message(Command("candidatura"))
async def candidatura(message: Message):
    await bot.send_document(message.chat.id,
                            document="BQACAgEAAxkBAAOJY3kyWRgpX92LB3j1aYTXgihYunUAAucCAAKnP6FEAxxEXa5Ct5IeBA",
                            caption='Guida per l\'invio della candidatura, redatta dal<b><a href="https://t.me/SelezioniConcorsiFerrovie"> gruppo telegram </a></b>👥')


# Contatto dell'assistenza
@router.message(F.chat.type == "private", F.reply_to_message, F.text, ~(F.from_user.id == ADMIN_ID))
async def contatta(message: Message):
    await message.reply(
        "🚄 Ciao! Il tuo messaggio è stato inviato <b>correttamente</b>.\n\nRiceverai una risposta appena possibile.")

    sent = await message.forward(ADMIN_ID)
    await bot.send_message(chat_id=ADMIN_ID,
                           text=str(message.chat.id) + " ha inviato un messaggio ❗",
                           reply_to_message_id=sent.message_id)


# Risposta da parte del supporto
@router.message(F.chat.type == "private", F.from_user.id == ADMIN_ID, F.reply_to_message, F.text)
async def rispondi(message: Message):
    await bot.send_message(message.reply_to_message.text.split(" ")[0], message.text)


# CallBackQuery Bottoni
@router.callback_query()
async def callback_query_handler(callback: CallbackQuery):
    passaggio = callback.data.split("/", 1)

    async with db_pool.acquire() as conn:

        # Modalità di ricerca
        if callback.data == "ricerca":
            await callback.message.edit_text(
                text="""⌨ <b>Digita</b> qualsiasi parola chiave tu voglia, ad esempio:

<b>- Settore, Ruolo, Tipo di Contratto</b>
<b>- Città, Regione, Diploma o Laurea</b>
<b>- Data di Scadenza o Pubblicazione</b>""",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='Annulla ❌', callback_data="menu")]
                ]),
                disable_web_page_preview=False)

        # Gestisco la regione di preferenza
        if passaggio[0] == "aggiungi":
            existing = await conn.fetchrow("SELECT zone FROM zones WHERE zone = $1 AND idUser = $2",
                                           passaggio[-1], callback.from_user.id)

            if existing is not None:
                await conn.execute("DELETE FROM zones WHERE zone = $1 AND idUser = $2",
                                   passaggio[-1], callback.from_user.id)
            else:
                await conn.execute("INSERT INTO zones(idUser, zone) values($1, $2)",
                                   callback.from_user.id, passaggio[-1])

            passaggio[0] = "personalizza"
            passaggio[-1] = "2"

        # Gestisco il settore di preferenza
        if passaggio[0] == "aggsett":
            existing = await conn.fetchrow("SELECT type FROM sectors WHERE type = $1 AND idUser = $2",
                                           passaggio[-1], callback.from_user.id)

            if existing is not None:
                await conn.execute("DELETE FROM sectors WHERE type = $1 AND idUser = $2",
                                   passaggio[-1], callback.from_user.id)
            else:
                await conn.execute("INSERT INTO sectors(idUser, type) values($1, $2)",
                                   callback.from_user.id, passaggio[-1])

            passaggio[0] = "personalizza"
            passaggio[-1] = "3"

        # Gestisco il tipo di notifica
        if passaggio[0] == "aggtipo":
            existing = await conn.fetchrow("SELECT type FROM notifications WHERE type = $1 AND idUser = $2",
                                           passaggio[-1], callback.from_user.id)

            if existing is not None:
                await conn.execute("DELETE FROM notifications WHERE type = $1 AND idUser = $2",
                                   passaggio[-1], callback.from_user.id)
            else:
                await conn.execute("INSERT INTO notifications(idUser, type) values($1, $2)",
                                   callback.from_user.id, passaggio[-1])

            passaggio[0] = "personalizza"
            passaggio[-1] = "4"

        # Effettuo la verifica delle preferenze
        if passaggio[0] == "verifica":

            pulsanti = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='Indietro ◀', callback_data="personalizza/" + str(4))],
                [InlineKeyboardButton(text='Indietro ↩', callback_data="profilo")]
            ])

            new_check = await conn.fetchrow(
                "SELECT * FROM notifications WHERE idUser = $1 AND type = 'Nuovo'",
                callback.from_user.id)

            if new_check is None:
                await callback.message.edit_text(
                    text=
                    """❓ Per verificare il corretto funzionamento, devi spuntare l'opzione <b>Nuovo</b> nella pagina 4.

Clicca il pulsante sotto per tornare indietro.""",
                    reply_markup=pulsanti,
                    disable_web_page_preview=True)
            else:
                user_zones_rows = await conn.fetch("SELECT zone FROM zones WHERE idUser = $1",
                                                   callback.from_user.id)
                user_sectors_rows = await conn.fetch("SELECT type FROM sectors WHERE idUser = $1",
                                                     callback.from_user.id)

                user_zones = [r['zone'] for r in user_zones_rows]
                user_sectors = [r['type'] for r in user_sectors_rows]

                if user_zones and user_sectors:
                    zone_patterns = [f"%{z}%" for z in user_zones]
                    jobs_filtered = await conn.fetch(
                        "SELECT * FROM jobs WHERE zone ILIKE ANY($1) AND sector = ANY($2)",
                        zone_patterns, user_sectors)
                elif user_zones:
                    zone_patterns = [f"%{z}%" for z in user_zones]
                    jobs_filtered = await conn.fetch(
                        "SELECT * FROM jobs WHERE zone ILIKE ANY($1)", zone_patterns)
                elif user_sectors:
                    jobs_filtered = await conn.fetch(
                        "SELECT * FROM jobs WHERE sector = ANY($1)", user_sectors)
                else:
                    jobs_filtered = await conn.fetch("SELECT * FROM jobs")

                testo = ""
                if jobs_filtered:
                    for annunciobuono in jobs_filtered:
                        testo += f"➤ <a href='{annunciobuono['url']}'>{annunciobuono['title']}</a> | 🔗 \n"

                    await callback.message.edit_text(
                        text=f"""🚄 Annunci corrispondenti ai <b>filtri:</b>\n\n{testo}
I prossimi annunci verranno inviati direttamente in questa chat!""",
                        reply_markup=pulsanti,
                        disable_web_page_preview=True)

                else:
                    await callback.message.edit_text(
                        text="""😢 <b>Ci dispiace...</b>

Non ci sono annunci con i <b>filtri</b> applicati.""",
                        reply_markup=pulsanti,
                        disable_web_page_preview=True)

        # Personalizza le notifiche
        if callback.data == "personalizza" or passaggio[0] == "personalizza":
            testo = ""
            bottoni = []

            numeromassimo = 5
            if callback.data == "personalizza":
                paginattuale = 1
            else:
                paginattuale = int(passaggio[-1])

            if paginattuale == 1:
                bottoni = [
                    [InlineKeyboardButton(text='Avanti ▶', callback_data="personalizza/" + str(paginattuale + 1))],
                    [InlineKeyboardButton(text='Indietro ↩', callback_data="profilo")]]

                testo = f"""📣 Scegli quali filtri applicare agli annunci da <b>notificare!</b>

Pagina <b>{paginattuale}/5.</b>

Cliccando su <b>Avanti</b> potrai passare alle pagine successive, per selezionare <i>settori</i>, <i>regioni</i> e altri campi di tuo interesse!"""

            if numeromassimo > paginattuale > 1:
                bottoni = []

                if paginattuale == 2:
                    count_zones = await conn.fetchval("SELECT COUNT(*) FROM zones WHERE idUser = $1",
                                                      callback.from_user.id)
                    user_zones_rows = await conn.fetch("SELECT zone FROM zones WHERE idUser = $1",
                                                       callback.from_user.id)
                    user_zones = [r['zone'] for r in user_zones_rows]

                    testo = f"""Scegli la <b>regione</b> a cui sei interessato per gli annunci 📍

Pagina <b>{paginattuale}/5.</b>
Selezionati: <b>{count_zones}.</b>

Puoi sceglierne più di una e cambiarle in qualsiasi momento!"""
                    rigaregioni = []
                    regioni = ["Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna",
                               "Friuli Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche",
                               "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia",
                               "Toscana", "Trentino-Alto Adige", "Umbria", "Veneto", "Val d'Aosta"]

                    for regione in regioni:

                        if regione in user_zones:
                            regionepuls = InlineKeyboardButton(text="✅ " + regione,
                                                               callback_data="aggiungi/" + regione)
                        else:
                            regionepuls = InlineKeyboardButton(text="❌ " + regione,
                                                               callback_data="aggiungi/" + regione)
                        rigaregioni.append(regionepuls)

                        if len(rigaregioni) == 4:
                            bottoni.append(rigaregioni)
                            rigaregioni = []

                if paginattuale == 3:
                    count_sectors = await conn.fetchval("SELECT COUNT(*) FROM sectors WHERE idUser = $1",
                                                        callback.from_user.id)
                    user_sectors_rows = await conn.fetch("SELECT type FROM sectors WHERE idUser = $1",
                                                         callback.from_user.id)
                    user_sectors = [r['type'] for r in user_sectors_rows]

                    testo = f"""👷🏻‍♂️Scegli il settore!

Pagina <b>{paginattuale}/5.</b>
Selezionati: <b>{count_sectors}.</b>

Puoi sceglierne più di uno e cambiarli in qualsiasi momento!"""
                    settori = [
                        "Altro", "Trasporti e logistica", "Ingegneria",
                        "Edilizia/Ingegneria civile", "Informatica"
                    ]

                    for settore in settori:
                        if settore in user_sectors:
                            pulsante = [
                                InlineKeyboardButton(text="✅ " + settore,
                                                     callback_data="aggsett/" + settore + "")]
                        else:
                            pulsante = [
                                InlineKeyboardButton(text="❌ " + settore,
                                                     callback_data="aggsett/" + settore + "")]

                        bottoni.append(pulsante)

                if paginattuale == 4:
                    count_types = await conn.fetchval("SELECT COUNT(*) FROM notifications WHERE idUser = $1",
                                                      callback.from_user.id)
                    user_types_rows = await conn.fetch("SELECT type FROM notifications WHERE idUser = $1",
                                                       callback.from_user.id)
                    user_types = [r['type'] for r in user_types_rows]

                    testo = f"""🔔 Scegli la <b>tipologia</b> di notifiche!

Pagina <b>{paginattuale}/5.</b>
Selezionati: <b>{count_types}</b>.

Puoi sceglierne più di una e cambiarle in qualsiasi momento!"""

                    tipologie = ["Nuovo", "Scaduto", "Aggiornato"]

                    for tipo in tipologie:
                        if tipo in user_types:
                            pulsante = [InlineKeyboardButton(text='✅ ' + tipo,
                                                             callback_data="aggtipo/" + tipo + "")]
                        else:
                            pulsante = [InlineKeyboardButton(text='❌ ' + tipo,
                                                             callback_data="aggtipo/" + tipo + "")]

                        bottoni.append(pulsante)

                bottoni.append(
                    [InlineKeyboardButton(text='Indietro ◀',
                                          callback_data="personalizza/" + str(paginattuale - 1)),
                     InlineKeyboardButton(text='Avanti ▶',
                                          callback_data="personalizza/" + str(paginattuale + 1))])
                bottoni.append([InlineKeyboardButton(text='Indietro ↩', callback_data="profilo")])

            if paginattuale == numeromassimo:
                bottoni = [[InlineKeyboardButton(text='Verifica ❗', callback_data="verifica")],
                           [InlineKeyboardButton(text='Indietro ◀',
                                                 callback_data="personalizza/" + str(paginattuale - 1))],
                           [InlineKeyboardButton(text='Indietro ↩', callback_data="profilo")]]
                testo = f"""Verifica i filtri inseriti! 🚄

Pagina <code>{paginattuale}/5</code>.

Verranno inviati gli annunci che rispettano i tuoi filtri, prova!"""

            await callback.message.edit_text(text=testo,
                                             reply_markup=InlineKeyboardMarkup(inline_keyboard=bottoni),
                                             disable_web_page_preview=True)

        # Visualizzo il profilo dell'utente
        if callback.data == "profilo":
            count_users = await conn.fetchval("SELECT COUNT(*) FROM users")

            bottoni = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='Preferiti 🔗', callback_data="preferiti"),
                 InlineKeyboardButton(text='Personalizza 📣', callback_data="personalizza")],
                [InlineKeyboardButton(text='Indietro ↩', callback_data="menu")]
            ])

            await callback.message.edit_text(text=f"""Questo è il tuo <b>Profilo 👤</b>

Qui potrai decidere di quali annunci ricevere le <i>notifiche</i> e salvare quelli che più ti <i>interessano</i>!

👥 <b>Utenti</b> » <code>{count_users}</code> """,
                                             reply_markup=bottoni,
                                             disable_web_page_preview=True)

        # Visualizza gli annunci preferiti dell'utente
        if callback.data == "preferiti":

            favorites = await conn.fetch("SELECT * FROM favorites WHERE idUser = $1",
                                         callback.from_user.id)

            home = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='Indietro ↩', callback_data="menu")]
            ])

            if len(favorites) == 0:
                await callback.message.edit_text(
                    text="""🏷 <b>Questa</b> è la lista dei tuoi annunci <b>preferiti!</b>

A quanto pare non è stato salvato nessun annuncio.

Per <b>aggiungerne</b>, puoi digitare il pulsante sotto ogni annuncio presente nel canale <b>@concorsiferrovie 🚄</b>""",
                    reply_markup=home,
                    disable_web_page_preview=False)
            else:
                fav_ids = [f['idjob'] for f in favorites]
                fav_jobs = await conn.fetch("SELECT * FROM jobs WHERE id = ANY($1)", fav_ids)
                testo = "🏷 <b>Questa</b> è la lista dei tuoi annunci <b>preferiti!</b> \n\n"

                for favorite in fav_jobs:
                    completo = "<a href='" + favorite['url'] + "'>" + favorite['title'] + "</a>"

                    testo = testo + "➤ " + completo + \
                            " | <a href='t.me/concorsiferroviebot?start=unlike_" + str(
                        favorite['id']) + "'>🔗</a> \n"

                testo = testo + "\nPer <b>rimuovere</b> quelli a cui non sei più interessato, digita sull'emoji a destra 🔗."
                await callback.message.edit_text(text=testo,
                                                 reply_markup=home,
                                                 disable_web_page_preview=True)

        # Ultimi annunci presenti nel db
        if callback.data == "ultime":

            messaggio = "Queste sono le ultime <b>10</b> posizioni presenti sul <b><a href='https://fscareers.gruppofs.it/jobs.php'>sito</a></b>: \n\n"

            last_jobs = await conn.fetch("SELECT * FROM jobs ORDER BY idmessage DESC LIMIT 10")

            for i in last_jobs:
                messaggio = messaggio + f"➤ <a href='{i['url']}'> {i['title']} </a> |  {i['date']}\n"

            await callback.message.edit_text(
                text=messaggio,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='Indietro ↩', callback_data="menu")]
                ]),
                disable_web_page_preview=True)

        # Contatto per l'assistenza
        if callback.data == "assistenza":
            annulla = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='Annulla ❌', callback_data="menu")]
            ])
            await callback.message.edit_text(
                text="""👉🏻 <b>Rispondi</b> trascinando verso sinistra questo messaggio, poi <b>digita</b> il messaggio per contattare <b>l'assistenza del bot</b>.

💡 La <b>richiesta</b> deve essere inviata in un <b>unico messaggio</b>, altri messaggi <b>non saranno recapitati</b>.""",
                reply_markup=annulla,
                disable_web_page_preview=False)

        # Recupero le regioni in modo univoco dal db
        if callback.data == "listaregioni":
            jobs_zone = await conn.fetch("SELECT DISTINCT zone FROM jobs")
            counter_jobs = await conn.fetchval("SELECT COUNT(*) FROM jobs")

            regioni = []
            for job in jobs_zone:
                if ',' in job['zone']:
                    valoreffettivo = job['zone'].split(',')
                    if "Italia" in valoreffettivo[0]:
                        regioni.append(valoreffettivo[1])

            bottonilista = []
            pulsanti = []
            for regionelavoro in regioni:

                pulsanti.append(
                    InlineKeyboardButton(text=regionelavoro + " ➡ ",
                                         callback_data=f"query/zone/{regionelavoro}/1"))
                if len(pulsanti) == 2:
                    bottonilista.append(pulsanti)
                    pulsanti = []

            bottonilista.append(pulsanti)

            testoquery = (
                f"Ci sono <b>{len(regioni)}</b> regioni e <b>{counter_jobs}</b> annunci.\n\nSelezionane "
                f"una:")
            if len(bottonilista) > 0:
                bottonilista.append(
                    [InlineKeyboardButton(text='Italia 🇮🇹', callback_data="query/zone/Italia/1")])
            bottonilista.append([InlineKeyboardButton(text='Indietro ↩', callback_data="menu")])

            await callback.message.edit_text(testoquery,
                                             reply_markup=InlineKeyboardMarkup(inline_keyboard=bottonilista))

        # Recupero i settori in modo univoco dal db
        if callback.data == "listasettore":
            annunci = await conn.fetchval("SELECT COUNT(*) FROM jobs")
            settori_rows = await conn.fetch("SELECT DISTINCT sector FROM jobs")

            bottonilista = []

            for settore in settori_rows:
                settorelavoro = settore['sector']
                pulsante = [
                    InlineKeyboardButton(text=settorelavoro + " ➡ ",
                                         callback_data=f"query/sector/{settorelavoro}/1")]
                bottonilista.append(pulsante)

            listasett = len(bottonilista)

            testoquery = f"Ci sono <b>{listasett}</b> settori e <b>{annunci}</b> annunci.\n\nSelezionane uno:"

            bottonilista.append([InlineKeyboardButton(text='Indietro ↩', callback_data="menu")])

            await callback.message.edit_text(testoquery,
                                             reply_markup=InlineKeyboardMarkup(inline_keyboard=bottonilista))

        # Visualizzo annunci impaginati
        if "query/" in callback.data:
            bottoneinfo = []
            messaggio = ""

            if "/sector/" in callback.data:

                type_sector = callback.data.split("/")[2]
                page_number = int(callback.data.split("/")[3])
                bottoneinfo = []

                counter_sector = await conn.fetchval("SELECT COUNT(*) FROM jobs WHERE sector = $1",
                                                     type_sector)

                max_page = math.ceil(counter_sector / 10)

                if page_number == 1:
                    sector_jobs = await conn.fetch("SELECT * FROM jobs WHERE sector = $1 LIMIT 10",
                                                   type_sector)
                else:
                    sector_jobs = await conn.fetch(
                        "SELECT * FROM jobs WHERE sector = $1 OFFSET $2 LIMIT 10",
                        type_sector, (page_number - 1) * 10)

                messaggio = f""" Questi sono i risultati per <b>{type_sector}</b>:\n
Totale: <b>{counter_sector}</b>.
Pagina {page_number}/{max_page}\n\n"""

                for job in sector_jobs:
                    messaggio = messaggio + f"➤ <a href='{job['url']}'>{job['title']}</a> | 🔗\n"

                if page_number == max_page and page_number != 1:
                    bottoneinfo = [[InlineKeyboardButton(text='◀ Pagina precedente',
                                                         callback_data=f"query/sector/{type_sector}/{str(page_number - 1)}")]]

                if 1 < page_number < max_page:
                    bottoneinfo = [[InlineKeyboardButton(text='◀ Pagina precedente',
                                                         callback_data=f"query/sector/{type_sector}/{str(page_number - 1)}"),
                                    InlineKeyboardButton(text='Pagina successiva ▶',
                                                         callback_data=f"query/sector/{type_sector}/{str(page_number + 1)}")]]

                if page_number == 1 and page_number < max_page:
                    bottoneinfo = [[InlineKeyboardButton(text='Pagina successiva ▶',
                                                         callback_data=f"query/sector/{type_sector}/{str(page_number + 1)}")]]

                bottoneinfo.append([InlineKeyboardButton(text='Indietro ↩', callback_data="listasettore")])

            if "/zone/" in callback.data:
                zone = callback.data.split("/")[2]
                page_number = int(callback.data.split("/")[3])

                counter_zone = await conn.fetchval("SELECT COUNT(*) FROM jobs WHERE zone LIKE $1",
                                                   f"%{zone}%")

                max_page = math.ceil(counter_zone / 10)

                if page_number == 1:
                    zone_jobs = await conn.fetch("SELECT * FROM jobs WHERE zone LIKE $1 LIMIT 10",
                                                 f"%{zone}%")
                else:
                    zone_jobs = await conn.fetch(
                        "SELECT * FROM jobs WHERE zone LIKE $1 OFFSET $2 LIMIT 10",
                        f"%{zone}%", (page_number - 1) * 10)

                messaggio = f"""Questi sono i risultati per <b>{zone}</b>:\n
Totale: <b>{counter_zone}</b>.
Pagina {page_number}/{max_page}\n\n"""

                for job in zone_jobs:
                    messaggio = messaggio + f"➤ <a href='{job['url']}'>{job['title']}</a> | 🔗\n"

                if page_number == max_page and page_number != 1:
                    bottoneinfo = [[InlineKeyboardButton(text='◀ Pagina precedente',
                                                         callback_data=f"query/zone/{zone}/{str(page_number - 1)}")]]

                if 1 < page_number < max_page:
                    bottoneinfo = [[InlineKeyboardButton(text='◀ Pagina precedente',
                                                         callback_data=f"query/zone/{zone}/{str(page_number - 1)}"),
                                    InlineKeyboardButton(text='Pagina successiva ▶',
                                                         callback_data=f"query/zone/{zone}/{str(page_number + 1)}")]]

                if page_number == 1 and page_number < max_page:
                    bottoneinfo = [[InlineKeyboardButton(text='Pagina successiva ▶',
                                                         callback_data=f"query/zone/{zone}/{str(page_number + 1)}")]]

                bottoneinfo.append([InlineKeyboardButton(text='Indietro ↩', callback_data="listaregioni")])

            await callback.message.edit_text(messaggio,
                                             reply_markup=InlineKeyboardMarkup(inline_keyboard=bottoneinfo),
                                             disable_web_page_preview=True)

        # Menu principale
        if callback.data == "menu":
            await callback.message.edit_text(
                start_message,
                reply_markup=start_message_buttons,
                disable_web_page_preview=False)


# Ricerca dell'annuncio in base alla parola chiave
@router.message(F.chat.type == "private", F.text)
async def controlla(message: Message):
    messaggio = message.text

    async with db_pool.acquire() as conn:
        pattern = f"%{messaggio.lower()}%"
        jobs_key = await conn.fetch(
            "SELECT * FROM jobs WHERE LOWER(url) LIKE $1 OR LOWER(title) LIKE $1 OR "
            "LOWER(zone) LIKE $1 OR LOWER(role) LIKE $1 OR LOWER(sector) LIKE $1",
            pattern)

        text = f"""Risultati per: <b>{message.text}</b> \n\nTotale: <b>{len(jobs_key)}</b>\n\n"""

        for i in jobs_key:
            text = text + f"➤ <a href='{i['url']}'>{i['title']}</a> | 🔗 \n"

        if len(jobs_key) > 0:
            await message.answer(text=text,
                                 reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                     [InlineKeyboardButton(text='Indietro ↩',
                                                           callback_data="menu")]
                                 ]),
                                 disable_web_page_preview=True)

        else:
            await message.answer(
                text="Il <b>termine</b> digitato non corrisponde ad alcun annuncio...\n\nProva a digitarlo in modo diverso - \n<i>es: macchinista ⇔ macchinisti</i>.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text='Indietro ↩',
                                          callback_data="menu")]
                ]))

        await message.delete()


# Elimina messaggi default
@router.message(F.chat.type == "private")
async def elimina(message: Message):
    await message.delete()


# Scraping
def scrape_all_pages_http():
    session = create_http_session()
    try:
        main_html = fetch_page_source_http(session, url_normalize(DOMAIN + "jobs.php?language=it_IT"))
        jobs_data = parse_jobs_list_html(main_html)
        if jobs_data:
            top_job = jobs_data[0]
            logger.info(
                "HTTP_TOP_JOB_ID=%s HTTP_TOP_JOB_DATE=%s HTTP_TOP_JOB_TITLE=%s",
                top_job["id"],
                top_job["date"],
                top_job["title"]
            )
        return jobs_data, "http"
    finally:
        session.close()


def scrape_all_pages():
    return scrape_all_pages_http()


def scrape_detail_pages_http(job_urls):
    detail_htmls = {}
    if not job_urls:
        return detail_htmls, {
            "detail_source": "http",
            "requested": 0,
            "loaded": 0
        }

    session = create_http_session()
    try:
        for url in job_urls:
            for attempt in range(1, 3):
                try:
                    detail_html = fetch_page_source_http(session, url)
                    if not is_valid_detail_html(detail_html):
                        raise ValueError("HTML dettaglio senza contenuto descrittivo atteso")
                    detail_htmls[url] = detail_html
                    break
                except Exception as e:
                    logger.warning("Tentativo %s/2 HTTP fallito per il dettaglio %s: %s", attempt, url, e)
                    if attempt == 2:
                        detail_htmls[url] = None
    finally:
        session.close()

    loaded_count = sum(1 for html in detail_htmls.values() if html)
    return detail_htmls, {
        "detail_source": "http",
        "requested": len(job_urls),
        "loaded": loaded_count
    }


def scrape_detail_pages(job_urls):
    return scrape_detail_pages_http(job_urls)


def verify_missing_jobs_http(job_rows):
    verified_removed_ids = []
    kept_ids = []
    error_ids = []
    if not job_rows:
        return verified_removed_ids, {
            "checked": 0,
            "kept": 0,
            "errors": 0
        }

    session = create_http_session()
    try:
        for job in job_rows:
            try:
                detail_html = fetch_page_source_http(session, job["url"])
                if is_valid_detail_html(detail_html):
                    kept_ids.append(job["id"])
                else:
                    logger.info(
                        "CLEAN_DETAIL_INVALID JOB_ID=%s JOB_TITLE=%s",
                        job["id"],
                        job["title"]
                    )
                    verified_removed_ids.append(job["id"])
            except req_http.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in {404, 410}:
                    logger.info(
                        "CLEAN_DETAIL_GONE JOB_ID=%s HTTP_STATUS=%s JOB_TITLE=%s",
                        job["id"],
                        status_code,
                        job["title"]
                    )
                    verified_removed_ids.append(job["id"])
                else:
                    logger.warning(
                        "Clean verifica dettaglio fallita per job %s (%s): %s",
                        job["id"],
                        job["title"],
                        exc
                    )
                    error_ids.append(job["id"])
            except Exception as exc:
                logger.warning(
                    "Clean verifica dettaglio fallita per job %s (%s): %s",
                    job["id"],
                    job["title"],
                    exc
                )
                error_ids.append(job["id"])
    finally:
        session.close()

    return verified_removed_ids, {
        "checked": len(job_rows),
        "kept": len(kept_ids),
        "errors": len(error_ids)
    }


async def scraping():
    # Fase 1: scarica la lista job via HTTP.
    started_at = time.monotonic()
    list_source = "unknown"
    for attempt in range(1, 4):
        try:
            jobs_data, list_source = await asyncio.to_thread(scrape_all_pages)
            break
        except Exception as e:
            logger.warning("Tentativo %s/3 fallito per caricare la pagina principale: %s", attempt, e)
            if attempt < 3:
                await asyncio.sleep(min(attempt * 5, 15))
            else:
                logger.error("SCRAPE_STATUS=failed LIST_SOURCE=%s reason=list_fetch_failed", list_source)
                await notify_admin_error("scraping: pagina principale non caricata dopo 3 tentativi")
                return

    if not jobs_data:
        logger.warning(
            "LIST_SOURCE=%s LIST_COUNT=0. Verifica se il sito ha cambiato markup o se il fetch ha restituito una pagina inattesa.",
            list_source
        )

    async with db_pool.acquire() as conn:
        users_blocked = []
        updated_jobs = 0
        sent_new_jobs = 0
        detail_stats = {
            "detail_source": "not_needed",
            "requested": 0,
            "loaded": 0
        }

        # Fase 2: Controlla DB per trovare job nuovi vs aggiornati
        new_jobs = []
        for job in jobs_data:
            try:
                dateDB = datetime.strptime(job['date'], '%d/%m/%Y').date()
                jobDB = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job['id'])

                if jobDB is not None:
                    if jobDB['date'] != dateDB:
                        await conn.execute(
                            "UPDATE jobs SET date = $1, sector = $2, role = $3, zone = $4, title = $5 WHERE id = $6",
                            dateDB, job['sector'], job['role'], job['zone'], job['title'], job['id'])
                        updated_jobs += 1

                        aggiornobuttons = build_updated_job_buttons(jobDB['idmessage'])
                        try:
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"""\U0001F4E3 <b>Annuncio aggiornato!</b>

\U0001F517 <a href='{job['url']}'>{job['title']}</a>

\U0001F4C5 <i>Data aggiornata: {job['date']}</i>""",
                                reply_markup=aggiornobuttons,
                                reply_to_message_id=jobDB['idmessage'],
                                disable_web_page_preview=True
                            )
                        except Exception as e:
                            logger.warning("Errore invio messaggio aggiornamento per %s: %s", job['id'], e)
                else:
                    new_jobs.append(job)
            except Exception as e:
                logger.warning("Errore processando annuncio %s: %s", job.get('id', '?'), e)
                continue

        # Fase 3: solo per i job nuovi, carica i dettagli via HTTP.
        if new_jobs:
            new_urls = [j['url'] for j in new_jobs]
            detail_htmls, detail_stats = await asyncio.to_thread(scrape_detail_pages, new_urls)

            for job in new_jobs:
                try:
                    detail_html = detail_htmls.get(job['url'])
                    detail_data = extract_job_detail_data(detail_html) if detail_html else None
                    if detail_data is None:
                        logger.warning("Descrizione non disponibile per job %s, annuncio non pubblicato.", job['id'])
                        continue

                    dateDB = datetime.strptime(job['date'], '%d/%m/%Y').date()
                    telegraph_url = await create_telegraph_page_url(job['title'], detail_data['description_html'])
                    if not telegraph_url:
                        logger.warning("Telegraph non disponibile per job %s, annuncio saltato.", job['id'])
                        continue

                    channel_buttons = build_channel_job_buttons(telegraph_url, job['id']).inline_keyboard
                    channel_message_text = build_channel_message_text(job, detail_data.get('deadline'))
                    sent_message = await bot.send_message(
                        CHAT_ID,
                        channel_message_text,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=channel_buttons),
                        disable_web_page_preview=True
                    )

                    channel_buttons = build_channel_job_buttons(
                        telegraph_url, job['id'], sent_message.get_url(), job['title']
                    ).inline_keyboard
                    message_edited = await bot.edit_message_reply_markup(
                        chat_id=CHAT_ID,
                        message_id=sent_message.message_id,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=channel_buttons)
                    )

                    await conn.execute(
                        "INSERT INTO jobs(id, date, url, title, zone, role, sector, idmessage) values ($1, $2, $3, $4, $5, $6, $7, $8)",
                        job['id'], dateDB, job['url'], job['title'], job['zone'], job['role'], job['sector'],
                        sent_message.message_id)
                    sent_new_jobs += 1

                    users = await conn.fetch("SELECT idUser FROM notifications WHERE type = 'Nuovo'")
                    user_ids = [u['iduser'] for u in users]
                    sectors_by_user = {}
                    zones_by_user = {}
                    if user_ids:
                        user_sectors_rows = await conn.fetch(
                            "SELECT idUser, type FROM sectors WHERE idUser = ANY($1)",
                            user_ids
                        )
                        user_zones_rows = await conn.fetch(
                            "SELECT idUser, zone FROM zones WHERE idUser = ANY($1)",
                            user_ids
                        )
                        for row in user_sectors_rows:
                            sectors_by_user.setdefault(row['iduser'], []).append(row['type'])
                        for row in user_zones_rows:
                            zones_by_user.setdefault(row['iduser'], []).append(row['zone'])

                    # Notifiche in parallelo
                    async def notify_user(user_id, _job=job, _msg=message_edited):
                        user_sectors = sectors_by_user.get(user_id, [])
                        user_zones = zones_by_user.get(user_id, [])

                        send = False
                        if user_zones and user_sectors:
                            if _job['sector'] in user_sectors and any(
                                    z in _job['zone'] for z in user_zones):
                                send = True
                        elif user_zones:
                            if any(z in _job['zone'] for z in user_zones):
                                send = True
                        elif user_sectors:
                            if _job['sector'] in user_sectors:
                                send = True
                        else:
                            send = True
                        if send:
                            try:
                                await bot.forward_message(chat_id=user_id,
                                                          from_chat_id=CHAT_ID,
                                                          message_id=_msg.message_id)
                            except Exception:
                                users_blocked.append(user_id)

                    # Invio notifiche in batch paralleli da 10
                    for i in range(0, len(user_ids), 10):
                        batch = user_ids[i:i + 10]
                        await asyncio.gather(*[notify_user(uid) for uid in batch])

                except Exception as e:
                    logger.exception("Errore processando nuovo annuncio %s", job.get('id', '?'))
                    continue

        gc.collect()
        if len(users_blocked) > 0:
            await conn.execute("DELETE FROM favorites WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM sectors WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM zones WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM notifications WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM users WHERE id = ANY($1)", users_blocked)
    logger.info(
        "SCRAPE_STATUS=ok LIST_SOURCE=%s DETAIL_SOURCE=%s LIST_COUNT=%s NEW_FOUND=%s NEW_SENT=%s UPDATED=%s DETAIL_OK=%s/%s DURATION=%.2fs",
        list_source,
        detail_stats["detail_source"],
        len(jobs_data),
        len(new_jobs),
        sent_new_jobs,
        updated_jobs,
        detail_stats["loaded"],
        detail_stats["requested"],
        time.monotonic() - started_at
    )


# Pulizia Annunci Scaduti
async def clean():
    started_at = time.monotonic()
    try:
        jobs_data, list_source = await asyncio.to_thread(scrape_all_pages)
    except Exception as e:
        logger.warning("Clean saltato: impossibile caricare la lista annunci: %s", e)
        return

    if not jobs_data:
        logger.warning("Clean saltato: lista annunci vuota o non parsabile.")
        return

    active_job_ids = {job["id"] for job in jobs_data}

    verify_stats = {
        "checked": 0,
        "kept": 0,
        "errors": 0
    }
    async with db_pool.acquire() as conn:
        jobs = await conn.fetch("SELECT id, url, title FROM jobs")
        missing_jobs = [
            {"id": job["id"], "url": job["url"], "title": job["title"]}
            for job in jobs
            if job["id"] not in active_job_ids
        ]
        delete_list, verify_stats = await asyncio.to_thread(verify_missing_jobs_http, missing_jobs)

        if delete_list:
            await conn.execute("DELETE FROM favorites WHERE idJob = ANY($1)", delete_list)
            await conn.execute("DELETE FROM jobs WHERE id = ANY($1)", delete_list)

    logger.info(
        "CLEAN_STATUS=ok LIST_SOURCE=%s LIST_COUNT=%s VERIFY_CHECKED=%s REMOVED=%s KEPT=%s VERIFY_ERRORS=%s DURATION=%.2fs",
        list_source,
        len(jobs_data),
        verify_stats["checked"],
        len(delete_list),
        verify_stats["kept"],
        verify_stats["errors"],
        time.monotonic() - started_at
    )


async def safe_clean():
    try:
        async with lock:
            await clean()
    except Exception as e:
        logger.exception("Errore in safe_clean")
        await notify_admin_error("safe_clean", e)


async def safe_scraping():
    try:
        async with lock:
            await scraping()
    except Exception as e:
        logger.exception("Errore in safe_scraping")
        await notify_admin_error("safe_scraping", e)


async def main():
    await init_db()

    scheduler = AsyncIOScheduler(timezone="Europe/Rome")
    scheduler.add_job(safe_clean, "cron", hour=1, misfire_grace_time=300)
    scheduler.add_job(safe_scraping, "interval", minutes=1, next_run_time=datetime.now() + timedelta(seconds=10))
    scheduler.start()

    keep_alive()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
