import asyncio
import math

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from telegraph import Telegraph
from url_normalize import url_normalize
import os
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from keep_alive import keep_alive
import asyncpg
from urllib.parse import urlparse, parse_qs
import requests as req_http
import gc

load_dotenv()

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-plugins")
chrome_options.add_argument("--disable-background-networking")
chrome_options.add_argument("--disable-sync")
chrome_options.add_argument("--no-first-run")
chrome_options.add_argument("--disable-default-apps")
chrome_options.add_argument("--blink-settings=imagesEnabled=false")
chrome_options.add_argument("--window-size=1280,720")
chrome_options.add_argument("--single-process")
chrome_options.add_argument("--disable-renderer-backgrounding")
chrome_options.add_argument("--disable-software-rasterizer")
chrome_options.add_argument("--js-flags=--max-old-space-size=256")
chrome_options.page_load_strategy = 'eager'

telegraph = Telegraph()
try:
    telegraph.create_account("@ConcorsiFerrovie")
except Exception as e:
    print(f"Telegraph create_account failed at startup: {e}")

lock = asyncio.Lock()
db_pool: asyncpg.Pool = None

bot = Bot(token=os.environ['bot_token'], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
router = Router()
dp = Dispatcher()
dp.include_router(router)

ADMIN_ID = 5239432590
CHAT_ID = int(os.environ['chat_id'])
DOMAIN = "https://fscareers.gruppofs.it/"


async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        database=os.environ["db_name"],
        host=os.environ["db_host"],
        user=os.environ["db_user"],
        password=os.environ["db_password"],
        port=os.environ["db_port"],
        min_size=2,
        max_size=10,
        statement_cache_size=0
    )


async def create_telegraph_page_url(title, description, retries=3):
    html_content = f'<p>{description or "Descrizione non disponibile."}</p>'
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = await asyncio.to_thread(
                telegraph.create_page,
                title,
                html_content=html_content
            )
            return response["url"]
        except Exception as exc:
            last_error = exc
            print(f"Telegraph create_page failed ({attempt}/{retries}) for '{title}': {exc}")
            if attempt < retries:
                await asyncio.sleep(min(attempt * 2, 5))

    print(f"Telegraph unavailable for '{title}'. Last error: {last_error}")
    return None


start_message = """Ciao! Questo è un bot <b>non ufficiale</b> delle <b>Ferrovie Dello Stato</b> 🚄
<a href="https://telegra.ph/file/9d5be8ab56b1788848e60.jpg"> </a>
Unisciti al canale per rimanere aggiornato sulle posizioni presenti sul <a href="https://fscareers.gruppofs.it/jobs.php"><b>sito ufficiale</b></a> ❗

<i>Usa i tasti per visualizzare le posizioni disponibili in base alle tue preferenze, o per cercarne una specifica</i> 👁"""
start_message_buttons = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='Canale notifiche ❗', url="t.me/concorsiferrovie"),
     InlineKeyboardButton(text='Gruppo discussione 🗣  ', url="t.me/selezioniconcorsiferrovie")],
    [InlineKeyboardButton(text='Ultime posizioni 📅 ', callback_data="ultime")],
    [InlineKeyboardButton(text='Lista per settore 👷🏻‍♂️ ', callback_data="listasettore"),
     InlineKeyboardButton(text='Lista per regione 📍 ', callback_data="listaregioni")],
    [InlineKeyboardButton(text='Cerca 🔍 ', callback_data="ricerca")],
    [InlineKeyboardButton(text='Profilo 👤', callback_data="profilo"),
     InlineKeyboardButton(text='Guadagna 💰', url="https://t.me/concorsiferrovie/1430")],
    [InlineKeyboardButton(text='Assistenza ✉', callback_data="assistenza")]
])


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
def scrape_all_pages():
    """Fase 1: Chrome scarica tutto l'HTML, poi viene chiuso. Niente async qui."""
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    driver.set_script_timeout(30)
    try:
        # Carica pagina lista
        driver.get(DOMAIN + "jobs.php")
        driver.implicitly_wait(10)
        driver.delete_cookie('lang')
        driver.add_cookie({
            'name': 'lang',
            'value': 'it_IT',
            'domain': '.fscareers.gruppofs.it',
            'path': '/',
            'secure': True,
            'httpOnly': True,
            'sameSite': 'None'
        })
        driver.refresh()
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "searchResultsBody")))
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "singleResult")))
        main_html = driver.page_source

        # Estrai URL dei job dalla lista
        soup = BeautifulSoup(main_html, "lxml")
        results = soup.find("div", {"class": "searchResultsBody"}).find_all(
            "div", {"class": "singleResult responsiveOnly"})

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
                print(f"Errore parsing annuncio dalla lista: {e}")
                continue

        return jobs_data
    finally:
        driver.quit()


def scrape_detail_pages(job_urls):
    """Fase 1b: Apre Chrome solo per le pagine dettaglio dei job NUOVI."""
    detail_htmls = {}
    if not job_urls:
        return detail_htmls

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    driver.set_script_timeout(30)
    try:
        for url in job_urls:
            try:
                driver.get(url)
                driver.implicitly_wait(10)
                driver.add_cookie({'name': 'lang', 'value': 'it_IT'})
                driver.refresh()
                detail_htmls[url] = driver.page_source
            except Exception as e:
                print(f"Errore caricamento dettaglio {url}: {e}")
                detail_htmls[url] = None
    finally:
        driver.quit()

    return detail_htmls


async def scraping():
    # Fase 1: Scarica lista job (Chrome apre e chiude)
    for attempt in range(1, 4):
        try:
            jobs_data = await asyncio.to_thread(scrape_all_pages)
            break
        except Exception as e:
            print(f"Tentativo {attempt}/3 fallito per caricare la pagina principale: {e}")
            if attempt < 3:
                await asyncio.sleep(min(attempt * 5, 15))
            else:
                print("Pagina principale non caricata dopo 3 tentativi. Scraping annullato.")
                return

    # Chrome è già chiuso qui - memoria libera

    async with db_pool.acquire() as conn:
        users_blocked = []

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

                        aggiornobuttons = InlineKeyboardMarkup(inline_keyboard=[[
                            InlineKeyboardButton(text='Visualizza messaggio 🔗',
                                                 url=f"t.me/concorsiferrovie/{jobDB['idmessage']}"),
                            InlineKeyboardButton(text='Guadagna 💰',
                                                 url="https://t.me/concorsiferrovie/1430")
                        ]])

                        try:
                            await bot.send_message(chat_id=CHAT_ID,
                                                   text=f"""📣 <b>Annuncio aggiornato!</b>

🔗 <a href='{job['url']}'>{job['title']}</a>

📅 <i>Data aggiornata: {job['date']}</i>""",
                                                   reply_markup=aggiornobuttons,
                                                   reply_to_message_id=jobDB['idmessage'],
                                                   disable_web_page_preview=True)
                        except Exception as e:
                            print(f"Errore invio messaggio aggiornamento per {job['id']}: {e}")
                else:
                    new_jobs.append(job)
            except Exception as e:
                print(f"Errore processando annuncio {job.get('id', '?')}: {e}")
                continue

        # Fase 3: Solo per i job NUOVI, apri Chrome per le descrizioni
        if new_jobs:
            new_urls = [j['url'] for j in new_jobs]
            detail_htmls = await asyncio.to_thread(scrape_detail_pages, new_urls)

            # Chrome è di nuovo chiuso qui

            for job in new_jobs:
                try:
                    detail_html = detail_htmls.get(job['url'])
                    jobDescription = ""
                    if detail_html:
                        soupannuncio = BeautifulSoup(detail_html, 'lxml')
                        description = (
                            soupannuncio.find("div", {"itemprop": "description"}) or
                            soupannuncio.find("div", {"class": "descriptionContainer"}) or
                            soupannuncio.find("div", {"class": "locationList"})
                        )
                        jobDescription = description.text.strip() if description else ""
                        del soupannuncio, description

                    dateDB = datetime.strptime(job['date'], '%d/%m/%Y').date()

                    telegraph_url = await create_telegraph_page_url(job['title'], jobDescription)
                    if not telegraph_url:
                        print(f"Skipping job {job['id']} because Telegraph page creation failed.")
                        continue
                    response = {"url": telegraph_url}

                    annunciobuttons = [[
                        InlineKeyboardButton(
                            text='Condividi il canale ❗',
                            url=
                            "https://telegram.me/share/url?url=https://telegram.me/concorsiferrovie&text=Unisciti%20per%20ricevere%20notifiche%20sulle%20nuove%20posizioni%20disponibili%20sul%20sito%20delle%20Ferrovie%20Dello%20Stato%20"
                        ),
                        InlineKeyboardButton(text='Gruppo discussione 🗣',
                                             url="t.me/selezioniconcorsiferrovie")
                    ], [InlineKeyboardButton(text='Descrizione 📃', url=f"{response['url']}")],
                        [InlineKeyboardButton(text='Guadagna 💰',
                                              url="https://t.me/concorsiferrovie/1430"),

                         InlineKeyboardButton(
                             text='Aggiungi ai preferiti 🏷',
                             url="t.me/concorsiferroviebot?start=like_" + str(job['id']) + "")
                         ]]

                    sentMessage = await bot.send_message(CHAT_ID,
                                                         f"""🚄 <b>Nuovo annuncio!</b>

🔗 <a href='{job['url']}'>{job['title']}</a>

📍 Sede: <b>{job['zone']}</b>
💼 Settore: <b>{job['sector']}</b>
📄 Ruolo: <b>{job['role']}</b>

📅 <i>Data di pubblicazione: {job['date']}</i>""",
                                                         reply_markup=InlineKeyboardMarkup(
                                                             inline_keyboard=annunciobuttons),
                                                         disable_web_page_preview=True)

                    annunciobuttons.append([
                        InlineKeyboardButton(text="Condividi su WhatsApp 📱", url=url_normalize(
                            "https://api.whatsapp.com/send?text=Guarda+questo+annuncio+di+lavoro+delle+Ferrovie+Dello+Stato:+"
                            + job['title'].replace(' ', '+') + "+" + sentMessage.get_url()))
                    ])

                    message_edited = await bot.edit_message_reply_markup(chat_id=CHAT_ID,
                                                                         message_id=sentMessage.message_id,
                                                                         reply_markup=InlineKeyboardMarkup(
                                                                             inline_keyboard=annunciobuttons))

                    await conn.execute(
                        "INSERT INTO jobs(id, date, url, title, zone, role, sector, idmessage) values ($1, $2, $3, $4, $5, "
                        "$6, $7, $8)",
                        job['id'], dateDB, job['url'], job['title'], job['zone'], job['role'], job['sector'],
                        sentMessage.message_id)

                    users = await conn.fetch("SELECT idUser FROM notifications WHERE type = 'Nuovo'")

                    # Notifiche in parallelo
                    async def notify_user(user_id, _job=job, _msg=message_edited):
                        user_sectors_rows = await conn.fetch(
                            "SELECT type FROM sectors WHERE idUser = $1", user_id)
                        user_zones_rows = await conn.fetch(
                            "SELECT zone FROM zones WHERE idUser = $1", user_id)
                        user_sectors = [s['type'] for s in user_sectors_rows]
                        user_zones = [z['zone'] for z in user_zones_rows]

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
                    user_ids = [u['iduser'] for u in users]
                    for i in range(0, len(user_ids), 10):
                        batch = user_ids[i:i + 10]
                        await asyncio.gather(*[notify_user(uid) for uid in batch])

                except Exception as e:
                    print(f"Errore processando nuovo annuncio {job.get('id', '?')}: {e}")
                    continue

        gc.collect()
        if len(users_blocked) > 0:
            await conn.execute("DELETE FROM favorites WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM sectors WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM zones WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM notifications WHERE idUser = ANY($1)", users_blocked)
            await conn.execute("DELETE FROM users WHERE id = ANY($1)", users_blocked)
    try:
        await bot.send_message(chat_id=ADMIN_ID, text="Finito scrape")
    except Exception as e:
        print(f"Errore invio notifica fine scrape: {e}")


# Pulizia Annunci Scaduti
async def clean():
    async with db_pool.acquire() as conn:
        jobs = await conn.fetch("SELECT * FROM jobs")

        delete_list = []
        session = req_http.Session()
        session.headers.update({'Accept-Language': 'it-IT,it;q=0.9'})

        for job in jobs:
            try:
                resp = await asyncio.to_thread(session.get, job['url'], timeout=15)
                soup = BeautifulSoup(resp.text, "lxml")
                if soup.find("div", {"class": "searchTitle"}) is not None:
                    delete_list.append(job['id'])
            except Exception:
                pass

        if len(delete_list) > 0:
            await conn.execute("DELETE FROM favorites WHERE idJob = ANY($1)", delete_list)
            await conn.execute("DELETE FROM jobs WHERE id = ANY($1)", delete_list)

    try:
        await bot.send_message(chat_id=ADMIN_ID, text="Finito clean")
    except Exception as e:
        print(f"Errore invio notifica fine clean: {e}")


async def safe_clean():
    try:
        async with lock:
            await clean()
    except Exception as e:
        print(f"Errore in safe_clean: {e}")


async def safe_scraping():
    try:
        async with lock:
            await scraping()
    except Exception as e:
        print(f"Errore in safe_scraping: {e}")


async def main():
    await init_db()

    scheduler = AsyncIOScheduler(timezone="Europe/Rome")
    scheduler.add_job(safe_clean, "cron", hour=1, misfire_grace_time=300)
    scheduler.add_job(safe_scraping, "interval", minutes=10, next_run_time=datetime.now() + timedelta(seconds=10))
    scheduler.start()

    keep_alive()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
