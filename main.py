import math

from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from telegraph import Telegraph
from url_normalize import url_normalize
import tgcrypto
import os
from dotenv import load_dotenv
import ssl
import certifi
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from keep_alive import keep_alive
import psycopg2
from urllib.parse import urlparse, parse_qs

ssl_context = ssl.create_default_context(cafile=certifi.where())

scheduler = AsyncIOScheduler(timezone="Europe/Rome")

options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

telegraph = Telegraph()

load_dotenv()

conn = psycopg2.connect(database=os.environ["db_name"],
                        host=os.environ["db_host"],
                        user=os.environ["db_user"],
                        password=os.environ["db_password"],
                        port="5432")

app = Client("ferrovie_bot",
             api_id=os.environ['api_id'],
             api_hash=os.environ['api_hash'],
             bot_token=os.environ['bot_token'])

CHAT_ID = int(os.environ['chat_id'])
DOMAIN = "https://fscareers.gruppofs.it/"

start_message = """Ciao! Questo è un bot <b>non ufficiale</b> delle <b>Ferrovie Dello Stato</b> 🚄
<a href= https://telegra.ph/file/9d5be8ab56b1788848e60.jpg> </a>
Unisciti al canale per rimanere aggiornato sulle posizioni presenti sul <a href=https://fscareers.gruppofs.it/jobs.php><b>sito ufficiale</b></a> ❗

__Usa i tasti per visualizzare le posizioni disponibili in base alle tue preferenze, o per cercarne una specifica__ 👁"""
start_message_buttons = [
    [InlineKeyboardButton('Canale notifiche ❗', url="t.me/concorsiferrovie"),
     InlineKeyboardButton('Gruppo discussione 🗣  ', url="t.me/selezioniconcorsiferrovie")],
    [InlineKeyboardButton('Ultime posizioni 📅 ', callback_data="ultime")],
    [InlineKeyboardButton('Lista per settore 👷🏻‍♂️ ', callback_data="listasettore"),
     InlineKeyboardButton('Lista per regione 📍 ', callback_data="listaregioni")],
    [InlineKeyboardButton('Cerca 🔍 ', callback_data="ricerca")],
    [InlineKeyboardButton('Profilo 👤', callback_data="profilo"),
     InlineKeyboardButton('Guadagna 💰', url="https://t.me/concorsiferrovie/1430")],
    [InlineKeyboardButton('Assistenza ✉', callback_data="assistenza")]
]


# Comando Start
@app.on_message(filters.command('start') & filters.private)
async def start_command(app, message):
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = %s", (message.chat.id,))

    if cur.fetchone() is None:
        cur.execute("INSERT INTO users(id, date) VALUES (%s, %s)", (message.chat.id, message.date))

    markup = InlineKeyboardMarkup(start_message_buttons)

    if message.text == "/start":
        await message.reply(text=start_message,
                            reply_markup=markup,
                            disable_web_page_preview=False)
    else:
        request = (message.text.split()[1]).split("_")
        azione = request[0]

        bottoni_link = [[InlineKeyboardButton('Profilo 👤', callback_data="profilo")],
                        [InlineKeyboardButton('Indietro ↩', callback_data="menu")]]

        cur.execute("SELECT * FROM jobs WHERE id = %s", (str(request[1]),))
        job = cur.fetchone()

        if job is None:
            await message.reply(
                text=f"""⚠ <b>Attenzione!</b>

A quanto pare, questo annuncio è stato <b>cancellato</b> dal nostro database.

Se l'annuncio è disponibile sul sito, ti invitiamo a contattare <b>l'assistenza</b>.""",
                reply_markup=InlineKeyboardMarkup(bottoni_link),
                disable_web_page_preview=True)
        else:
            cur.execute("SELECT * FROM favorites WHERE idUser = %s AND idJob = %s", (message.chat.id, request[1]))
            jobFavorite = cur.fetchone()
            if azione == "like":
                if jobFavorite is not None:
                    await message.reply(
                        text=f"""❓ <b>Annuncio già aggiunto!</b>
    
➤ <a href='{job[2]}'> {job[3]} </a> | 🔗
    
Digita il pulsante sottostante per accedere al tuo <b>Profilo 👤</b>""",
                        reply_markup=InlineKeyboardMarkup(bottoni_link),
                        disable_web_page_preview=True)
                else:
                    cur.execute("INSERT INTO favorites(idUser, idJob) values (%s, %s)", (message.chat.id, request[1]))
                    await message.reply(
                        text=f"""✔ <b>Annuncio salvato correttamente!</b>
    
➤ <a href='{job[2]}'> {job[3]} </a> | 🔗
    
Potrai visualizzare questo e gli altri nel tuo <b>Profilo 👤</b>""",
                        reply_markup=InlineKeyboardMarkup(bottoni_link),
                        disable_web_page_preview=True)

            if azione == "unlike":

                if jobFavorite is not None:
                    cur.execute("DELETE FROM favorites WHERE idUser = %s AND idJob = %s", (message.chat.id, request[1]))
                    await message.reply(
                        text=f"""❌ <b>Annuncio rimosso!</b>
    
➤ <a href='{job[2]}'> {job[3]} </a> | 🔗
    
Questo annuncio non sarà più visualizzabile nel <b>Profilo 👤</b>""",
                        reply_markup=InlineKeyboardMarkup(bottoni_link),
                        disable_web_page_preview=True)
                else:
                    await message.reply(
                        text=f"""❓ <b>Annuncio non presente!</b>
    
➤ <a href='{job[2]}'> {job[3]} </a> | 🔗
    
Potrai visualizzare gli altri nel tuo <b>Profilo 👤</b> o aggiungerli dal canale <b> @concorsiferrovie </b>""",
                        reply_markup=InlineKeyboardMarkup(bottoni_link),
                        disable_web_page_preview=True)

    conn.commit()


# Comando Help
@app.on_message(filters.command('help') & filters.private)
async def help_command(app, message):
    await app.send_message(message.chat.id,
                           "Vuoi chiedere informazioni? Unisciti al gruppo <b>@selezioniconcorsiferrovie</b>  👥")


# Invio file guida alle selezioni
@app.on_message(filters.command('selezioni'))
async def selezioni(app, message):
    await app.send_document(message.chat.id,
                            document="BQACAgEAAxkBAAOLY3kya8znQHFPlyaWUVh6Kza1rjUAAi0CAAKRc8lHK8mW1THwNpEeBA",
                            caption="Guida per l'iter di selezione, redatta dal<b><a href=t.me/SelezioniConcorsiFerrovie> gruppo telegram </a></b>👥")


# Invio file guida alla candidatura
@app.on_message(filters.command('candidatura'))
async def candidatura(app, message):
    await app.send_document(message.chat.id,
                            document="BQACAgEAAxkBAAOJY3kyWRgpX92LB3j1aYTXgihYunUAAucCAAKnP6FEAxxEXa5Ct5IeBA",
                            caption="Guida per l'invio della candidatura, redatta dal<b><a href=t.me/SelezioniConcorsiFerrovie> gruppo telegram </a></b>👥")


# Contatto dell'assistenzza
@app.on_message(filters.text & filters.private & filters.reply & ~filters.user(5453376840))
async def contatta(app, message):
    await message.reply_text(
        "🚄 Ciao! Il tuo messaggio è stato inviato <b>correttamente</b>.\n\nRiceverai una risposta appena possibile.")

    sent = await message.forward(5453376840)
    await app.send_message(chat_id=5453376840,
                           text=str(message.chat.id) + " ha inviato un messaggio ❗",
                           reply_to_message_id=sent.id)


# Risposta da parte del supporto
@app.on_message(filters.text & filters.private & filters.user(5453376840) & filters.reply)
async def rispondi(app, message):
    await app.send_message(message.reply_to_message.text.split(" ")[0], message.text)


# Ricerca dell'annuncio in base alla parola chiave
@app.on_message(filters.private & filters.text)
async def controlla(app, message):
    messaggio = message.text

    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE LOWER(url) LIKE %s OR LOWER(title) LIKE %s OR "
                "LOWER(zone) LIKE %s OR LOWER(role) LIKE %s OR LOWER(sector) LIKE %s",
                (f"%{messaggio}%".lower(),) * 5)
    jobs_key = cur.fetchall()

    text = f"""Risultati per: <b>{message.text}</b> \n\nTotale: <b>{len(jobs_key)}</b>\n\n"""

    for i in jobs_key:
        text = text + f"➤ <a href={i[2]}>{i[3]}</a> | 🔗 \n"

    if len(jobs_key) > 0:
        await message.reply_text(text=text,
                                 reply_markup=InlineKeyboardMarkup(
                                     [[InlineKeyboardButton('Indietro ↩',
                                                            callback_data="menu")]]), disable_web_page_preview=True)

    else:
        await message.reply_text(
            text="Il <b>termine</b> digitato non corrisponde ad alcun annuncio...\n\nProva a digitarlo in modo diverso - \n<i>es: macchinista ⇔ macchinisti</i>.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Indietro ↩',
                                                                     callback_data="menu")]]))

    await app.delete_messages(message.chat.id, message.id)


# CallBackQuery Bottoni
@app.on_callback_query()
def callback_query(app, CallbackQuery):
    passaggio = CallbackQuery.data.split("/", 1)

    cur = conn.cursor()

    # Modalità di ricerca
    if CallbackQuery.data == "ricerca":
        CallbackQuery.edit_message_text(text="""⌨ <b>Digita</b> qualsiasi parola chiave tu voglia, ad esempio: 

<b>- Settore, Ruolo, Tipo di Contratto</b> 
<b>- Città, Regione, Diploma o Laurea</b> 
<b>- Data di Scadenza o Pubblicazione</b>""",
                                        reply_markup=InlineKeyboardMarkup(
                                            [[InlineKeyboardButton('Annulla ❌', callback_data="menu")]]),
                                        disable_web_page_preview=False)

    # Gestisco la regione di preferenza
    if passaggio[0] == "aggiungi":
        cur.execute("SELECT zone FROM zones WHERE zone = %s AND idUser = %s",
                    (passaggio[-1], CallbackQuery.from_user.id))

        if cur.fetchone() is not None:
            cur.execute("DELETE FROM zones WHERE zone = %s AND idUser = %s",
                        (passaggio[-1], CallbackQuery.from_user.id))
        else:
            cur.execute("INSERT INTO zones(idUser, zone) values(%s, %s)", (CallbackQuery.from_user.id, passaggio[-1]))

        passaggio[0] = "personalizza"
        passaggio[-1] = "2"

    # Gestisco il settore di preferenza
    if passaggio[0] == "aggsett":
        cur.execute("SELECT type FROM sectors WHERE type = %s AND idUser = %s",
                    (passaggio[-1], CallbackQuery.from_user.id))

        if cur.fetchone() is not None:
            cur.execute("DELETE FROM sectors WHERE type = %s AND idUser = %s",
                        (passaggio[-1], CallbackQuery.from_user.id))
        else:
            cur.execute("INSERT INTO sectors(idUser, type) values(%s, %s)", (CallbackQuery.from_user.id, passaggio[-1]))

        passaggio[0] = "personalizza"
        passaggio[-1] = "3"

    # Gestisco il tipo di notifica
    if passaggio[0] == "aggtipo":
        cur.execute("SELECT type FROM notifications WHERE type = %s AND idUser = %s",
                    (passaggio[-1], CallbackQuery.from_user.id))

        if cur.fetchone() is not None:
            cur.execute("DELETE FROM notifications WHERE type = %s AND idUser = %s",
                        (passaggio[-1], CallbackQuery.from_user.id))
        else:
            cur.execute("INSERT INTO notifications(idUser, type) values(%s, %s)",
                        (CallbackQuery.from_user.id, passaggio[-1]))

        passaggio[0] = "personalizza"
        passaggio[-1] = "4"

    # Effettuo la verifica delle preferenze
    if passaggio[0] == "verifica":

        pulsanti = [[InlineKeyboardButton('Indietro ◀', callback_data="personalizza/" + str(4))],
                    [InlineKeyboardButton('Indietro ↩', callback_data="profilo")]]

        cur.execute("SELECT * FROM notifications WHERE idUser = %s AND type = 'Nuovo'", (CallbackQuery.from_user.id,))
        new_check = cur.fetchone()

        if new_check is None:
            CallbackQuery.edit_message_text(
                text=
                """❓ Per verificare il corretto funzionamento, devi spuntare l'opzione <b>Nuovo</b> nella pagina 4.

Clicca il pulsante sotto per tornare indietro.""",
                reply_markup=InlineKeyboardMarkup(pulsanti),
                disable_web_page_preview=True)
        else:
            cur.execute("SELECT zone FROM zones WHERE idUser = %s", (CallbackQuery.from_user.id,))
            user_zones = cur.fetchall()

            cur.execute("SELECT type FROM sectors WHERE idUser = %s", (CallbackQuery.from_user.id,))
            user_sectors = cur.fetchall()

            if user_zones:
                user_zones = list(map(lambda x: f"%{x[0]}%", user_zones))

            if user_zones and user_sectors:
                cur.execute("SELECT * FROM jobs WHERE zone ILIKE ANY (%s) AND sector in %s",
                            (user_zones, tuple(user_sectors)))
            elif user_zones:
                cur.execute("SELECT * FROM jobs WHERE zone ILIKE ANY (%s)", (user_zones,))
            elif user_sectors:
                cur.execute("SELECT * FROM jobs WHERE sector in %s", (tuple(user_sectors),))
            else:
                cur.execute("SELECT * FROM jobs")

            jobs_filtered = cur.fetchall()
            testo = ""
            if jobs_filtered:
                for annunciobuono in jobs_filtered:
                    testo = testo + \
                            f"""➤ <a href='{annunciobuono[2]}'>{annunciobuono[3]}</a> | 🔗 \n"""

                CallbackQuery.edit_message_text(
                    text=f"""🚄 Annunci corrispondenti ai <b>filtri:</b>\n\n{testo}
I prossimi annunci verranno inviati direttamente in questa chat!""",
                    reply_markup=InlineKeyboardMarkup(pulsanti),
                    disable_web_page_preview=True)

            else:
                CallbackQuery.edit_message_text(
                    text="""😢 <b>Ci dispiace...</b>
    
Non ci sono annunci con i <b>filtri</b> applicati.""",
                    reply_markup=InlineKeyboardMarkup(pulsanti),
                    disable_web_page_preview=True)

    # Personalizza le notifiche
    if CallbackQuery.data == "personalizza" or passaggio[0] == "personalizza":
        testo = ""
        bottoni = []

        numeromassimo = 5
        if CallbackQuery.data == "personalizza":
            paginattuale = 1
        else:
            paginattuale = int(passaggio[-1])

        if paginattuale == 1:
            bottoni = [[InlineKeyboardButton('Avanti ▶', callback_data="personalizza" + "/" + str(paginattuale + 1))],
                       [InlineKeyboardButton('Indietro ↩', callback_data="profilo")]]

            testo = f"""📣 Scegli quali filtri applicare agli annunci da <b>notificare!</b>
    
Pagina <b>{paginattuale}/5.</b>
    
Cliccando su <b>Avanti</b> potrai passare alle pagine successive, per selezionare <i>settori</i>, <i>regioni</i> e altri campi di tuo interesse!"""

        if numeromassimo > paginattuale > 1:
            bottoni = []

            if paginattuale == 2:
                cur.execute("SELECT COUNT(*) FROM zones WHERE idUser = %s", (CallbackQuery.from_user.id,))
                count_zones = cur.fetchone()

                cur.execute("SELECT zone FROM zones WHERE idUser = %s", (CallbackQuery.from_user.id,))
                user_zones = cur.fetchall()

                testo = f"""Scegli la <b>regione</b> a cui sei interessato per gli annunci 📍

Pagina <b>{paginattuale}/5.</b>
Selezionati: <b>{count_zones[0]}.</b>

Puoi sceglierne più di una e cambiarle in qualsiasi momento!"""
                rigaregioni = []
                regioni = ["Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna",
                           "Friuli Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche",
                           "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia",
                           "Toscana", "Trentino-Alto Adige", "Umbria", "Veneto", "Val d'Aosta"]

                for regione in regioni:

                    if regione in list(map(lambda x: str(x[0]), user_zones)):
                        regionepuls = InlineKeyboardButton("✅ " + regione, callback_data="aggiungi/" + regione)
                    else:
                        regionepuls = InlineKeyboardButton("❌ " + regione, callback_data="aggiungi/" + regione)
                    rigaregioni.append(regionepuls)

                    if len(rigaregioni) == 4:
                        bottoni.append(rigaregioni)
                        rigaregioni = []

            if paginattuale == 3:
                cur.execute("SELECT COUNT(*) FROM sectors WHERE idUser = %s", (CallbackQuery.from_user.id,))
                count_sectors = cur.fetchone()

                cur.execute("SELECT type FROM sectors WHERE idUser = %s", (CallbackQuery.from_user.id,))
                user_sectors = cur.fetchall()

                testo = f"""👷🏻‍♂️Scegli il settore!

Pagina <b>{paginattuale}/5.</b>
Selezionati: <b>{count_sectors[0]}.</b>

Puoi sceglierne più di uno e cambiarli in qualsiasi momento!"""
                settori = [
                    "Altro", "Trasporti e logistica", "Ingegneria",
                    "Edilizia/Ingegneria civile", "Informatica"
                ]

                for settore in settori:
                    if settore in list(map(lambda x: str(x[0]), user_sectors)):
                        pulsante = [InlineKeyboardButton("✅ " + settore, callback_data="aggsett/" + settore + "")]

                    else:
                        pulsante = [InlineKeyboardButton("❌ " + settore, callback_data="aggsett/" + settore + "")]

                    bottoni.append(pulsante)

            if paginattuale == 4:
                cur.execute("SELECT COUNT(*) FROM notifications WHERE idUser = %s", (CallbackQuery.from_user.id,))
                count_types = cur.fetchone()

                cur.execute("SELECT type FROM notifications WHERE idUser = %s", (CallbackQuery.from_user.id,))
                user_types = cur.fetchall()

                testo = f"""🔔 Scegli la <b>tipologia</b> di notifiche!      

Pagina <b>{paginattuale}/5.</b>
Selezionati: <b>{count_types[0]}</b>.

Puoi sceglierne più di una e cambiarle in qualsiasi momento!"""

                tipologie = ["Nuovo", "Scaduto", "Aggiornato"]

                for tipo in tipologie:
                    if tipo in list(map(lambda x: x[0], user_types)):
                        pulsante = [InlineKeyboardButton('✅ ' + tipo, callback_data="aggtipo/" + tipo + "")]
                    else:
                        pulsante = [InlineKeyboardButton('❌ ' + tipo, callback_data="aggtipo/" + tipo + "")]

                    bottoni.append(pulsante)

            bottoni.append([InlineKeyboardButton('Indietro ◀', callback_data="personalizza/" + str(paginattuale - 1)),
                            InlineKeyboardButton('Avanti ▶', callback_data="personalizza/" + str(paginattuale + 1))])
            bottoni.append([InlineKeyboardButton('Indietro ↩', callback_data="profilo")])

        if paginattuale == numeromassimo:
            bottoni = [[InlineKeyboardButton('Verifica ❗', callback_data="verifica")],
                       [InlineKeyboardButton('Indietro ◀', callback_data="personalizza/" + str(paginattuale - 1))],
                       [InlineKeyboardButton('Indietro ↩', callback_data="profilo")]]
            testo = f"""Verifica i filtri inseriti! 🚄

Pagina <code>{paginattuale}/5</code>.

Verranno inviati gli annunci che rispettano i tuoi filtri, prova!"""

        CallbackQuery.edit_message_text(text=testo,
                                        reply_markup=InlineKeyboardMarkup(bottoni),
                                        disable_web_page_preview=True)

    # Visualizzo il profilo dell'utente
    if CallbackQuery.data == "profilo":
        cur.execute("SELECT COUNT(*) FROM users")

        bottoni = [[InlineKeyboardButton('Preferiti 🔗', callback_data="preferiti"),
                    InlineKeyboardButton('Personalizza 📣', callback_data="personalizza")],
                   [InlineKeyboardButton('Indietro ↩', callback_data="menu")]]

        CallbackQuery.edit_message_text(text=f"""Questo è il tuo <b>Profilo 👤</b>

Qui potrai decidere di quali annunci ricevere le <i>notifiche</i> e salvare quelli che più ti <i>interessano</i>!

👥 <b>Utenti</b> » <code>{cur.fetchone()[0]}</code> """,
                                        reply_markup=InlineKeyboardMarkup(bottoni),
                                        disable_web_page_preview=True)

    # Visualizza gli annunci preferiti dell'utente
    if CallbackQuery.data == "preferiti":

        cur.execute("SELECT * FROM favorites WHERE idUser = %s", (CallbackQuery.from_user.id,))
        favorites = cur.fetchall()

        home = [[InlineKeyboardButton('Indietro ↩', callback_data="menu")]]

        if len(favorites) == 0:
            CallbackQuery.edit_message_text(text="""🏷 <b>Questa</b> è la lista dei tuoi annunci <b>preferiti!</b>

A quanto pare non è stato salvato nessun annuncio.

Per <b>aggiungerne</b>, puoi digitare il pulsante sotto ogni annuncio presente nel canale <b>@concorsiferrovie 🚄</b>""",
                                            reply_markup=InlineKeyboardMarkup(home),
                                            disable_web_page_preview=False)
        else:
            cur.execute("SELECT * FROM jobs WHERE id IN %s", (tuple(list(map(lambda x: x[1], favorites))),))
            testo = "🏷 <b>Questa</b> è la lista dei tuoi annunci <b>preferiti!</b> \n\n"

            for favorite in cur.fetchall():
                completo = "<a href=" + favorite[2] + ">" + favorite[3] + "</a>"

                testo = testo + "➤ " + completo + \
                        " | <a href='t.me/concorsiferroviebot?start=unlike_" + str(favorite[0]) + "'>🔗</a> \n"

            testo = testo + "\nPer <b>rimuovere</b> quelli a cui non sei più interessato, digita sull'emoji a destra 🔗."
            CallbackQuery.edit_message_text(text=testo,
                                            reply_markup=InlineKeyboardMarkup(home),
                                            disable_web_page_preview=True)

    # Ultimi annunci presenti nel db
    if CallbackQuery.data == "ultime":

        messaggio = "Queste sono le ultime <b>10</b> posizioni presenti sul <b><a href=https://fscareers.gruppofs.it/jobs.php>sito</a></b>: \n\n"

        cur.execute("SELECT * FROM jobs ORDER BY idmessage DESC LIMIT 10")

        for i in cur.fetchall():
            messaggio = messaggio + f"➤ <a href={i[2]}> {i[3]} </a> |  {i[1]}\n"

        CallbackQuery.edit_message_text(
            text=messaggio,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Indietro ↩', callback_data="menu")]]),
            disable_web_page_preview=True)

    # Contatto per l'assistenza
    if CallbackQuery.data == "assistenza":
        annulla = [[InlineKeyboardButton('Annulla ❌', callback_data="menu")]]
        CallbackQuery.edit_message_text(text="""👉🏻 <b>Rispondi</b> trascinando verso sinistra questo messaggio, poi <b>digita</b> il messaggio per contattare </b>l'assistenza del bot</b>.

💡 La <b>richiesta</b> deve essere inviata in un <b>unico messaggio</b>, altri messaggi <b>non saranno recapitati</b>.""",
                                        reply_markup=InlineKeyboardMarkup(annulla),
                                        disable_web_page_preview=False)

    # Recupero le regioni in modo univoco dal db
    if CallbackQuery.data == "listaregioni":
        cur.execute("SELECT DISTINCT zone FROM jobs")
        jobs_zone = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM jobs")
        counter_jobs = cur.fetchone()

        regioni = []
        for job in jobs_zone:
            if ',' in job[0]:
                valoreffettivo = job[0].split(',')
                if "Italia" in valoreffettivo[0]:
                    regioni.append(valoreffettivo[1])

        bottonilista = []
        pulsanti = []
        for regionelavoro in regioni:

            pulsanti.append(InlineKeyboardButton(regionelavoro + " ➡ ", callback_data=f"query/zone/{regionelavoro}/1"))
            if len(pulsanti) == 2:
                bottonilista.append(pulsanti)
                pulsanti = []

        bottonilista.append(pulsanti)

        testoquery = (f"Ci sono <b>{len(regioni)}</b> regioni e <b>{counter_jobs[0]}</b> annunci.\n\nSelezionane "
                      f"una:")
        if len(bottonilista) > 0:
            bottonilista.append([InlineKeyboardButton('Italia 🇮🇹', callback_data="query/zone/Italia/1")])
        bottonilista.append([InlineKeyboardButton('Indietro ↩', callback_data="menu")])

        CallbackQuery.edit_message_text(testoquery, reply_markup=InlineKeyboardMarkup(bottonilista))

    # Recupero i settori in modo univoco dal db
    if CallbackQuery.data == "listasettore":
        cur.execute("SELECT COUNT(*) FROM jobs")
        annunci = cur.fetchone()
        cur.execute("SELECT DISTINCT sector FROM jobs")

        bottonilista = []

        for settore in cur.fetchall():
            settorelavoro = settore[0]
            pulsante = [InlineKeyboardButton(settorelavoro + " ➡ ", callback_data=f"query/sector/{settorelavoro}/1")]
            bottonilista.append(pulsante)

        listasett = len(bottonilista)

        testoquery = f"Ci sono <b>{listasett}</b> settori e <b>{annunci[0]}</b> annunci.\n\nSelezionane uno:"

        bottonilista.append([InlineKeyboardButton('Indietro ↩', callback_data="menu")])

        CallbackQuery.edit_message_text(testoquery, reply_markup=InlineKeyboardMarkup(bottonilista))

    # Visualizzo annunci impaginati
    if "query/" in CallbackQuery.data:
        bottoneinfo = []
        messaggio = ""

        if "/sector/" in CallbackQuery.data:

            type_sector = CallbackQuery.data.split("/")[2]
            page_number = int(CallbackQuery.data.split("/")[3])
            bottoneinfo = []

            cur.execute("SELECT COUNT(*) FROM jobs WHERE sector = %s", (type_sector,))
            counter_sector = cur.fetchone()

            max_page = math.ceil(counter_sector[0] / 10)

            if page_number == 1:
                cur.execute("SELECT * FROM jobs WHERE sector = %s LIMIT 10", (type_sector,))

            else:
                cur.execute("SELECT * FROM jobs WHERE sector = %s OFFSET %s LIMIT 10",
                            (type_sector, ((page_number - 1) * 10)))

            messaggio = f""" Questi sono i risultati per <b>{type_sector}</b>:\n
Totale: <b>{counter_sector[0]}</b>.
Pagina {page_number}/{max_page}\n\n"""

            for job in cur.fetchall():
                messaggio = messaggio + f"➤ <a href={job[2]}>{job[3]}</a> | 🔗\n"

            if page_number == max_page and page_number != 1:
                bottoneinfo = [[InlineKeyboardButton('◀ Pagina precedente',
                                                     callback_data=f"query/sector/{type_sector}/{str(page_number - 1)}")]]

            if 1 < page_number < max_page:
                bottoneinfo = [[InlineKeyboardButton('◀ Pagina precedente',
                                                     callback_data=f"query/sector/{type_sector}/{str(page_number - 1)}"),
                                InlineKeyboardButton('Pagina successiva ▶',
                                                     callback_data=f"query/sector/{type_sector}/{str(page_number + 1)}")]]

            if page_number == 1 and page_number < max_page:
                bottoneinfo = [[InlineKeyboardButton('Pagina successiva ▶',
                                                     callback_data=f"query/sector/{type_sector}/{str(page_number + 1)}")]]

            bottoneinfo.append([InlineKeyboardButton('Indietro ↩', callback_data="listasettore")])

        if "/zone/" in CallbackQuery.data:
            zone = CallbackQuery.data.split("/")[2]
            page_number = int(CallbackQuery.data.split("/")[3])

            cur.execute("SELECT COUNT(*) FROM jobs WHERE zone LIKE %s", (f"%{zone}%",))
            counter_zone = cur.fetchone()

            max_page = math.ceil(counter_zone[0] / 10)

            if page_number == 1:
                cur.execute("SELECT * FROM jobs WHERE zone LIKE %s LIMIT 10",
                            (f"%{zone}%",))

            else:
                cur.execute("SELECT * FROM jobs WHERE zone LIKE %s OFFSET %s LIMIT 10",
                            (f"%{zone}%", ((page_number - 1) * 10)))

            messaggio = f"""Questi sono i risultati per <b>{zone}</b>:\n
Totale: <b>{counter_zone[0]}</b>.
Pagina {page_number}/{max_page}\n\n"""

            for job in cur.fetchall():
                messaggio = messaggio + f"➤ <a href={job[2]}>{job[3]}</a> | 🔗\n"

            if page_number == max_page and page_number != 1:
                bottoneinfo = [[InlineKeyboardButton('◀ Pagina precedente',
                                                     callback_data=f"query/zone/{zone}/{str(page_number - 1)}")]]

            if 1 < page_number < max_page:
                bottoneinfo = [[InlineKeyboardButton('◀ Pagina precedente',
                                                     callback_data=f"query/zone/{zone}/{str(page_number - 1)}"),
                                InlineKeyboardButton('Pagina successiva ▶',
                                                     callback_data=f"query/zone/{zone}/{str(page_number + 1)}")]]

            if page_number == 1 and page_number < max_page:
                bottoneinfo = [[InlineKeyboardButton('Pagina successiva ▶',
                                                     callback_data=f"query/zone/{zone}/{str(page_number + 1)}")]]

            bottoneinfo.append([InlineKeyboardButton('Indietro ↩', callback_data="listaregioni")])

        CallbackQuery.edit_message_text(messaggio,
                                        reply_markup=InlineKeyboardMarkup(bottoneinfo),
                                        disable_web_page_preview=True)

    # Menu principale
    if CallbackQuery.data == "menu":
        CallbackQuery.edit_message_text(
            start_message,
            reply_markup=InlineKeyboardMarkup(start_message_buttons),
            disable_web_page_preview=False)

    conn.commit()


# Elimina messaggi default
@app.on_message(filters.private)
async def elimina(app, message):
    await app.delete_messages(message.chat.id, message.id)


# Scraping
async def scraping():

    cur = conn.cursor()
    users_blocked = []
    driver = webdriver.Chrome(options)

    driver.get(DOMAIN + "jobs.php")
    driver.implicitly_wait(10)

    driver.add_cookie({'name': 'lang', 'value': 'it_IT'})
    driver.refresh()

    soup = BeautifulSoup(driver.page_source, "lxml")

    results = soup.find("div", {
        "class": "searchResultsBody"
    }).find_all("div", {"class": "singleResult responsiveOnly"})

    for result in results:
        details = result.find("div", {"class": "details"})

        jobUrl = url_normalize(DOMAIN + details.find("a")["href"])
        jobTitle = details.find("h3").text.strip()

        lista_posizione = [
            span.text.strip().title() for span in details.find_next(
                string="Sede:").find_next("td").find_next("span").find_all("span")
            if span.text.strip()
        ]

        jobZone = ' , '.join(lista_posizione)

        driver.get(jobUrl)
        driver.implicitly_wait(10)
        driver.add_cookie({'name': 'lang', 'value': 'it_IT'})
        driver.refresh()

        soupannuncio = BeautifulSoup(driver.page_source, 'lxml')

        try:
            jobDescription = soupannuncio.find("div", {
                "itemprop": "description"
            }).text.strip()
        except:
            jobDescription = soupannuncio.find("div", {
                "class": "locationList"
            }).text.strip()

        jobSector = details.find_next(string="Settore:").find_next("span").text
        jobRole = details.find_next(string="Ruolo:").find_next("span").text

        jobDate = details.find("span", {"class": "date"}).text

        dateDB = datetime.strptime(jobDate, '%d/%m/%Y').strftime('%Y-%m-%d')

        parsed_url = urlparse(jobUrl)
        params = parse_qs(parsed_url.query)
        jobID = params['id'][0]

        cur.execute("SELECT * FROM jobs WHERE id = %s", (jobID,))
        jobDB = cur.fetchone()

        if jobDB is not None:
            if str(jobDB[1]) != str(dateDB):
                cur.execute(
                    "UPDATE jobs SET date = %s, sector = %s, role = %s, zone = %s, title = %s WHERE id = %s",
                    (dateDB, jobSector, jobRole, jobZone, jobTitle, jobID))

                aggiornobuttons = [[
                    InlineKeyboardButton('Visualizza messaggio 🔗', url=f"t.me/concorsiferrovie/{jobDB[7]}"),
                    InlineKeyboardButton('Guadagna 💰', url="https://t.me/concorsiferrovie/1430")
                ]]

                await app.send_message(chat_id=CHAT_ID,
                                       text=f"""📣 <b>Annuncio aggiornato!</b>

🔗 <a href='{jobUrl}'>{jobTitle}</a>

📅 __Data aggiornata: {jobDate}__""",
                                       reply_markup=InlineKeyboardMarkup(aggiornobuttons),
                                       reply_to_message_id=jobDB[7],
                                       disable_web_page_preview=True)

        else:
            telegraph.create_account("@ConcorsiFerrovie")
            response = telegraph.create_page(f'{jobTitle}',
                                             html_content=f'<p>{jobDescription}</p>')

            annunciobuttons = [[
                InlineKeyboardButton(
                    'Condividi il canale ❗',
                    url=
                    "https://telegram.me/share/url?url=https://telegram.me/concorsiferrovie&text=Unisciti%20per%20ricevere%20notifiche%20sulle%20nuove%20posizioni%20disponibili%20sul%20sito%20delle%20Ferrovie%20Dello%20Stato%20"
                ),
                InlineKeyboardButton('Gruppo discussione 🗣',
                                     url="t.me/selezioniconcorsiferrovie")
            ], [InlineKeyboardButton('Descrizione 📃', url=f"{response['url']}")],
                [InlineKeyboardButton('Guadagna 💰',
                                      url="https://t.me/concorsiferrovie/1430"),

                 InlineKeyboardButton(
                     'Aggiungi ai preferiti 🏷',
                     url="t.me/concorsiferroviebot?start=like_" + jobID + "")
                 ]]

            sentMessage = await app.send_message(CHAT_ID,
                                                 f"""🚄 <b>Nuovo annuncio!</b>

🔗 <a href='{jobUrl}'>{jobTitle}</a>

📍 Sede: <b>{jobZone}</b>
💼 Settore: <b>{jobSector}</b>
📄 Ruolo: <b>{jobRole}</b>

📅 <i>Data di pubblicazione: {jobDate}</i>""",
                                                 reply_markup=InlineKeyboardMarkup(annunciobuttons),
                                                 disable_web_page_preview=True)

            annunciobuttons.append([
                InlineKeyboardButton("Condividi su WhatsApp 📱", url=url_normalize(
                    "https://api.whatsapp.com/send?text=Guarda+questo+annuncio+di+lavoro+delle+Ferrovie+Dello+Stato:+"
                    + jobTitle.replace(' ', '+') + "+" + sentMessage.link))
            ])

            message_edited = await app.edit_message_reply_markup(chat_id=CHAT_ID, message_id=sentMessage.id,
                                                                 reply_markup=InlineKeyboardMarkup(annunciobuttons))

            cur.execute(
                "INSERT INTO jobs(id, date, url, title, zone, role, sector, idmessage) values (%s, %s, %s, %s, %s, "
                "%s, %s, %s)",
                (jobID, dateDB, jobUrl, jobTitle, jobZone, jobRole, jobSector, sentMessage.id))

            cur.execute("SELECT idUser FROM notifications WHERE type = 'Nuovo'")
            users = cur.fetchall()
            for user in users:
                cur.execute("SELECT type FROM sectors WHERE idUser = %s", (user[0],))
                user_sectors = cur.fetchall()
                cur.execute("SELECT zone FROM zones WHERE idUser = %s", (user[0],))
                user_zones = cur.fetchall()

                send = False
                if user_zones and user_sectors:
                    if jobSector in [sector[0] for sector in user_sectors] and any(
                            zone[0] in jobZone for zone in user_zones):
                        send = True
                elif user_zones:
                    if any(zone[0] in jobZone for zone in user_zones):
                        send = True
                elif user_sectors:
                    if jobSector in [sector[0] for sector in user_sectors]:
                        send = True
                else:
                    send = True
                if send:
                    try:
                        await app.forward_messages(chat_id=user[0],
                                                   from_chat_id=CHAT_ID,
                                                   message_ids=message_edited.id)
                    except:
                        users_blocked.append(user[0])
        conn.commit()
    if len(users_blocked) > 0:
        cur.execute("DELETE FROM favorites WHERE idUser IN %s", (tuple(users_blocked),))
        cur.execute("DELETE FROM sectors WHERE idUser IN %s", (tuple(users_blocked),))
        cur.execute("DELETE FROM zones WHERE idUser IN %s", (tuple(users_blocked),))
        cur.execute("DELETE FROM notifications WHERE idUser IN %s", (tuple(users_blocked),))
        cur.execute("DELETE FROM users WHERE id IN %s", (tuple(users_blocked),))

    conn.commit()
    driver.quit()
    await app.send_message(chat_id=5453376840, text="Finito scrape")


# Pulizia Annunci Scaduti
async def clean():
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs")
    jobs = cursor.fetchall()

    driver = webdriver.Chrome(options)
    driver.get(DOMAIN)
    driver.add_cookie({'name': 'lang', 'value': 'it_IT'})
    driver.refresh()
    delete_list = []

    for job in jobs:

        driver.get(job[2])
        driver.implicitly_wait(10)
        soup = BeautifulSoup(driver.page_source, "lxml")

        if soup.find("div", {"class": "searchTitle"}) is not None:
            delete_list.append(job[0])

    if len(delete_list) > 0:
        cursor.execute("DELETE FROM favorites WHERE idJob IN %s", (tuple(delete_list),))
        cursor.execute("DELETE FROM jobs WHERE id IN %s", (tuple(delete_list),))

    driver.quit()
    conn.commit()


app.start()
scheduler.add_job(clean, "cron", hour=1, next_run_time=datetime.now() + timedelta(seconds=30))
scheduler.add_job(scraping, "interval", minutes=10, next_run_time=datetime.now() + timedelta(seconds=10))
scheduler.start()
keep_alive()
idle()
