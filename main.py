import json
import httpx
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

ssl_context = ssl.create_default_context(cafile=certifi.where())


def start():
    print("started")


scheduler = AsyncIOScheduler(timezone="Europe/Rome")

options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

telegraph = Telegraph()

load_dotenv()

api_id_importato = os.environ['api_id']
api_hash_importato = os.environ['api_hash']
bot_token_importato = os.environ['bot_token']
chat_id_importato = os.environ['chat_id']

telegram = Client("ferrovie-bot",
                  api_id=api_id_importato,
                  api_hash=api_hash_importato,
                  bot_token=bot_token_importato)

CHAT_ID = int(chat_id_importato)
MESSAGE = """

🚄 <b>Nuovo annuncio!</b>

🔗 <a href='{url}'>{title}</a> 

📍 Sede: <b>{zone}</b>  
💼 Settore: <b>{sector}</b> 
📄 Ruolo: <b>{role}</b> 

📅 <i>Data di pubblicazione: {day}/{month}/{year}</i> 
"""

DOMAIN = "https://fscareers.gruppofs.it/"
CACHE_FILE = "cacheferrovie.json"

start_message = """
Ciao! Questo è un bot <b>non ufficiale delle Ferrovie Dello Stato</b> 🚄
<a href= https://telegra.ph/file/9d5be8ab56b1788848e60.jpg> </a>
Unisciti al canale per rimanere aggiornato sulle posizioni presenti sul <a href=https://fscareers.gruppofs.it/jobs.php><b>sito ufficiale</b></a> ❗

__Usa i tasti per visualizzare le posizioni disponibili in base alle tue preferenze, o per cercarne una specifica__ 👁

"""
start_message_buttons = [
    [
        InlineKeyboardButton('Canale notifiche ❗', url="t.me/concorsiferrovie"),
        InlineKeyboardButton('Gruppo discussione 🗣  ',
                             url="t.me/selezioniconcorsiferrovie")
    ],
    [InlineKeyboardButton('Ultime posizioni 📅 ', callback_data="ultime")],
    [
        InlineKeyboardButton('Lista per settore 👷🏻‍♂️ ',
                             callback_data="listasettore"),
        InlineKeyboardButton('Lista per regione 📍 ', callback_data="listaregioni")
    ],
    [InlineKeyboardButton('Cerca 🔍 ', callback_data="ricerca")],
    [
        # InlineKeyboardButton('Guida candidatura 📄 ', callback_data="candidatura"),
        # InlineKeyboardButton(' Guida selezioni 📝', callback_data="selezioni")
        InlineKeyboardButton('Profilo 👤', callback_data="profilo"),
        # InlineKeyboardButton('FAQ ❔', callback_data="domande")
        InlineKeyboardButton('Guadagna 💰',
                             url="https://t.me/concorsiferrovie/1430")
    ],
    [InlineKeyboardButton('Assistenza ✉', callback_data="assistenza")]
]


@telegram.on_message(filters.command('start') & filters.private)
async def start_command(telegram, message):
    listautenti = json.load(open("utenti.json"))
    data = {}

    data["iduser"] = message.chat.id
    data["date"] = message.date

    if any(data["iduser"] == i["iduser"] for i in listautenti):
        print("utente già presente")
    else:
        listautenti.append(data)

        f = open("utenti.json", "w+", encoding="utf-8")
        f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))

    markup = InlineKeyboardMarkup(start_message_buttons)

    if message.text == "/start":
        await message.reply(text=start_message,
                            reply_markup=markup,
                            disable_web_page_preview=False)
    else:
        diviso = (message.text).split()
        risultato = (diviso[1]).split("_")
        azione = risultato[0]

        bottoni_link = [[
            InlineKeyboardButton('Profilo 👤', callback_data="profilo"),
        ], [InlineKeyboardButton('Indietro ↩', callback_data="menu")]]

        if azione == "like":
            trovato = 0
            annuncio = risultato[1]
            annuncioint = int(annuncio)

            utente = message.chat.id

            listannunci = json.load(open("cacheferrovie.json"))
            for persona in listautenti:
                if persona["iduser"] == utente:
                    if "preferiti" in persona:

                        preferiti = persona['preferiti']
                        check = len(preferiti)

                        if check == 0:
                            for annunci in listannunci:
                                if annuncioint == annunci['idmessaggio']:
                                    annunciojson = {}
                                    annunciojson['titolo'] = annunci['title']
                                    annunciojson['link'] = annunci['linkmessaggio']
                                    annunciojson['id'] = annuncioint

                                    link = annunciojson['link']
                                    titolo = annunciojson['titolo']

                        for preferito in preferiti:

                            if annuncioint == preferito['id']:
                                trovato = 1

                                for annunci in preferiti:
                                    if annuncioint == annunci['id']:
                                        link = annunci['link']
                                        titolo = annunci['titolo']

                            else:

                                for annunci in listannunci:
                                    if annuncioint == annunci['idmessaggio']:
                                        annunciojson = {}
                                        annunciojson['titolo'] = annunci['title']
                                        annunciojson['link'] = annunci['linkmessaggio']
                                        annunciojson['id'] = annuncioint

                                        link = annunciojson['link']
                                        titolo = annunciojson['titolo']

                    else:

                        persona['preferiti'] = []
                        for annunci in listannunci:
                            if annuncioint == annunci['idmessaggio']:
                                annunciojson = {}
                                annunciojson['titolo'] = annunci['title']
                                annunciojson['link'] = annunci['linkmessaggio']
                                annunciojson['id'] = annuncioint

                                link = annunciojson['link']
                                titolo = annunciojson['titolo']

                    if trovato == 1:
                        await message.reply(
                            text=f"""❓ <b>Annuncio già aggiunto!</b>

➤ <a href='{link}'> {titolo} </a> | 🔗

Digita il pulsante sottostante per accedere al tuo <b>Profilo 👤</b>

""",
                            reply_markup=InlineKeyboardMarkup(bottoni_link),
                            disable_web_page_preview=True)
                    if trovato == 0:
                        try:
                            persona['preferiti'].append(annunciojson)

                            await message.reply(
                                text=f"""✔ <b>Annuncio salvato correttamente!</b>

➤ <a href='{link}'> {titolo} </a> | 🔗

Potrai visualizzare questo e gli altri nel tuo <b>Profilo 👤</b>

  """,
                                reply_markup=InlineKeyboardMarkup(bottoni_link),
                                disable_web_page_preview=True)
                        except:
                            await message.reply(
                                text=f"""⚠ <b>Attenzione!</b>

A quanto pare, questo annuncio è stato <b>cancellato</b> dal nostro database.

Se l'annuncio è disponibile sul sito, ti invitiamo a contattare <b>l'assistenza</b>.

""",
                                reply_markup=InlineKeyboardMarkup(bottoni_link),
                                disable_web_page_preview=True)

                    f = open("utenti.json", "w+", encoding="utf-8")
                    f.write(
                        json.dumps(listautenti, indent=4, sort_keys=True, default=str))

        if azione == "unlike":
            trovato = 0
            annuncio = risultato[1]
            annuncioint = int(annuncio)

            utente = message.chat.id

            listannunci = json.load(open("cacheferrovie.json"))
            for persona in listautenti:
                if persona["iduser"] == utente:
                    if "preferiti" in persona:

                        preferiti = persona['preferiti']
                        check = len(preferiti)

                        if check == 0:

                            for annunci in listannunci:
                                if annuncioint == annunci['idmessaggio']:
                                    annunciojson = {}
                                    annunciojson['titolo'] = annunci['title']
                                    annunciojson['link'] = annunci['linkmessaggio']
                                    annunciojson['id'] = annuncioint

                                    link = annunci['linkmessaggio']
                                    titolo = annunci['title']
                        else:
                            for preferito in preferiti:
                                if annuncioint == preferito['id']:
                                    trovato = 1

                                    for annunci in listannunci:
                                        if annuncioint == annunci['idmessaggio']:
                                            annunciojson = {}
                                            annunciojson['titolo'] = annunci['title']
                                            annunciojson['link'] = annunci['linkmessaggio']
                                            annunciojson['id'] = annuncioint

                                            link = annunci['linkmessaggio']
                                            titolo = annunci['title']

                                else:

                                    for annunci in listannunci:
                                        if annuncioint == annunci['idmessaggio']:
                                            annunciojson = {}
                                            annunciojson['titolo'] = annunci['title']
                                            annunciojson['link'] = annunci['linkmessaggio']
                                            annunciojson['id'] = annuncioint

                                            link = annunciojson['link']
                                            titolo = annunciojson['titolo']

                    else:

                        for annunci in listannunci:
                            if annuncioint == annunci['idmessaggio']:
                                annunciojson = {}
                                annunciojson['titolo'] = annunci['title']
                                annunciojson['link'] = annunci['linkmessaggio']
                                annunciojson['id'] = annuncioint

                                link = annunciojson['link']
                                titolo = annunciojson['titolo']

                    if trovato == 1:
                        try:
                            persona['preferiti'].remove(annunciojson)

                            await message.reply(
                                text=f"""❌ <b>Annuncio rimosso!</b>

➤ <a href='{link}'> {titolo} </a> | 🔗

Questo annuncio non sarà più visualizzabile nel <b>Profilo 👤</b>

  """,
                                reply_markup=InlineKeyboardMarkup(bottoni_link),
                                disable_web_page_preview=True)
                        except:

                            await message.reply(
                                text=f"""⚠ <b>Attenzione!</b>

A quanto pare, questo annuncio è stato <b>cancellato</b> dal nostro database.

Se l'annuncio è disponibile sul sito, ti invitiamo a contattare <b>l'assistenza</b>.

""",
                                reply_markup=InlineKeyboardMarkup(bottoni_link),
                                disable_web_page_preview=True)
                    if trovato == 0:
                        await message.reply(
                            text=f"""❓ <b>Annuncio non presente!</b>

➤ <a href='{link}'> {titolo} </a> | 🔗

Potrai visualizzare gli altri nel tuo <b>Profilo 👤</b> o aggiungerli dal canale <b> @concorsiferrovie </b>

""",
                            reply_markup=InlineKeyboardMarkup(bottoni_link),
                            disable_web_page_preview=True)

                    f = open("utenti.json", "w+", encoding="utf-8")
                    f.write(
                        json.dumps(listautenti, indent=4, sort_keys=True, default=str))


@telegram.on_message(filters.command('help') & filters.private)
async def help_command(telegram, message):
    await telegram.send_message(
        message.chat.id,
        "Vuoi chiedere informazioni? Unisciti al gruppo <b>@selezioniconcorsiferrovie</b>  👥"
    )


@telegram.on_message(filters.command('selezioni'))
async def selezioni(telegram, message):
    await telegram.send_document(
        message.chat.id,
        document=
        "BQACAgEAAxkBAAOLY3kya8znQHFPlyaWUVh6Kza1rjUAAi0CAAKRc8lHK8mW1THwNpEeBA",
        caption=
        "Guida per l'iter di selezione, redatta dal<b><a href=t.me/SelezioniConcorsiFerrovie> gruppo telegram </a></b>👥"
    )


@telegram.on_message(filters.command('candidatura'))
async def candidatura(telegram, message):
    await telegram.send_document(
        message.chat.id,
        document=
        "BQACAgEAAxkBAAOJY3kyWRgpX92LB3j1aYTXgihYunUAAucCAAKnP6FEAxxEXa5Ct5IeBA",
        caption=
        "Guida per l'invio della candidatura, redatta dal<b><a href=t.me/SelezioniConcorsiFerrovie> gruppo telegram </a></b>👥"
    )


@telegram.on_message(filters.text & filters.private & filters.reply
                     & ~filters.user(5052203932))
def contatta(telegram, message):
    message.reply_text(
        "🚄 Ciao! Il tuo messaggio è stato inviato <b>correttamente</b>. \n\nRiceverai una risposta appena possibile."
    )

    id = message.chat.id
    if id != "":
        sent = message.forward(5052203932)
        reply = sent.id
        telegram.send_message(chat_id=5052203932,
                              text="`" + str(id) + "`" +
                                   " ha inviato un messaggio ❗",
                              reply_to_message_id=reply)


@telegram.on_message(filters.text & filters.private & filters.user(5052203932)
                     & filters.reply)
def rispondi(telegram, message):
    chatidrisposta = message.reply_to_message.text.split(" ")[0]

    telegram.send_message(chatidrisposta, message.text)


@telegram.on_message(filters.private & filters.text)
def controlla(telegram, message):
    messaggio = message.text

    bottone = [[InlineKeyboardButton('Indietro ↩', callback_data="menu")]]
    jsonFile = open('cacheferrovie.json')

    data = json.load(jsonFile)

    inizio = f"""Risultati per: <b>{message.text}</b> \n\n"""
    invia = ""
    trovato = 0
    for i in data:

        titolo = i['title']
        url = i['url']
        link = f"<a href={url}>{titolo}</a>"

        info = [
            i['title'], i['zone'], i['role'], i['sector'], i['descrizione'], i['url']
        ]
        if any(messaggio.lower() in stringa.lower() for stringa in info):
            trovato = trovato + 1

            invia = invia + "➤ " + link + " | 🔗 \n"

    if trovato >= 1:

        totale = f"Totale: <b>{trovato}</b>\n\n"
        telegram.delete_messages(message.chat.id, message.id)
        message.reply_text(text=inizio + totale + invia,
                           reply_markup=InlineKeyboardMarkup(bottone),
                           disable_web_page_preview=True)

    else:
        telegram.delete_messages(message.chat.id, message.id)
        message.reply_text(
            text=
            "Il <b>termine</b> digitato non corrisponde ad alcun annuncio... \n\nProva a digitarlo in modo diverso - \n<i>es: macchinista ⇔ macchinisti</i>.",
            reply_markup=InlineKeyboardMarkup(bottone))


@telegram.on_callback_query()
def callback_query(Client, CallbackQuery):
    if CallbackQuery.data == "ricerca":
        annulla = [[InlineKeyboardButton('Annulla ❌', callback_data="menu")]]
        CallbackQuery.edit_message_text(text="""
⌨ <b>Digita</b> qualsiasi parola chiave tu voglia, ad esempio: 

<b>- Settore, Ruolo, Tipo di Contratto</b> 
<b>- Città, Regione, Diploma o Laurea</b> 
<b>- Data di Scadenza o Pubblicazione</b> 



""",
                                        reply_markup=InlineKeyboardMarkup(annulla),
                                        disable_web_page_preview=False)

    passaggio = CallbackQuery.data.split("/", 1)
    print(passaggio)
    if passaggio[0] == "aggiungi":

        listautenti = json.load(open("utenti.json"))
        data = {}

        data["iduser"] = CallbackQuery.from_user.id

        if any(data["iduser"] == utente["iduser"] for utente in listautenti):
            print("ok")
        else:
            listautenti.append(data)
            f = open("utenti.json", "w+", encoding="utf-8")
            f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))
        for utente in listautenti:
            #####
            if utente["iduser"] == data["iduser"]:
                print("bella")

                if utente.get('regione') != None:

                    regione = passaggio[-1]

                    if passaggio[-1] in utente["regione"]:

                        utente["regione"].remove(regione)

                    else:
                        utente["regione"].append(regione)

                else:
                    utente["regione"] = []
                    utente["regione"].append(passaggio[-1])

                f = open("utenti.json", "w+", encoding="utf-8")
                f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))

                passaggio[0] = "personalizza"
                passaggio[-1] = "2"
                CallbackQuery.from_user.id = utente['iduser']

    if passaggio[0] == "aggsett":

        listautenti = json.load(open("utenti.json"))
        data = {}

        data["iduser"] = CallbackQuery.from_user.id

        if any(data["iduser"] == i["iduser"] for i in listautenti):
            print("ok")
        else:
            listautenti.append(data)
            f = open("utenti.json", "w+", encoding="utf-8")
            f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))
        for utente in listautenti:
            #####
            if utente["iduser"] == data["iduser"]:
                print("bella")

                settore = passaggio[-1]

                if utente.get('settore') != None:

                    if passaggio[-1] in utente["settore"]:

                        utente["settore"].remove(settore)

                    else:
                        utente["settore"].append(settore)

                else:

                    utente["settore"] = []

                    utente["settore"].append(settore)

                f = open("utenti.json", "w+", encoding="utf-8")
                f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))

                passaggio[0] = "personalizza"
                passaggio[-1] = "3"
                CallbackQuery.from_user.id = utente['iduser']

    if passaggio[0] == "aggtipo":

        listautenti = json.load(open("utenti.json"))
        data = {}

        data["iduser"] = CallbackQuery.from_user.id

        if any(data["iduser"] == i["iduser"] for i in listautenti):
            print("ok")
        else:
            listautenti.append(data)
            f = open("utenti.json", "w+", encoding="utf-8")
            f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))
        for utente in listautenti:
            #####
            if utente["iduser"] == data["iduser"]:
                print("bella")
                tipo = passaggio[-1]

                if utente.get("tiponot") != None:

                    if passaggio[-1] in utente["tiponot"]:

                        utente["tiponot"].remove(tipo)

                    else:
                        utente["tiponot"].append(tipo)
                else:
                    utente["tiponot"] = []
                    utente["tiponot"].append(tipo)

                f = open("utenti.json", "w+", encoding="utf-8")
                f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))
                passaggio[0] = "personalizza"
                passaggio[-1] = "4"
                CallbackQuery.from_user.id = utente['iduser']

    if passaggio[0] == "verifica":
        listautenti = json.load(open("utenti.json"))

        idutente = CallbackQuery.from_user.id

        if any(idutente == i["iduser"] for i in listautenti):
            print("ok")
        else:
            listautenti.append(data)
            f = open("utenti.json", "w+", encoding="utf-8")
            f.write(json.dumps(listautenti, indent=4, sort_keys=True, default=str))

        listannunci = json.load(open("cacheferrovie.json"))

        for utente in listautenti:
            if utente["iduser"] == idutente:
                utentecheck = utente

        inviati = 0
        passa = 0
        testo = "\n\n"
        pulsanti = [[
            InlineKeyboardButton('Indietro ◀',
                                 callback_data="personalizza/" + str(4))
        ], [InlineKeyboardButton('Indietro ↩', callback_data="profilo")]]
        for tipo in utentecheck["tiponot"]:
            if tipo == "Nuovo":
                passa = 1

        listannuncicheck = []
        if passa == 1:
            for annuncio in listannunci:

                for settore in utentecheck["settore"]:
                    if annuncio["sector"] == settore:
                        inviati = 1

                        listannuncicheck.append(annuncio)
                for regione in utentecheck["regione"]:

                    annunciozone = annuncio["zone"].split(" , ")
                    lungh = len(annunciozone)
                    if lungh > 1:
                        annuncioregione = annunciozone[1]
                        if annuncioregione == regione:
                            if annuncio in listannuncicheck:
                                print("già presente")
                            else:
                                listannuncicheck.append(annuncio)
                                inviati = 1

            for annunciobuono in listannuncicheck:
                annunciolink = annunciobuono["linkmessaggio"]
                annunciotitolo = annunciobuono["title"]
                testo = testo + \
                        f"""➤ <a href='{annunciolink}'>{annunciotitolo}</a> | 🔗 \n"""

            if inviati == 1:
                CallbackQuery.edit_message_text(
                    text=f"""🚄 Annunci corrispondenti ai <b>filtri:</b>""" + testo + """
I prossimi annunci verranno inviati direttamente in questa chat!""",
                    reply_markup=InlineKeyboardMarkup(pulsanti),
                    disable_web_page_preview=True)

            if inviati == 0:
                CallbackQuery.edit_message_text(
                    text="""😢 <b>Ci dispiace...</b>

Non ci sono annunci con i <b>filtri</b> applicati """,
                    reply_markup=InlineKeyboardMarkup(pulsanti),
                    disable_web_page_preview=True)

        else:

            CallbackQuery.edit_message_text(
                text=
                """❓ Per verificare il corretto funzionamento, devi spuntare l'opzione <b> Nuovo </b> nella pagina 4.
        
        Clicca il pulsante sotto per tornare indietro.""",
                reply_markup=InlineKeyboardMarkup(pulsanti),
                disable_web_page_preview=True)

    if CallbackQuery.data == "personalizza" or passaggio[0] == "personalizza":
        print("entrato")
        numeromassimo = 5
        if CallbackQuery.data == "personalizza":
            paginattuale = 1

        else:
            paginattuale = int(passaggio[-1])
        if paginattuale == 1:
            bottoni = [[
                InlineKeyboardButton('Avanti ▶',
                                     callback_data="personalizza" + "/" +
                                                   str(paginattuale + 1))
            ], [InlineKeyboardButton('Indietro ↩', callback_data="profilo")]]
        Lista: list = json.load(open("utenti.json"))

        testo = f"""Scegli quali filtri applicare agli annunci da <b>notificare!</b> 📣

Pagina <b>{paginattuale}/5.</b>

Cliccando su <b>Avanti</b> potrai passare alle pagine successive, per selezionare <i>settori</i>, <i>regioni</i> e altri campi di tuo interesse!


"""

        if paginattuale < numeromassimo and paginattuale > 1:
            bottoni = []

            file = open("utenti.json")

            listautenti = json.load(file)
            if paginattuale == 2:

                for utente in listautenti:
                    if utente["iduser"] == CallbackQuery.from_user.id:

                        if utente.get('regione') == None:
                            utente["regione"] = []

                        utentecontrollo = utente["regione"]

                testo = f"""Scegli la <b>regione</b> a cui sei interessato per gli annunci 📍

Pagina <b>{paginattuale}/5.</b>

Puoi sceglierne più di una e cambiarle in qualsiasi momento!

"""
                rigaregioni = []
                regioni = [
                    "Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna",
                    "Friuli Venezia Giulia", "Lazio", "Liguria", "Lombardia", "Marche",
                    "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana",
                    "Trentino-Alto Adige", "Umbria", "Veneto", "Val d'Aosta"
                ]
                for regione in regioni:

                    if regione in utentecontrollo:
                        regionepuls = InlineKeyboardButton("✅ " + regione,
                                                           callback_data="aggiungi/" +
                                                                         regione + "")
                    else:
                        regionepuls = InlineKeyboardButton("❌ " + regione,
                                                           callback_data="aggiungi/" +
                                                                         regione + "")
                    rigaregioni.append(regionepuls)

                    controllo = len(rigaregioni)
                    if controllo == 4:
                        bottoni.append(rigaregioni)
                        rigaregioni = []

            if paginattuale == 3:

                for utente in listautenti:
                    if utente["iduser"] == CallbackQuery.from_user.id:

                        if utente.get('settore') == None:
                            utente["settore"] = []

                        utentecontrollo = utente["settore"]

                testo = f"""Scegli il settore 👷🏻‍♂️        

Pagina <b>{paginattuale}/5.</b>

Puoi sceglierne più di uno e cambiarli in qualsiasi momento!

"""
                pulsante = []
                settori = [
                    "Altro", "Trasporti e logistica", "Ingegneria",
                    "Edilizia/Ingegneria civile", "Informatica"
                ]

                for settore in settori:
                    if settore in utentecontrollo:

                        pulsante = [
                            InlineKeyboardButton("✅ " + settore,
                                                 callback_data="aggsett/" + settore + "")
                        ]

                    else:

                        pulsante = [
                            InlineKeyboardButton("❌ " + settore,
                                                 callback_data="aggsett/" + settore + "")
                        ]

                    bottoni.append(pulsante)
                    pulsante = []

            if paginattuale == 4:

                for utente in listautenti:
                    if utente["iduser"] == CallbackQuery.from_user.id:

                        if utente.get('tiponot') == None:
                            utente["tiponot"] = []

                        utentecontrollo = utente["tiponot"]

                testo = f"""Scegli la <b>tipologia</b> di notifiche 🔔        

Pagina <b>{paginattuale}/5.</b>

Puoi sceglierne più di una e cambiarle in qualsiasi momento!

"""

                tipologie = ["Nuovo", "Scaduto", "Aggiornato"]

                pulsante = []
                for tipo in tipologie:
                    if tipo in utentecontrollo:
                        pulsante = [
                            InlineKeyboardButton('✅ ' + tipo,
                                                 callback_data="aggtipo/" + tipo + "")
                        ]
                    else:
                        pulsante = [
                            InlineKeyboardButton('❌ ' + tipo,
                                                 callback_data="aggtipo/" + tipo + "")
                        ]

                    bottoni.append(pulsante)
                    pulsante = []

            orientamento = [
                InlineKeyboardButton('Indietro ◀',
                                     callback_data="personalizza/" +
                                                   str(paginattuale - 1)),
                InlineKeyboardButton('Avanti ▶',
                                     callback_data="personalizza/" +
                                                   str(paginattuale + 1))
            ]
            bottoni.append(orientamento)

            bottoni.append(
                [InlineKeyboardButton('Indietro ↩', callback_data="profilo")])

        if paginattuale == numeromassimo:
            bottoni = [[
                InlineKeyboardButton('Verifica ❗', callback_data="verifica")
            ],
                [
                    InlineKeyboardButton('Indietro ◀',
                                         callback_data="personalizza/" +
                                                       str(paginattuale - 1))
                ],
                [InlineKeyboardButton('Indietro ↩', callback_data="profilo")]]
            testo = f"""Verifica i filtri inseriti! 🚄

Pagina <code>{paginattuale}/5</code>.

Verranno inviati gli annunci che rispettano i tuoi filtri, prova!

"""

        CallbackQuery.edit_message_text(text=testo,
                                        reply_markup=InlineKeyboardMarkup(bottoni),
                                        disable_web_page_preview=True)

    if CallbackQuery.data == "profilo":
        listautenti = json.load(open("utenti.json"))
        registrati = len(listautenti)
        bottoni = [[
            InlineKeyboardButton('Preferiti 🔗', callback_data="preferiti"),
            InlineKeyboardButton('Personalizza 📣', callback_data="personalizza")
        ], [InlineKeyboardButton('Indietro ↩', callback_data="menu")]]

        CallbackQuery.edit_message_text(text=f"""Questo è il tuo <b>Profilo 👤</b>

Qui potrai decidere di quali annunci ricevere le <i>notifiche</i> e salvare quelli che più ti <i>interessano</i>!

👥 <b>Utenti</b> » <code>{registrati}</code> """,
                                        reply_markup=InlineKeyboardMarkup(bottoni),
                                        disable_web_page_preview=True)

    if CallbackQuery.data == "domande":
        CallbackQuery.answer("Funzione al momento non disponibile! ⚠",
                             show_alert=True)

    if CallbackQuery.data == "preferiti":

        listautenti = json.load(open("utenti.json"))
        utente = CallbackQuery.from_user.id

        for persona in listautenti:

            if persona["iduser"] == utente:
                if "preferiti" in persona:
                    print('già creato')
                else:
                    persona['preferiti'] = []
                    f = open("utenti.json", "w+", encoding="utf-8")
                    f.write(
                        json.dumps(listautenti, indent=4, sort_keys=True, default=str))

                home = [[InlineKeyboardButton('Indietro ↩', callback_data="menu")]]
                listapreferiti = persona['preferiti']
                quantita = len(listapreferiti)

                if quantita == 0:
                    CallbackQuery.edit_message_text(
                        text="""
🏷 <b>Questa</b> è la lista dei tuoi annunci <b>preferiti!</b>

A quanto pare non è stato salvato nessun annuncio.

Per <b>aggiungerne</b>, puoi digitare il pulsante sotto ogni annuncio presente nel canale <b>@concorsiferrovie 🚄</b>

""",
                        reply_markup=InlineKeyboardMarkup(home),
                        disable_web_page_preview=False)
                else:
                    testo = "🏷 <b>Questa</b> è la lista dei tuoi annunci <b>preferiti!</b> \n\n"

                    preferiti = persona['preferiti']
                    for preferito in preferiti:
                        id = str(preferito['id'])
                        link = preferito['link']
                        titolo = preferito['titolo']
                        completo = "<a href=" + link + ">" + titolo + "</a>"

                        testo = testo + "➤ " + completo + \
                                " | <a href='t.me/concorsiferroviebot?start=unlike_" + id + "'>🔗</a> \n"

                    testo = testo + "\nPer <b>rimuovere</b> quelli a cui non sei più interessato, digita sull'emoji a destra 🔗."
                    CallbackQuery.edit_message_text(
                        text=testo,
                        reply_markup=InlineKeyboardMarkup(home),
                        disable_web_page_preview=True)

    if CallbackQuery.data == "ultime":

        messaggio = "Queste sono le ultime <b>10</b> posizioni presenti sul <b><a href=https://fscareers.gruppofs.it/jobs.php>sito</a></b>: \n\n"

        jsonFile = open('cacheferrovie.json')

        data = json.load(jsonFile)

        ultimi = data[-10:]

        ordinati = list(reversed(ultimi))

        for i in ordinati:
            url = i['url']
            titolo = i['title']
            pubblicato = f"{i['day']}/{i['month']}/{i['year']}"

            link = f"<a href={url}> {titolo} </a>"

            messaggio = messaggio + "➤ " + link + "| " + pubblicato + "\n"

        testoultime = messaggio

        menu = [InlineKeyboardButton('Indietro ↩', callback_data="menu")]

        bottonilista = [menu]

        CallbackQuery.edit_message_text(
            text=testoultime,
            reply_markup=InlineKeyboardMarkup(bottonilista),
            disable_web_page_preview=True)
    if CallbackQuery.data == "assistenza":
        annulla = [[InlineKeyboardButton('Annulla ❌', callback_data="menu")]]
        CallbackQuery.edit_message_text(text="""
👉🏻 <b>Rispondi</b> trascinando verso sinistra questo messaggio, poi <b>digita</b> il messaggio per contattare </b>l'assistenza del bot</b>.

💡 La <b>richiesta</b> deve essere inviata in un <b>unico messaggio</b>, altri messaggi <b>non saranno recapitati</b>.

""",
                                        reply_markup=InlineKeyboardMarkup(annulla),
                                        disable_web_page_preview=False)

    # CallbackQuery.answer( "Digita il comando /contatta seguito dal testo che vuoi inviare all'assistenza 🖊", show_alert=True)

    if CallbackQuery.data == "listaregioni":

        jsonFile = open('cacheferrovie.json')

        data = json.load(jsonFile)
        annunci = len(data)

        listazone = []
        for i in data:
            listazone.append(i["zone"])

        regioni = []
        for valore in listazone:

            valoreffettivo = valore.split(',')
            lunghezza = len(valoreffettivo)

            if lunghezza > 1:

                if valoreffettivo[0] == "Italia ":
                    regione = valoreffettivo[1]
                    regioni.append(regione)

        regioniuniche = []
        for regione in regioni:
            if (regione not in regioniuniche):
                regioniuniche.append(regione)

        bottonilista = []
        listareg = 0
        pulsanti = []
        for regione in regioniuniche:

            regionelavoro = regione

            pulsanti.append(
                InlineKeyboardButton(regionelavoro + " ➡ ",
                                     callback_data=regionelavoro))

            listareg = listareg + 1
            quantita = len(pulsanti)
            if quantita == 2:
                bottonilista.append(pulsanti)

                pulsanti = []

        bottonilista.append(pulsanti)

        testoquery = f"Ci sono <b>{listareg}</b> regioni e <b>{annunci}</b> annunci.\n\nSelezionane una:"

        menu = [InlineKeyboardButton('Indietro ↩', callback_data="menu")]
        nazione = [InlineKeyboardButton('Italia 🇮🇹', callback_data="Italia")]

        bottonilista.append(nazione)

        bottonilista.append(menu)

        CallbackQuery.edit_message_text(
            testoquery, reply_markup=InlineKeyboardMarkup(bottonilista))

    if CallbackQuery.data == "listasettore":
        # httpx.AsyncClient(timeout=10)

        # risposta = httpx.get(DOMAIN + "jobs.php", cookies={'lang': 'it_IT'})
        # soupannunci = BeautifulSoup(risposta.text, "lxml")

        # settori = soupannunci.find("select", {"name": "sector"}).find_all("option")

        # annunci = soupannunci.find("span", {"class": "number"}).text.strip()

        jsonFile = open('cacheferrovie.json')

        data = json.load(jsonFile)
        annunci = len(data)
        settoriunici = []
        for i in data:
            if (i["sector"] not in settoriunici):
                settoriunici.append(i["sector"])

        bottonilista = []

        for settore in settoriunici:
            settorelavoro = settore

            pulsante = [
                InlineKeyboardButton(settorelavoro + " ➡ ",
                                     callback_data=settorelavoro)
            ]

            bottonilista.append(pulsante)

            listasett = len(bottonilista)

        testoquery = f"Ci sono <b>{listasett}</b> settori e <b>{annunci}</b> annunci.\n\nSelezionane uno:"

        menu = [InlineKeyboardButton('Indietro ↩', callback_data="menu")]

        bottonilista.append(menu)

        CallbackQuery.edit_message_text(
            testoquery, reply_markup=InlineKeyboardMarkup(bottonilista))

    elif CallbackQuery.data == "menu":
        CallbackQuery.edit_message_text(
            start_message,
            reply_markup=InlineKeyboardMarkup(start_message_buttons),
            disable_web_page_preview=False)

    # httpx.AsyncClient(timeout=10)

    # risposta = httpx.get(DOMAIN + "jobs.php", cookies={'lang': 'it_IT'})
    # soupannunci = BeautifulSoup(risposta.text, "lxml")

    # settori = soupannunci.find("select", {"name": "sector"}).find_all("option")

    jsonFile = open('cacheferrovie.json')

    data = json.load(jsonFile)

    settoriunici = []
    for i in data:
        if (i["sector"] not in settoriunici):
            settoriunici.append(i["sector"])

    listazone = []
    regioniuniche = []
    for i in data:
        listazone.append(i["zone"])

        regioni = []
        for valore in listazone:
            # valorenuovo = valore.replace(",", "")

            valoreffettivo = valore.split(',')

            lunghezza = len(valoreffettivo)
            if lunghezza == 1:
                nazione = valoreffettivo[0]
                regioni.append(nazione)
            if lunghezza > 1:

                if valoreffettivo[0] == "Italia ":
                    regione = valoreffettivo[1]
                    regioni.append(regione)

    for regione in regioni:
        if (regione not in regioniuniche):
            regioniuniche.append(regione)

    lis = list(CallbackQuery.data.split(" "))
    length = len(lis)
    pagina = lis[length - 1]
    ricercato = ""

    active = True

    for settore in settoriunici:
        if active == True:
            selezionato = settore
            if CallbackQuery.data == selezionato:

                ricercato = CallbackQuery.data
                callback = "listasettore"

                paginattuale = 1
                active = False
            elif CallbackQuery.data.rsplit(' ', 1)[0] == selezionato:

                ricercato = CallbackQuery.data.rsplit(' ', 1)[0]
                callback = "listasettore"

                paginattuale = int(pagina)
                active = False

    for regione in regioniuniche:
        if active == True:
            selezionato = regione
            if CallbackQuery.data == selezionato:

                callback = "listaregioni"
                ricercato = CallbackQuery.data

                paginattuale = 1
                active = False
            elif CallbackQuery.data.rsplit(' ', 1)[0] == selezionato:

                ricercato = CallbackQuery.data.rsplit(' ', 1)[0]
                callback = "listaregioni"

                paginattuale = int(pagina)
                active = False

    if ricercato != "":

        jsonFile = open('cacheferrovie.json')

        data = json.load(jsonFile)

        lista = []

        for i in data:

            info = [i['zone'], i['sector']]
            if any(ricercato in stringa for stringa in info):
                # if (ricercato in i['sector']):

                url = i['url']
                titolo = i['title']

                link = f"<a href={url}> {titolo} </a>"
                lista.append(link)

        jsonFile.close()
        totali = {len(lista)}

        prova = totali.pop()

        numeromassimo = int(prova / 10)

        if prova % 10 != 0:
            numeromassimo = numeromassimo + 1

        messaggio = f""" Questi sono i risultati per <b>{ricercato}</b>:\n
Totale: <b>{len(lista)}</b>.
Pagina {paginattuale}/{numeromassimo}\n
"""

        if numeromassimo == 1:

            for i in range(0, prova):
                link = lista[i]

                messaggio = messaggio + "➤ " + link + "| 🔗 \n"

        elif numeromassimo > 1:

            if paginattuale == numeromassimo:

                numero = int(prova / 10) * 10

                if prova % 10 == 0:
                    calcolo = int(prova / 10 - 1)
                    numero = calcolo * 10
                for i in range(numero, prova):
                    link = lista[i]

                    messaggio = messaggio + "➤ " + link + "| 🔗 \n"

            if paginattuale < numeromassimo and paginattuale > 1:

                minimo = (paginattuale - 1) * 10
                massimo = paginattuale * 10

                for i in range(minimo, massimo):
                    link = lista[i]

                    messaggio = messaggio + "➤ " + link + "| 🔗 \n"

            if paginattuale == 1:
                for i in range(0, 10):
                    link = lista[i]

                    messaggio = messaggio + "➤ " + link + "| 🔗 \n"

        testosettore = messaggio

        if numeromassimo == 1:
            bottoneinfo = [[
                InlineKeyboardButton('Indietro ↩', callback_data=callback)
            ]]
        elif numeromassimo > 1:

            if paginattuale == numeromassimo:
                bottoneinfo = [[
                    InlineKeyboardButton('◀ Pagina precedente',
                                         callback_data=ricercato + " " +
                                                       str(paginattuale - 1)),
                ], [InlineKeyboardButton('Indietro ↩', callback_data=callback)]]

            if paginattuale > 1 and paginattuale < numeromassimo:
                bottoneinfo = [[
                    InlineKeyboardButton('◀ Pagina precedente',
                                         callback_data=ricercato + " " +
                                                       str(paginattuale - 1)),
                    InlineKeyboardButton('Pagina successiva ▶',
                                         callback_data=ricercato + " " +
                                                       str(paginattuale + 1))
                ], [InlineKeyboardButton('Indietro ↩', callback_data=callback)]]

            if paginattuale == 1:
                bottoneinfo = [[
                    InlineKeyboardButton('Pagina successiva ▶',
                                         callback_data=ricercato + " " +
                                                       str(paginattuale + 1))
                ], [InlineKeyboardButton('Indietro ↩', callback_data=callback)]]

        CallbackQuery.edit_message_text(
            testosettore,
            reply_markup=InlineKeyboardMarkup(bottoneinfo),
            disable_web_page_preview=True)


@telegram.on_message(filters.private)
def elimina(telegram, message):
    telegram.delete_messages(message.chat.id, message.id)


async def scraping():
    print("entrato scraping")

    driver = webdriver.Chrome(options)

    driver.get(DOMAIN + "jobs.php")
    driver.implicitly_wait(10)

    driver.add_cookie({'name': 'lang', 'value': 'it_IT'})
    driver.refresh()

    html = driver.page_source

    print("dopo aver aspettato scraping")

    soup = BeautifulSoup(html, "lxml")

    results = soup.find("div", {
        "class": "searchResultsBody"
    }).find_all("div", {"class": "singleResult responsiveOnly"})

    annuncijson = open(CACHE_FILE)
    CACHE: list = json.load(annuncijson)

    for result in results:
        data = {}
        details = result.find("div", {"class": "details"})
        linkStrip = DOMAIN + details.find("a")["href"]

        linkFixed = url_normalize(linkStrip)

        data["url"] = linkFixed
        print(data["url"])
        data["title"] = details.find("h3").text.strip()

        lista_posizione = [
            span.text.strip().title() for span in details.find_next(
                string="Sede:").find_next("td").find_next("span").find_all("span")
            if span.text.strip()
        ]

        data["zone"] = ' , '.join(lista_posizione)

        driver.get(data['url'])
        driver.implicitly_wait(10)
        driver.add_cookie({'name': 'lang', 'value': 'it_IT'})
        driver.refresh()
        responseannuncio = driver.page_source

        soupannuncio = BeautifulSoup(responseannuncio, 'lxml')

        try:

            descrizione = soupannuncio.find("div", {
                "itemprop": "description"
            }).text.strip()
        except:

            descrizione = soupannuncio.find("div", {
                "class": "locationList"
            }).text.strip()
        data["descrizione"] = descrizione

        data["sector"] = details.find_next(string="Settore:").find_next("span").text
        data["role"] = details.find_next(string="Ruolo:").find_next("span").text
        data["day"], data["month"], data["year"] = map(
            int,
            details.find("span", {
                "class": "date"
            }).text.split("/"))

        if any(data['url'] == i['url'] for i in CACHE):
            for i in CACHE:
                if data['url'] == i['url']:
                    if data["day"] != i["day"] or data["month"] != i["month"] or data[
                        "year"] != i["year"]:
                        i["day"] = data["day"]
                        i["title"] = data["title"]
                        i["zone"] = data["zone"]
                        i["sector"] = data["sector"]
                        i["role"] = data["role"]
                        i["month"] = data["month"]
                        i["year"] = data["year"]
                        i["descrizione"] = data["descrizione"]
                        f = open('cacheferrovie.json', 'w', encoding='utf-8')

                        f.write(json.dumps(CACHE, indent=4))

                        aggiornobuttons = [[
                            InlineKeyboardButton('Visualizza messaggio 🔗',
                                                 url=f"{i['linkmessaggio']}"),
                            InlineKeyboardButton('Guadagna 💰',
                                                 url="https://t.me/concorsiferrovie/1430")
                        ]]

                        markupaggiornato = InlineKeyboardMarkup(aggiornobuttons)

                        await telegram.send_message(chat_id=CHAT_ID,
                                                    text=f"""📣 <b>Annuncio aggiornato!</b>

🔗 <a href='{i['url']}'>{i['title']}</a>

📅 __Data aggiornata: {i['day']}/{i['month']}/{i['year']}__""",
                                                    reply_markup=markupaggiornato,
                                                    reply_to_message_id=i["idmessaggio"],
                                                    disable_web_page_preview=True)

            continue

        telegraph.create_account(short_name='Ferrovie')
        response = telegraph.create_page(f'{data["title"]}',
                                         html_content=f'<p>{descrizione}</p>')
        linktelegraph = response['url']
        CACHE.append(data)

        riga3 = [
            InlineKeyboardButton('Guadagna 💰',
                                 url="https://t.me/concorsiferrovie/1430")
            # InlineKeyboardButton('Cerca 🔍', url="t.me/concorsiferroviebot?start=cerca")
        ]

        annunciobuttons = [[
            InlineKeyboardButton(
                'Condividi il canale ❗',
                url=
                "https://telegram.me/share/url?url=https://telegram.me/concorsiferrovie&text=Unisciti%20per%20ricevere%20notifiche%20sulle%20nuove%20posizioni%20disponibili%20sul%20sito%20delle%20Ferrovie%20Dello%20Stato%20"
            ),
            InlineKeyboardButton('Gruppo discussione 🗣',
                                 url="t.me/selezioniconcorsiferrovie")
        ], [InlineKeyboardButton('Descrizione 📃', url=f"{linktelegraph}")], riga3]

        markupannuncio = InlineKeyboardMarkup(annunciobuttons)

        inviato = await telegram.send_message(CHAT_ID,
                                              MESSAGE.format(**data),
                                              reply_markup=markupannuncio,
                                              disable_web_page_preview=True)

        linkmessaggio = inviato.link
        data['linkmessaggio'] = linkmessaggio
        titolo = data["title"]
        replaced = titolo.replace(' ', '+')
        linkwhatsapp = url_normalize(
            "https://api.whatsapp.com/send?text=Guarda+questo+annuncio+di+lavoro+delle+Ferrovie+Dello+Stato:+"
            + replaced + "+" + linkmessaggio)

        whatsapp = [
            InlineKeyboardButton("Condividi su WhatsApp 📱", url=linkwhatsapp)
        ]

        idmessaggio = inviato.id
        data["idmessaggio"] = idmessaggio
        stringid = str(idmessaggio)
        preferiti = InlineKeyboardButton(
            'Aggiungi ai preferiti 🏷',
            url="t.me/concorsiferroviebot?start=like_" + stringid + "")
        riga3.append(preferiti)
        annunciobuttons.append(whatsapp)

        await telegram.edit_message_reply_markup(
            CHAT_ID, idmessaggio, InlineKeyboardMarkup(annunciobuttons))
        print("edit scraping")

        with open(CACHE_FILE, "w+", encoding="utf-8") as f:
            f.write(json.dumps(CACHE, indent=4))
            print("fine scraping")
            f.close()

        file = open("utenti.json")

        listautenti = json.load(file)

        for utente in listautenti:
            invia = 0
            if utente.get("tiponot") != None:
                if "Nuovo" in utente["tiponot"]:
                    if utente.get("settore") != None:

                        if data["sector"] in utente["settore"]:
                            invia = 1

                    lunghezzalista = len(lista_posizione)
                    if lunghezzalista > 1:
                        if lista_posizione[1] in utente["regione"]:
                            invia = 1

                    if invia == 1:

                        try:
                            await telegram.forward_messages(chat_id=utente["iduser"],
                                                            from_chat_id=CHAT_ID,
                                                            message_ids=idmessaggio)

                            print("Già inviato")
                        except:
                            print("non inviato perchè bloccato")

    driver.quit()


async def clean():
    print("entrato pulizia")

    file = 'cacheferrovie.json'

    jsonFile = open('cacheferrovie.json')
    print("dopo aver aspettato")
    data = json.load(jsonFile)

    for idx, oggetto in enumerate(data):

        url = oggetto['url']

        httpx.AsyncClient(timeout=10)
        response = httpx.get(url, cookies={'lang': 'it_IT'})
        soup = BeautifulSoup(response.text, "lxml")

        try:

            stato = soup.find("div", {"class": "searchTitle"}).text.strip()

            now = datetime.now()
            date = now.strftime("%d/%m/%Y")
            eliminatobuttons = [[
                InlineKeyboardButton('Visualizza messaggio 🔗',
                                     url=f"{oggetto['linkmessaggio']}")
            ]]

            markupeliminato = InlineKeyboardMarkup(eliminatobuttons)

            # await telegram.send_message(chat_id=CHAT_ID,
            #          text=f"""❌ <b>Annuncio scaduto!</b>

            # 🔗 <a href='{oggetto['url']}'>{oggetto['title']}</a>

            # 📅 __Data scadenza: {date}__""",
            #   reply_markup=markupeliminato,
            #  reply_to_message_id=oggetto["idmessaggio"],
            #  disable_web_page_preview=True)

            print("eliminato")
            data.pop(idx)

        except:
            print("andato")

    aggiornato = data
    with open('cacheferrovie.json', 'w', encoding='utf-8') as f:

        f.write(json.dumps(aggiornato, indent=4))
        f.close()


telegram.start()

scheduler.add_job(clean,
                  "interval",
                  hours=12,
                  next_run_time=datetime.now() + timedelta(seconds=2))
scheduler.add_job(scraping,
                  "interval",
                  minutes=5,
                  next_run_time=datetime.now() + timedelta(seconds=55))

scheduler.start()

keep_alive()

idle()
