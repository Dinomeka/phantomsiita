
import sqlite3
import re
import os
import logging
import asyncio
import requests
import html

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.request import HTTPXRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

# Токен и путь к базе
TOKEN = os.environ.get('BOT_TOKEN')
DB_PATH = os.environ.get('DB_PATH', 'ps.db')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ===================== ФУНКЦИИ СКАЧИВАНИЯ =====================

async def download_google_file(url: str, ext: str, dest_folder: str = "media") -> str:
    if not url or "drive.google.com" not in url: return None
    try:
        file_id = url.split("/d/")[1].split("/")[0]
    except: return None
    
    dest_path = os.path.join(dest_folder, f"{file_id}.{ext}")
    if os.path.exists(dest_path): return dest_path
    os.makedirs(dest_folder, exist_ok=True)

    def _download():
        headers = {'User-Agent': 'Mozilla/5.0'}
        d_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        with requests.get(d_url, stream=True, headers=headers) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(32768): f.write(chunk)
    
    await asyncio.to_thread(_download)
    return dest_path

# ===================== ФУНКЦИИ БД (СОРТИРОВКА ПО ID) =====================

def db_query(query, params=()):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchall()
    conn.close()
    return result

def get_original_songs():
    return db_query("SELECT id, NameJp, NameRom FROM songs WHERE CoverOrOriginal = 'original' ORDER BY id ASC")

def get_cover_songs():
    return db_query("SELECT id, NameJp, NameRom FROM songs WHERE CoverOrOriginal = 'cover' ORDER BY id ASC")

def get_events():
    return db_query("SELECT id, NameJp, NameRom FROM events ORDER BY id ASC")

def get_song(song_id, cover_type):
    res = db_query("""
        SELECT NameJp, NameRom, CoverOrOriginal,
               NameEn, TranslationName, Data, DataMv, Album, NumAlbum, Other, CoverArtist, Center
        FROM songs WHERE id = ? AND CoverOrOriginal = ?""", (song_id, cover_type))
    return res[0] if res else None

def get_song_events(song_id, cover_type):
    return db_query("""
        SELECT e.NameJp, e.NameRom 
        FROM performances p
        JOIN events e ON e.id = p.event_id
        WHERE p.song_id = ? AND p.CoverOrOriginal = ?
        GROUP BY e.id ORDER BY e.id ASC""", (song_id, cover_type))

def get_original_files(song_id, cover_type):
    return db_query("SELECT id, Name, LinkGoogle FROM original_files WHERE song_id = ? AND CoverOrOriginal = ?", (song_id, cover_type))


def get_event(event_id):
    res = db_query("""
        SELECT NameJp, NameRom, Data, Setlist, Posts, Other, Link
        FROM events
        WHERE id = ?
    """, (event_id,))

    return res[0] if res else None

def get_event_songs(event_id):
    return db_query("""
        SELECT DISTINCT s.id, s.NameJp, s.NameRom, s.CoverOrOriginal 
        FROM performances p 
        JOIN songs s ON s.id = p.song_id AND s.CoverOrOriginal = p.CoverOrOriginal
        WHERE p.event_id = ? ORDER BY s.id ASC""", (event_id,))

def get_performance_files(song_id, event_id, cover_type):
    return db_query("SELECT id, format, LinkGoogle, Link FROM performances WHERE song_id = ? AND event_id = ? AND CoverOrOriginal = ?", (song_id, event_id, cover_type))

def search_song_by_name(text):
    res = db_query("SELECT id, CoverOrOriginal FROM songs WHERE NameJp LIKE ? OR NameRom LIKE ? LIMIT 1", ('%' + text + '%', '%' + text + '%'))
    return res[0] if res else None

def search_event_by_name(text):
    res = db_query("SELECT id FROM events WHERE NameJp LIKE ? OR NameRom LIKE ? LIMIT 1", ('%' + text + '%', '%' + text + '%'))
    return res[0][0] if res else None

def format_name(n1, n2): return f"{n1} ({n2})" if n2 and n2 != "-" else n1

# ===================== ОБРАБОТЧИКИ ТЕКСТА =====================

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw_input = update.message.text.strip()
    
    # 1. Поиск по кодам (O[ID], C[ID], 0[ID]) - теперь ищет напрямую по ID из базы
    match = re.fullmatch(r"([OoCc0ОоСс])\s*(\d+)", raw_input)
    if match:
        prefix = match.group(1).upper()
        sid = match.group(2)
        if prefix in ["O", "О", "0"]: # Оригиналы
            return await show_song(update, context, sid, "original")
        elif prefix in ["C", "С"]: # Каверы
            return await show_song(update, context, sid, "cover")

    # 2. Поиск по числу (ID выступления)
    if raw_input.isdigit():
        return await show_event(update, context, raw_input)

    # 3. Поиск по названию песни
    found_song = search_song_by_name(raw_input)
    if found_song:
        return await show_song(update, context, found_song[0], found_song[1])

    # 4. Поиск по названию события
    found_event = search_event_by_name(raw_input)
    if found_event:
        return await show_event(update, context, found_event)

    await update.message.reply_text("Ничего не найдено. Уточните название или номер ID.")

# ===================== ЭКРАНЫ ВЫВОДА =====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [[InlineKeyboardButton("🎵 Записи", callback_data="records")], [InlineKeyboardButton("🎤 Выступления", callback_data="events")]]
    await update.message.reply_text("Выберите раздел или введите ID/название:", reply_markup=InlineKeyboardMarkup(kb))

async def show_records(update, context):
    originals = get_original_songs()
    covers = get_cover_songs()

    text = "🎵 <b>Оригинальные:</b>\n"
    text += "\n".join([f"O{s[0]}. {format_name(s[1], s[2])}" for s in originals])
    text += "\n\n🎵 <b>Каверы:</b>\n"
    text += "\n".join([f"C{s[0]}. {format_name(s[1], s[2])}" for s in covers])
    
    msg = update.callback_query.message if update.callback_query else update.message
    if update.callback_query: await msg.edit_text(text, parse_mode="HTML")
    else: await msg.reply_text(text, parse_mode="HTML")


async def show_events(update, context):
    events = get_events()
    text = "🎤 <b>Выступления:</b>\n\n"
    SOLO_ID = 29
    text += "\n".join([
        f"{e[0]}. <u>{format_name(e[1], e[2])}</u>" if e[0] == SOLO_ID
        else f"{e[0]}. {format_name(e[1], e[2])}"
        for e in events
    ])
    msg = update.callback_query.message if update.callback_query else update.message
    if update.callback_query:
        await msg.edit_text(text, parse_mode="HTML")
    else:
        await msg.reply_text(text, parse_mode="HTML")

async def show_song(update, context, song_id, cover_type):
    song = get_song(song_id, cover_type)
    if not song:
        msg = update.message if update.message else update.callback_query.message
        return await msg.reply_text(f"Песня с ID {song_id} ({cover_type}) не найдена.")

    (NameJp, NameRom, CoverOrOriginal,
     NameEn, TranslationName, Data, DataMv, Album, NumAlbum, Other, CoverArtist, Center) = song

    title = format_name(NameJp, NameRom)
    text = f"🎵 <b>{html.escape(title)}</b>\n"

    # --- Названия ---
    if (NameEn and NameEn != "-") or (TranslationName and TranslationName != "-"):
        text += "\n🌐 <b>Названия:</b>\n"
        if NameEn and NameEn != "-":
            text += f"• English: {html.escape(NameEn)}\n"
        if TranslationName and TranslationName != "-":
            text += f"• Русский: {html.escape(TranslationName)}\n"

    # --- Тип ---
    if CoverOrOriginal == "cover":
        text += f"\n🎭 <b>Тип:</b> Кавер на "
        if CoverArtist and CoverArtist != "-":
            text += html.escape(CoverArtist)
        text += "\n"
    else:
        text += "\n🎭 <b>Тип:</b> Оригинал\n"

    # --- Релиз ---
    if Album != "-" or Data != "-":
        text += "\n💿 <b>Релиз:</b>\n"
        if Album and Album != "-":
            if NumAlbum and NumAlbum != "-":
                text += f"• Альбом: {html.escape(Album)} (№{NumAlbum})\n"
            else:
                text += f"• Альбом: {html.escape(Album)}\n"
        if Data and Data != "-":
            text += f"• Дата выхода: {html.escape(Data)}\n"
        if DataMv and DataMv != "-":
            text += f"• Дата MV: {html.escape(DataMv)}\n"

    if Center and Center != "-":
        text += f"\nЦентр: {html.escape(Center)}\n"

    # ===================== ССЫЛКИ ИЗ ORIGINAL_FILES =====================

    files = db_query("""
        SELECT Name, Link 
        FROM original_files 
        WHERE song_id = ? AND CoverOrOriginal = ?
        ORDER BY id ASC
    """, (song_id, cover_type))

    mv_link = None
    audio_link = None
    other_links = []

    for name, link in files:
        if not link or link.strip() in ["", "-"]:
            continue

        name_clean = name.strip()

        if name_clean.upper() == "OFFICIAL MUSIC VIDEO":
            mv_link = link

        elif name_clean.upper() == "OFFICIAL MUSIC AUDIO":
            audio_link = link

        elif name_clean.lower() == "dance practice (video)":
            other_links.append(("Dance Practice", link))

        else:
            other_links.append((name_clean, link))

    # --- MV и Audio (как раньше, просто из новой таблицы) ---
    if mv_link:
        text += f"\n▶ <a href='{mv_link}'>Music Video</a>\n"
    if audio_link:
        text += f"🎧 <a href='{audio_link}'>Audio</a>\n"

    # --- Остальные ссылки ---
    if other_links:
        text += "\n🔗 <b>Ссылки:</b>\n"
        for name, link in other_links:
            text += f"• <a href=\"{link}\">{html.escape(name)}</a>\n"

    # --- Выступления (сворачиваемые) ---
    events = get_song_events(song_id, cover_type)
    if events:
        ev_list = "\n".join([f"• {format_name(e[0], e[1])}" for e in events])
        text += f"\n🎤 <b>Выступления:</b>\n<blockquote expandable>{html.escape(ev_list)}</blockquote>"

    # --- Credits (сворачиваемые) ---
    if Other and Other != "-":
        text += f"\n📄 <b>Credits:</b>\n<blockquote expandable>{html.escape(Other)}</blockquote>"

    files = get_original_files(song_id, cover_type)
    kb = [[InlineKeyboardButton(f[1], callback_data=f"orig_{f[0]}")] for f in files]

    msg = update.message if update.message else update.callback_query.message
    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(kb) if kb else None, parse_mode="HTML", disable_web_page_preview=True)

async def show_event(update, context, event_id):
    event = get_event(event_id)
    if not event:
        msg = update.message if update.message else update.callback_query.message
        return await msg.reply_text(f"Выступление с ID {event_id} не найдено.")

    (NameJp, NameRom, Data, Setlist, Posts, Other, Link) = event

    title = format_name(NameJp, NameRom)
    text = f"🎤 <b>{html.escape(title)}</b>\n"

    if Data and Data != "-":
        text += f"📅 {html.escape(Data)}\n"

    # --- О выступлении / Other + Links ---
    if Other and Other != "-":
        text += f"\n📝 <b>О выступлении:</b>\n"
        text += f"<blockquote expandable>"

        lines = Other.strip().split("\n")

        for line in lines:
            if ":" in line:
                name, link = line.split(":", 1)
                name = name.strip()
                link = link.strip()

                text += f"<a href=\"{link}\">{html.escape(name)}</a>\n"
            else:
                text += f"{html.escape(line)}\n"

        text += f"</blockquote>"

    # --- Сетлист ---
    if Setlist and Setlist != "-":
        text += f"\n🎶 <b>Сетлист:</b>\n"
        text += f"<blockquote expandable>"
        text += f"{html.escape(Setlist)}\n"
        text += f"</blockquote>"

    # --- Посты ---
    if Posts and Posts != "-":
        text += "\n💬 <b>Посты:</b>\n"
        text += "<blockquote expandable>"

        lines = Posts.strip().split("\n")

        for line in lines:
            line = line.strip()

            if ":" in line:
                name, link = line.split(":", 1)
                name = name.replace("—", "").strip()
                link = link.strip()

                text += f"— <a href=\"{link}\">{html.escape(name)}</a>\n"
            else:
                text += f"{html.escape(line)}\n"

        text += "</blockquote>\n"

    # --- Кнопки ---
    kb = []

    if Link and str(Link).strip() not in ["", "-"]:
        kb.append([InlineKeyboardButton("🔗 Смотреть целиком", url=Link)])

    songs = get_event_songs(event_id)
    for s in songs:
        t_code = 'o' if s[3] == 'original' else 'c'
        kb.append([
            InlineKeyboardButton(
                format_name(s[1], s[2]),
                callback_data=f"perf_{t_code}_{s[0]}_{event_id}"
            )
        ])

    msg = update.message if update.message else update.callback_query.message
    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

# ===================== ОБРАБОТКА КНОПОК И ФАЙЛОВ =====================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "records": await show_records(update, context)
    elif data == "events": await show_events(update, context)
    
    elif data.startswith("orig_"):
        fid = data.split("_")[1]
        res = db_query("SELECT LinkGoogle, format FROM original_files WHERE id = ?", (fid,))
        if res: await process_file(query, res[0][0], res[0][1], context)
        
    elif data.startswith("pf_"):
        fid = data.split("_")[1]
        res = db_query("SELECT LinkGoogle, format FROM performances WHERE id = ?", (fid,))
        if res: await process_file(query, res[0][0], res[0][1], context)

    elif data.startswith("perf_"):
        parts = data.split("_")
        t_code, s_id, e_id = parts[1], parts[2], parts[3]
        c_type = 'original' if t_code == 'o' else 'cover'
        
        files = get_performance_files(s_id, e_id, c_type)
        kb = []
        yt_link = None
        for f in files:
            label = "🎧 MP3" if f[1].lower() == "mp3" else "📹 MP4"
            kb.append([InlineKeyboardButton(label, callback_data=f"pf_{f[0]}")])
            if f[3] and str(f[3]).strip() not in ["", "-"]: yt_link = f[3]

        if yt_link:
            kb.insert(0, [InlineKeyboardButton("▶ Смотреть выступление", url=yt_link)])

        await query.edit_message_text(f"Выберите формат:", reply_markup=InlineKeyboardMarkup(kb))

async def process_file(query, link, fmt, context):
    fmt = (fmt or "mp4").lower().strip()
    status = await query.message.reply_text("⏳ Загрузка и отправка...")
    try:
        path = await download_google_file(link, fmt)
        if path and os.path.exists(path):
            with open(path, "rb") as f:
                try:
                    if fmt == "mp3":
                        await context.bot.send_audio(chat_id=query.message.chat_id, audio=f, write_timeout=300)
                    else:
                        await context.bot.send_video(chat_id=query.message.chat_id, video=f, write_timeout=300)
                except Exception as net_err:
                    logger.warning(f"Сетевая ошибка при отправке (файл мог дойти): {net_err}")
            os.remove(path)
            await status.delete()
        else: await status.edit_text("❌ Ошибка: файл не скачан.")
    except Exception as e:
        logger.error(f"Global process_file error: {e}")
        await status.edit_text(f"❌ Ошибка: {e}")

# ===================== ЗАПУСК И ОШИБКИ =====================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if "httpx" in str(context.error) or "NetworkError" in str(context.error):
        return # Игнорируем сетевые тайм-ауты в консоли
    logger.error(f"Ошибка: {context.error}", exc_info=context.error)

from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"OK")

def run_healthcheck_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()


# ... (весь ваш предыдущий код с функциями и обработчиками остается без изменений) ...

def main():
    # Настройка таймаутов для работы с большими файлами
    req = HTTPXRequest(connect_timeout=60.0, read_timeout=300.0, write_timeout=300.0)
    app = Application.builder().token(TOKEN).request(req).build()
    
    app.add_error_handler(error_handler)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # Настройки для Render
    # Render автоматически передает PORT. Если его нет, используем 8000
    PORT = int(os.environ.get("PORT", 8000))
    # URL вашего приложения на Render (например, https://my-bot.onrender.com)
    WEBHOOK_URL = os.environ.get('WEBHOOK_URL')

    if WEBHOOK_URL:
        logger.info(f"Запуск в режиме WEBHOOK на порту {PORT}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN, # Скрытый путь (используем токен для безопасности)
            webhook_url=f"{WEBHOOK_URL}/{TOKEN}"
        )
    else:
        logger.info("Запуск в режиме POLLING")
        app.run_polling()

if __name__ == "__main__":
    main()
