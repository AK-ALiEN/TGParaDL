"""
TGParaDL - High-Performance Telegram Parallel Downloader
--------------------------------------------------------
A Telegram bot script utilizing Hydrogram to download files using multi-DC 
parallel chunking for maximum speed. Features include a queuing system, 
graceful cancellation, and basic file management.

Author: Alien
License: MIT License
"""

import asyncio
import time
import os
import configparser
import shutil
import math
import re
from datetime import datetime
import aiofiles

from hydrogram import Client, filters
from hydrogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from hydrogram.enums import ParseMode
from hydrogram.errors import FloodWait, MessageNotModified

CONFIG_FILE = "settings.ini"

def load_config():
    """
    Loads configuration from 'settings.ini'.
    If the file doesn't exist, prompts the user via CLI to generate it.
    Returns a dictionary of validated configuration values.
    """
    config_parser = configparser.ConfigParser()
    
    default_config = {
        "API_ID": "0",
        "API_HASH": "",
        "BOT_TOKEN": "",
        "DOWNLOAD_DIR": "./downloads/",
        "ALLOWED_USERS": "",
        "MAX_CONCURRENT_DOWNLOADS": "1",
        "CHUNK_WORKERS": "4"
    }
    
    if os.path.exists(CONFIG_FILE):
        config_parser.read(CONFIG_FILE)
        if not config_parser.has_section("Settings"):
            config_parser.add_section("Settings")
            
        updated = False
        for k, v in default_config.items():
            if not config_parser.has_option("Settings", k):
                config_parser.set("Settings", k, v)
                updated = True
                
        if updated:
            with open(CONFIG_FILE, "w") as f:
                config_parser.write(f)
    else:
        print("Configuration file not found. Please provide the required credentials:")
        config_parser.add_section("Settings")
        config_parser.set("Settings", "API_ID", input("Enter API_ID: ") or "0")
        config_parser.set("Settings", "API_HASH", input("Enter API_HASH: "))
        config_parser.set("Settings", "BOT_TOKEN", input("Enter BOT_TOKEN: "))
        config_parser.set("Settings", "DOWNLOAD_DIR", input("Enter DOWNLOAD_DIR (default './downloads/'): ") or "./downloads/")
        config_parser.set("Settings", "ALLOWED_USERS", "")
        config_parser.set("Settings", "MAX_CONCURRENT_DOWNLOADS", "3")
        config_parser.set("Settings", "CHUNK_WORKERS", "4")
        
        with open(CONFIG_FILE, "w") as f:
            config_parser.write(f)
        print(f"Configuration successfully saved to {CONFIG_FILE}\n")

    settings = config_parser["Settings"]
    
    # Parse allowed users into a list of integers
    users_str = settings.get("ALLOWED_USERS", "")
    allowed_users = [int(x.strip()) for x in users_str.split(",") if x.strip().isdigit()]

    return {
        "API_ID": int(settings.get("API_ID", "0")),
        "API_HASH": settings.get("API_HASH", ""),
        "BOT_TOKEN": settings.get("BOT_TOKEN", ""),
        "DOWNLOAD_DIR": settings.get("DOWNLOAD_DIR", "./downloads/"),
        "ALLOWED_USERS": allowed_users,
        "MAX_CONCURRENT_DOWNLOADS": int(settings.get("MAX_CONCURRENT_DOWNLOADS", "3")),
        "CHUNK_WORKERS": int(settings.get("CHUNK_WORKERS", "4"))
    }

# Initialize configurations
config = load_config()
DOWNLOAD_DIR = config["DOWNLOAD_DIR"].rstrip('/') + '/'
if not os.path.exists(DOWNLOAD_DIR): 
    os.makedirs(DOWNLOAD_DIR)

# Initialize Hydrogram Client
app = Client(
    "TGParaDL",
    api_id=config["API_ID"],
    api_hash=config["API_HASH"],
    bot_token=config["BOT_TOKEN"],
    max_concurrent_transmissions=100, 
    workers=100,
    sleep_threshold=60,
    parse_mode=ParseMode.MARKDOWN
)

# Globals for task management
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(config.get("MAX_CONCURRENT_DOWNLOADS", 3))
cancel_events = {}

async def check_auth(_, __, message: Message):
    """Filter to ensure only authorized users can interact with the bot."""
    users = config.get("ALLOWED_USERS", [])
    if not users: 
        return True
    return message.from_user.id in users

is_admin = filters.create(check_auth)

def sanitize_filename(name):
    """Removes invalid characters from filenames to prevent OS errors."""
    return re.sub(r'[\\/*?:"<>|]', "", name)

def get_main_keyboard():
    """Returns the main interactive reply keyboard."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("📁 View Files"), KeyboardButton("🗑️ Delete Files")],
            [KeyboardButton("📦 Move Files"), KeyboardButton("❌ Delete All")],
            [KeyboardButton("ℹ️ Status")]
        ], resize_keyboard=True
    )

def human_size(size):
    """Converts bytes to a human-readable string format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0: 
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

# ================= COMMANDS =================

@app.on_message(filters.command("start") & is_admin)
async def start_command(client, message: Message):
    """Handler for the /start command."""
    welcome_text = (
        "**🚀 TGParaDL - High-Performance Downloader**\n\n"
        "Welcome to the multi-DC parallel chunk downloading agent.\n"
        "Please transmit any file to initiate the download sequence.\n\n"
        "**Features:** Task Queuing | Graceful Cancellation | Auto-Sanitization\n"
        "Use the interactive menu below to manage your storage ecosystem."
    )
    await message.reply(welcome_text, reply_markup=get_main_keyboard())

@app.on_message(filters.command("help") & is_admin)
async def help_command(client, message: Message):
    """Handler for the /help command."""
    help_text = (
        "**🛠 TGParaDL Command Guide:**\n\n"
        "**Available Commands:**\n"
        "• `/start` - Initialize bot and display the main menu.\n"
        "• `/help` - Show this system manual.\n"
        "• `/status` - View real-time storage and system diagnostics.\n"
        "• `/ping` - Check system response latency.\n\n"
        "**How to use:**\n"
        "Simply forward or upload any document, video, or audio file here. "
        "The system will automatically queue it and initiate a multi-DC parallel download."
    )
    await message.reply(help_text, reply_markup=get_main_keyboard())

@app.on_message(filters.command("status") & is_admin)
async def status_command(client, message: Message):
    """Handler for the /status command."""
    await show_status(client, message)

@app.on_message(filters.command("ping") & is_admin)
async def ping_command(client, message: Message):
    """Handler for the /ping command to measure latency."""
    start_t = time.time()
    msg = await message.reply("🔄 **Pinging system...**")
    end_t = time.time()
    await msg.edit(f"🏓 **Pong!**\n**Latency:** `{round((end_t - start_t) * 1000)}ms`")

# ================= BUTTON HANDLER =================

@app.on_message(filters.text & ~filters.command(["start", "help", "status", "ping"]) & is_admin)
async def handle_menu_buttons(client, message: Message):
    """Routes text messages from the custom keyboard to their respective functions."""
    text = message.text
    if text == "📁 View Files": 
        await view_files(client, message)
    elif text == "🗑️ Delete Files": 
        await delete_files_only(client, message)
    elif text == "📦 Move Files": 
        await move_files(client, message)
    elif text == "ℹ️ Status": 
        await show_status(client, message)
    elif text == "❌ Delete All": 
        await confirm_delete_all(client, message)
    elif text == "✅ Confirm Deletion": 
        await delete_all_items(client, message)
    elif text == "🔙 Cancel Action": 
        await message.reply("✅ Action cancelled. Returning to main menu.", reply_markup=get_main_keyboard())

# ================= CORE FUNCTIONS =================

async def confirm_delete_all(client, message: Message):
    """Requests confirmation before purging all storage data."""
    keyboard = ReplyKeyboardMarkup([["✅ Confirm Deletion"], ["🔙 Cancel Action"]], resize_keyboard=True)
    await message.reply(
        "⚠️ **CRITICAL WARNING:**\n"
        "This operation will permanently erase **all files and directories** in the storage path.\n\n"
        "Proceed with full deletion?", 
        reply_markup=keyboard
    )

async def view_files(client, message: Message):
    """Lists files and directories currently present in the download path."""
    try:
        def fetch_files():
            items = os.listdir(DOWNLOAD_DIR)
            flist, fdlist = [], []
            for item in items:
                # Ignore hidden files and active session files
                if item.startswith('.') or 'TGParaDL' in item: continue
                p = os.path.join(DOWNLOAD_DIR, item)
                if os.path.isfile(p): 
                    flist.append(f"📄 `{item}` ({human_size(os.path.getsize(p))})")
                elif os.path.isdir(p): 
                    fdlist.append(f"📁 `{item}` ({len(os.listdir(p))} items)")
            return flist, fdlist
        
        # Run synchronous OS operations in a thread pool
        file_list, folder_list = await asyncio.to_thread(fetch_files)
        
        res = "**📁 Storage Overview:**\n\n"
        if file_list: 
            res += "**Files:**\n" + "\n".join(file_list[:20])
        if folder_list: 
            res += "\n\n**Directories:**\n" + "\n".join(folder_list[:10])
        
        if len(file_list) > 20 or len(folder_list) > 10:
            res += "\n\n*(Displaying limited results to prevent message overflow)*"
            
        await message.reply(res[:4000] if file_list or folder_list else "📂 **Storage is currently empty.**")
    except Exception as e: 
        await message.reply(f"❌ **System Error:** `{e}`")

async def delete_files_only(client, message: Message):
    """Deletes all files in the root download directory, preserving folders."""
    def del_job():
        cnt = 0
        for i in os.listdir(DOWNLOAD_DIR):
            p = os.path.join(DOWNLOAD_DIR, i)
            if os.path.isfile(p) and not i.startswith('.'):
                try:
                    os.remove(p)
                    cnt += 1
                except: pass
        return cnt
        
    count = await asyncio.to_thread(del_job)
    await message.reply(f"✅ **Storage Cleanup Complete:** Removed `{count}` file(s).", reply_markup=get_main_keyboard())

async def delete_all_items(client, message: Message):
    """Purges all files and directories recursively from the download path."""
    def del_all_job():
        cnt = 0
        for item in os.listdir(DOWNLOAD_DIR):
            if 'TGParaDL' in item or item.startswith('.'): continue
            p = os.path.join(DOWNLOAD_DIR, item)
            try:
                if os.path.isfile(p): 
                    os.remove(p)
                elif os.path.isdir(p): 
                    shutil.rmtree(p)
                cnt += 1
            except: pass
        return cnt
        
    count = await asyncio.to_thread(del_all_job)
    await message.reply(f"✅ **Full Wipe Successful:** Purged `{count}` item(s), including directories.", reply_markup=get_main_keyboard())

async def move_files(client, message: Message):
    """Archives current files into a timestamped directory."""
    target = os.path.join(DOWNLOAD_DIR, datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    os.makedirs(target, exist_ok=True)
    
    def move_job():
        cnt = 0
        for i in os.listdir(DOWNLOAD_DIR):
            if 'TGParaDL' not in i and not i.startswith('.') and i != os.path.basename(target):
                try:
                    shutil.move(os.path.join(DOWNLOAD_DIR, i), os.path.join(target, i))
                    cnt += 1
                except: pass
        return cnt
        
    count = await asyncio.to_thread(move_job)
    await message.reply(f"📦 **Archive Operation Successful**\nTransferred `{count}` item(s) to:\n`{target}`", reply_markup=get_main_keyboard())

async def show_status(client, message: Message):
    """Displays hardware and application status, including disk usage."""
    def get_stats():
        total = sum(os.path.getsize(os.path.join(r, f)) for r, _, files in os.walk(DOWNLOAD_DIR) for f in files)
        disk = shutil.disk_usage(DOWNLOAD_DIR)
        return total, disk
    
    total_size, disk = await asyncio.to_thread(get_stats)
    await message.reply(f"**📊 TGParaDL System Diagnostics**\n\n"
                        f"**Active Workers:** `{config.get('CHUNK_WORKERS', 4)}`\n"
                        f"**Queue Capacity:** `{config.get('MAX_CONCURRENT_DOWNLOADS', 3)}`\n"
                        f"**Storage Allocated:** `{human_size(total_size)}`\n"
                        f"**Storage Available:** `{human_size(disk.free)}`",
                        reply_markup=get_main_keyboard())

# ================= DOWNLOAD HANDLERS =================

@app.on_callback_query(filters.regex(r"^cancel_(.+)"))
async def cancel_callback(client, callback_query):
    """Handles the cancellation of an active download task."""
    task_id = callback_query.data.split("_")[1]
    if task_id in cancel_events:
        cancel_events[task_id].set()
        await callback_query.answer("Initiating cancellation sequence...", show_alert=True)
    else:
        await callback_query.answer("Task unavailable or already completed.", show_alert=True)

@app.on_message((filters.document | filters.video | filters.audio) & is_admin)
async def hybrid_parallel_download_handler(client: Client, message: Message):
    """
    Core handler for processing incoming media files.
    Implements multi-DC parallel chunk downloading to bypass Telegram's single-connection speed limits.
    """
    media = message.document or message.video or message.audio
    raw_name = getattr(media, 'file_name', f"file_{int(time.time())}")
    file_name = sanitize_filename(raw_name)
    file_size = getattr(media, 'file_size', 0)
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    # Task specific event for cancellation
    task_id = str(message.id)
    cancel_event = asyncio.Event()
    cancel_events[task_id] = cancel_event
    cancel_markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Abort", callback_data=f"cancel_{task_id}")]])
    
    status_msg = await message.reply(
        f"⏳ **Task Queued**\n**File:** `{file_name}`\n*Awaiting available worker slot...*", 
        quote=True, 
        reply_markup=cancel_markup
    )
    
    try:
        # Respect global concurrent download limits
        async with DOWNLOAD_SEMAPHORE:
            if cancel_event.is_set():
                await status_msg.edit(f"❌ **Task Aborted:** `{file_name}`")
                return

            await status_msg.edit(f"⚡ **Initializing Download Sequence...**\n**File:** `{file_name}`", reply_markup=cancel_markup)
            
            start_time = time.time()
            chunk_size = 1048576  # Telegram standard chunk size (1 MB)
            total_chunks = math.ceil(file_size / chunk_size)
            downloaded = 0
            last_update = time.time()
            
            # Queue to orchestrate thread-safe disk writes
            write_queue = asyncio.Queue()
            
            async def disk_writer():
                """Background task that receives byte chunks and writes them to disk at exact offsets."""
                # Pre-allocate file space to avoid fragmentation
                async with aiofiles.open(file_path, 'wb') as f:
                    if file_size > 0:
                        await f.seek(file_size - 1)
                        await f.write(b'\0')
                        
                # Continuous write loop until signaled to stop
                async with aiofiles.open(file_path, 'r+b') as f:
                    while True:
                        item = await write_queue.get()
                        if item is None: break  # Poison pill to stop writer
                        offset, data = item
                        await f.seek(offset)
                        await f.write(data)
                        write_queue.task_done()
                        
            writer_task = asyncio.create_task(disk_writer())
            
            # Download first chunk sequentially to authenticate CDN/DC properly
            first_chunk_data = b""
            async for data in client.stream_media(message, limit=1, offset=0):
                if cancel_event.is_set(): break
                first_chunk_data += data
                
            if first_chunk_data and not cancel_event.is_set():
                await write_queue.put((0, first_chunk_data))
                downloaded += len(first_chunk_data)
                
            # Distribute remaining chunks across concurrent workers
            workers = config.get("CHUNK_WORKERS", 4)
            semaphore = asyncio.Semaphore(workers) 
            
            async def fetch_chunk(chunk_index):
                """Fetches a specific chunk from Telegram and queues it for writing."""
                nonlocal downloaded, last_update
                if cancel_event.is_set(): return
                byte_offset = chunk_index * chunk_size
                
                async with semaphore:
                    if cancel_event.is_set(): return
                    retries = 3
                    while retries > 0:
                        try:
                            chunk_data = b""
                            async for data in client.stream_media(message, limit=1, offset=chunk_index):
                                if cancel_event.is_set(): return
                                chunk_data += data
                            
                            if chunk_data and not cancel_event.is_set():
                                await write_queue.put((byte_offset, chunk_data))
                                downloaded += len(chunk_data)
                                
                                # Throttle UI updates to prevent FloodWaits
                                now = time.time()
                                if now - last_update > 3:
                                    last_update = now
                                    elapsed = now - start_time
                                    speed = downloaded / elapsed if elapsed > 0 else 0
                                    percentage = round((downloaded / file_size) * 100, 1) if file_size > 0 else 0
                                    
                                    try:
                                        await status_msg.edit(
                                            f"⚡ **Downloading:** `{file_name}`\n"
                                            f"📊 **Progress:** `{percentage}%`\n"
                                            f"🚀 **Speed:** `{human_size(speed)}/s`\n"
                                            f"🔗 **Active Workers:** `{workers}`\n"
                                            f"📦 **Data:** `{human_size(downloaded)}` / `{human_size(file_size)}`",
                                            reply_markup=cancel_markup
                                        )
                                    except MessageNotModified: pass
                                    except FloodWait as e: await asyncio.sleep(e.value)
                                    except Exception: pass
                            break
                        except FloodWait as e:
                            await asyncio.sleep(e.value)
                        except Exception as e:
                            retries -= 1
                            if retries == 0: raise e
                            await asyncio.sleep(1)
            
            # Execute chunk fetches concurrently
            if total_chunks > 1 and not cancel_event.is_set():
                tasks = [fetch_chunk(i) for i in range(1, total_chunks)]
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # Signal writer to close and await final disk operations
            await write_queue.put(None)
            await writer_task
            
            # Handle cancellation post-download attempt
            if cancel_event.is_set():
                if os.path.exists(file_path): 
                    await asyncio.to_thread(os.remove, file_path)
                try: 
                    await status_msg.edit(f"❌ **Task Aborted:** `{file_name}`")
                except: pass
                return
            
            # Finalize UI representation
            total_time = max(1, round(time.time() - start_time))
            avg_speed = human_size(file_size / total_time)
            mins, secs = divmod(total_time, 60)
            
            try:
                await status_msg.edit(
                    f"✅ **Download Successfully Finalized**\n\n"
                    f"**File:** `{file_name}`\n"
                    f"**Size:** `{human_size(file_size)}`\n"
                    f"**Avg. Speed:** `{avg_speed}/s` ⚡\n"
                    f"**Duration:** `{mins}m {secs}s`"
                )
            except: pass

    except Exception as e:
        try: 
            await status_msg.edit(f"❌ **Process Failed:** `{str(e)}`")
        except: pass
    finally:
        # Cleanup task footprint from events dictionary
        cancel_events.pop(task_id, None)

if __name__ == "__main__":
    print("Initializing TGParaDL System...")
    try: 
        app.run()
    except KeyboardInterrupt: 
        print("\nTGParaDL process terminated gracefully.")
