# TGParaDL - High-Performance Telegram Parallel Downloader

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Hydrogram](https://img.shields.io/badge/Hydrogram-2.0+-green.svg)](https://docs.hydrogram.org/)

**TGParaDL** is a Telegram bot that leverages **multi-DC parallel chunk downloading** to maximize download speeds from Telegram. By splitting files into chunks and fetching them concurrently across multiple Telegram datacenters, it bypasses the standard single-connection rate limits, achieving significantly faster transfers. The bot includes a robust queuing system, graceful cancellation, and an intuitive file management interface.

> **Author**: Alien  
> **Channel**: https://t.me/AlienDevLab  
> **License**: MIT

## ✨ Features

-  **Parallel Chunk Downloading** – Downloads file chunks concurrently using multiple Telegram Data Centers (DCs) for maximum throughput.
-  **Configurable Worker Pool** – Adjust the number of concurrent chunk workers and overall parallel downloads.
-  **Task Queuing** – Respects global download concurrency limits; queued tasks wait for available slots.
-  **Graceful Cancellation** – Cancel any active download instantly via an inline button; partially downloaded files are removed.
-  **Built-in File Management** – View, delete, move, and wipe all stored files via an interactive reply keyboard.
-  **Authorization** – Restrict bot access to specific Telegram user IDs (optional).
-  **Status & Diagnostics** – Monitor storage usage, system performance, and active worker counts.
-  **Auto-Sanitization** – Filenames are automatically sanitized to prevent OS errors.
-  **Persistent Configuration** – Settings stored in a simple `settings.ini` file; first-run guided setup.

## 📋 Prerequisites

- Python 3.8 or higher
- A Telegram Bot Token (obtain from [@BotFather](https://t.me/BotFather))
- API ID and API Hash from [my.telegram.org](https://my.telegram.org/apps)

## 🔧 Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/TGParaDL.git
   cd TGParaDL
   ```

2. **Install required dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the bot**  
   Run the bot once; it will prompt you for credentials and generate a `settings.ini` file:
   ```bash
   python TGParaDL.py
   ```
   Follow the interactive prompts to enter your **API_ID**, **API_HASH**, **BOT_TOKEN**, and preferred download directory.

   Alternatively, you can manually create `settings.ini` with the following structure:
   ```ini
   [Settings]
   API_ID = your_api_id
   API_HASH = your_api_hash
   BOT_TOKEN = your_bot_token
   DOWNLOAD_DIR = ./downloads/
   ALLOWED_USERS = 123456789,987654321
   MAX_CONCURRENT_DOWNLOADS = 3
   CHUNK_WORKERS = 4
   ```

## ⚙️ Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `API_ID` | Your Telegram API ID (integer) | `0` |
| `API_HASH` | Your Telegram API Hash (string) | (empty) |
| `BOT_TOKEN` | Your Telegram Bot Token (string) | (empty) |
| `DOWNLOAD_DIR` | Directory to store downloaded files | `./downloads/` |
| `ALLOWED_USERS` | Comma-separated list of Telegram user IDs authorized to use the bot. Leave empty to allow all users. | (empty) |
| `MAX_CONCURRENT_DOWNLOADS` | Maximum number of files downloaded simultaneously (global queue limit) | `3` |
| `CHUNK_WORKERS` | Number of concurrent chunk fetchers per download (parallelism degree) | `4` |

## 🚀 Usage

Run the bot with:
```bash
python TGParaDL.py
```

Once the bot is running, interact with it on Telegram.

### Start the Bot
Send the `/start` command to see the welcome message and the main interactive menu.

### Download Files
Simply forward or upload any **document**, **video**, or **audio** file to the bot. The download will begin automatically (or queue if the slot is busy). A progress message is updated every few seconds, showing:
- Percentage completed
- Current download speed
- Active chunk workers
- Data transferred

You can cancel any download by clicking the **"❌ Abort"** button on the progress message.

### File Management Commands
The bot provides a custom reply keyboard with the following buttons:

| Button | Action |
|--------|--------|
| `📁 View Files` | List all files and directories in the download folder (limited to prevent message overflow). |
| `🗑️ Delete Files` | Remove **all files** in the root download directory (preserves subdirectories). |
| `📦 Move Files` | Archive all current files into a timestamped subdirectory (e.g., `2026-08-17_12-34-56/`). |
| `❌ Delete All` | **Permanently wipe** all files *and* subdirectories in the download path (requires confirmation). |
| `ℹ️ Status` | Display system diagnostics: storage usage, available disk space, worker counts, etc. |

### Text Commands

| Command | Description |
|---------|-------------|
| `/start` | Initialize the bot and show the main menu. |
| `/help` | Display a help message with command reference. |
| `/status` | Show real-time storage and system diagnostics. |
| `/ping` | Measure bot response latency. |

## 🧠 How It Works

1. **File Reception** – When a supported media file (document/video/audio) is sent, the bot extracts metadata (filename, size) and generates a unique task ID.

2. **Queuing** – If the current number of active downloads is at the `MAX_CONCURRENT_DOWNLOADS` limit, the task waits until a slot frees up.

3. **Parallel Chunking** – The file is split into 1 MB chunks (Telegram’s standard). The bot first downloads the first chunk sequentially to authenticate the session with the correct datacenter. Then, it spawns up to `CHUNK_WORKERS` concurrent fetchers, each requesting a specific chunk offset via `client.stream_media()`. This parallel approach leverages Telegram’s multi-DC infrastructure, saturating your network bandwidth.

4. **Thread-Safe Writing** – A dedicated asynchronous writer receives chunks from the queue and writes them to the correct file offsets using `aiofiles`. The file is pre-allocated to avoid fragmentation.

5. **Progress Updates** – Download progress and speed are calculated and updated on the status message every 3 seconds to avoid Telegram’s rate limits (`FloodWait`).

6. **Cancellation** – If the user cancels, the `cancel_event` is set, interrupting all pending chunk fetches, and the partially written file is removed.

7. **Completion** – Once all chunks are written, the final status message reports the average speed and total duration. The file is now ready in the download directory.

## 📁 File Storage

All downloads are stored under the `DOWNLOAD_DIR` (default `./downloads/`). The bot creates this directory automatically if it does not exist.

- **Naming**: Filenames are sanitized by removing characters invalid for the OS (`\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`).
- **Management**: Use the built-in buttons to keep your storage organized. The `Move Files` button is particularly useful for archiving old downloads without deleting them.

## 🛡️ Security & Authorization

- By default, the bot is open to all users. To restrict access, populate the `ALLOWED_USERS` list in `settings.ini` with the numeric Telegram user IDs of authorized users.
- The bot uses Hydrogram, which handles secure communication with Telegram’s API.
- All downloaded files reside on the server; no external uploads are performed.

## 📝 License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.

## 👤 Credits

- **Author**: Alien
- Built with [Hydrogram](https://docs.hydrogram.org/) – a powerful Telegram MTProto library.

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to open a pull request or an issue on GitHub.

## 📞 Support

For questions or feedback, please open an issue on the repository.

**Enjoy lightning-fast downloads from Telegram with TGParaDL!** 🚀
