import os
import time
import asyncio
import nest_asyncio
from io import BytesIO

from flask import Flask, render_template, request, redirect, url_for, session, flash

import pandas as pd
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import UserNotParticipantError, MediaEmptyError, FloodWaitError, ApiIdInvalidError
import requests

# Apply nest_asyncio globally for Flask
try:
    nest_asyncio.apply()
except RuntimeError:
    pass # Already applied or not needed in this environment

# --- Flask App Setup ---
app_flask = Flask(__name__)
app_flask.secret_key = os.urandom(24) # Important for session management

# --- Global variables (Telethon client and download folder) ---
telethon_client: TelegramClient = None
DOWNLOAD_FOLDER_UI = 'downloads_colab_ui_async'

if not os.path.exists(DOWNLOAD_FOLDER_UI):
    os.makedirs(DOWNLOAD_FOLDER_UI)

# --- Log function ---
def add_log(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    if 'logs' not in session:
        session['logs'] = []
    session['logs'].insert(0, log_entry)
    session['logs'] = session['logs'][:100]
    session.modified = True

# --- ASYNC HELPER ---
def run_async_task(async_func_with_args):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(async_func_with_args)
    except RuntimeError as e:
        if "This event loop is already running" in str(e):
            add_log(f"Async execution helper runtime error (nested loop): {e}")
            raise
        add_log(f"Async execution helper error: {type(e).__name__} - {e}")
        return None
    except Exception as e:
        add_log(f"Async execution helper error: {type(e).__name__} - {e}")
        return None


# --- ASYNC Logic for Telethon Operations ---
async def _connect_session_async(api_id_str, api_hash_str, chosen_session_type, session_identifier):
    global telethon_client
    current_api_id = int(api_id_str)

    if telethon_client and telethon_client.is_connected:
        add_log("Disconnecting previous async session...")
        try: await telethon_client.disconnect()
        except Exception as e: add_log(f"Note during old client disconnect: {e}")
    telethon_client = None
    session['session_active'] = False

    try:
        add_log(f"Attempting async connect using {chosen_session_type} session: '{session_identifier[:20] if chosen_session_type=='string' else session_identifier}...'")
        if chosen_session_type == 'string':
            telethon_client = TelegramClient(StringSession(session_identifier), current_api_id, api_hash_str)
        else:
            telethon_client = TelegramClient(session_identifier, current_api_id, api_hash_str)

        await telethon_client.connect()

        if telethon_client.is_connected() and await telethon_client.is_user_authorized():
            me = await telethon_client.get_me()
            add_log(f"Async connect successful! Logged in as: {me.first_name} (@{me.username or 'N/A'})")
            session['session_active'] = True
            session['user_info'] = f"{me.first_name} (@{me.username or 'N/A'})"
            return True
        else:
            add_log("Async connect: Failed to connect or session not authorized.")
            if telethon_client.is_connected(): await telethon_client.disconnect()
            session['session_active'] = False
            return False
    except ApiIdInvalidError:
        add_log("Telethon Error: Invalid API ID or API Hash. Please check your credentials.")
        session['session_active'] = False; return False
    except FloodWaitError as e_flood:
        add_log(f"Connection Error (Flood Wait): Please wait {e_flood.seconds} seconds.")
        if telethon_client and telethon_client.is_connected(): await telethon_client.disconnect()
        session['session_active'] = False; return False
    except ConnectionError as e_conn:
        add_log(f"Network Connection Error: {e_conn}.")
        if telethon_client and telethon_client.is_connected(): await telethon_client.disconnect()
        session['session_active'] = False; return False
    except Exception as e:
        add_log(f"Error during async session connection: {type(e).__name__} - {e}")
        if telethon_client and telethon_client.is_connected(): await telethon_client.disconnect()
        session['session_active'] = False; return False

async def _disconnect_session_async():
    global telethon_client
    add_log("Disconnecting current async session...")
    if telethon_client and telethon_client.is_connected():
        try:
            await telethon_client.disconnect()
            add_log("Async session disconnected.")
        except Exception as e:
            add_log(f"Note during async disconnect: {e}")
    else:
        add_log("No active async session to disconnect.")
    telethon_client = None
    session['session_active'] = False
    session.pop('user_info', None)
    return True

async def _upload_file_async(filepath, caption):
    if not session.get('session_active') or not telethon_client or not telethon_client.is_connected():
        add_log("Upload Error: Not connected.")
        flash("Upload Error: Not connected.", "danger")
        return False

    session['upload_progress'] = 0
    session.modified = True

    def progress_callback_sync(current, total):
        if total > 0:
            percent = (current / total) * 100
            session['upload_progress'] = percent
            if int(percent) % 20 == 0 and int(percent) < 100 : # Log every 20%, avoid spamming 100%
                 add_log(f"Upload progress: {percent:.0f}% for {os.path.basename(filepath)}")
            session.modified = True

    try:
        add_log(f"Async Upload: Starting '{os.path.basename(filepath)}' to Saved Messages...");
        await telethon_client.send_file('me', filepath, caption=caption, progress_callback=progress_callback_sync)
        add_log(f"Async Upload: Completed: {os.path.basename(filepath)}");
        session['upload_progress'] = 100
        flash(f"Successfully uploaded {os.path.basename(filepath)}", "success")
        return True
    except MediaEmptyError:
        add_log(f"Upload Error (Telegram): File '{os.path.basename(filepath)}' is empty.");
        flash(f"Upload Error: File '{os.path.basename(filepath)}' is empty.", "danger")
    except UserNotParticipantError:
        add_log("Upload Error (Telegram): UserNotParticipantError.");
        flash("Upload Error: UserNotParticipantError.", "danger")
    except FloodWaitError as e_tg:
        add_log(f"Upload Error (Telegram Flood Wait): {e_tg.seconds}s");
        flash(f"Upload Error (Telegram Flood Wait): Please wait {e_tg.seconds}s.", "warning")
    except Exception as e_upload:
        add_log(f"Async Upload failed: {type(e_upload).__name__} - {e_upload}");
        flash(f"Upload failed: {e_upload}", "danger")
    session['upload_progress'] = -1 # Indicate error
    return False


async def _run_csv_process_async(csv_file_bytes):
    is_client_ready = session.get('session_active') and telethon_client and telethon_client.is_connected()
    if not is_client_ready:
        add_log("CSV Info: Not connected to Telegram. Files will only be downloaded.")
        flash("Not connected to Telegram. Files will only be downloaded.", "info")

    session['csv_overall_progress'] = 0
    session['csv_current_item_progress'] = 0
    session.modified = True

    try:
        try:
            dataframe = pd.read_csv(BytesIO(csv_file_bytes), delimiter=';')
        except Exception as e_parse:
            add_log(f"CSV Parsing Error: Could not parse CSV. Check format/delimiter. Details: {e_parse}")
            flash(f"CSV Parsing Error: {e_parse}", "danger")
            session['csv_overall_progress'] = -1
            return False

        if 'link' not in dataframe.columns:
            add_log("CSV Error: CSV file must contain a 'link' column (lowercase).")
            flash("CSV Error: CSV file must contain a 'link' column.", "danger")
            session['csv_overall_progress'] = -1
            return False

        total_csv_items = len(dataframe)
        if total_csv_items == 0:
            add_log("CSV Info: The CSV file is empty (no data rows).")
            flash("CSV Info: File is empty.", "info")
            return True

        add_log(f"Starting CSV processing for {total_csv_items} items...")
        successful_uploads = 0

        for index, row_data in dataframe.iterrows():
            item_num = index + 1
            session['csv_overall_progress'] = (item_num / total_csv_items) * 100
            session.modified = True

            url_from_row = row_data.get('link')
            if not isinstance(url_from_row, str) or not url_from_row.strip():
                add_log(f"CSV Item {item_num}/{total_csv_items}: Skipped (empty or invalid URL in 'link' column).")
                continue

            add_log(f"CSV Item {item_num}/{total_csv_items}: Processing URL '{url_from_row}'")
            filename_from_csv_url = url_from_row.split('/')[-1].split('?')[0]
            filename_from_csv_url = "".join(c for c in filename_from_csv_url if c.isalnum() or c in ('.', '_', '-')).strip()
            if not filename_from_csv_url:
                filename_from_csv_url = f"csv_dl_{int(time.time())}_{index}"
            dl_item_path = os.path.join(DOWNLOAD_FOLDER_UI, filename_from_csv_url)

            try:
                add_log(f"CSV Item {item_num}: Downloading '{filename_from_csv_url}'...")
                response = requests.get(url_from_row, stream=True, timeout=60)
                response.raise_for_status()
                with open(dl_item_path, 'wb') as f_item_dl:
                    for chunk in response.iter_content(chunk_size=8192*4):
                        if chunk: f_item_dl.write(chunk)
                add_log(f"CSV Item {item_num}: Downloaded '{filename_from_csv_url}'.")

                if is_client_ready:
                    caption_csv = str(row_data.get('caption', ''))
                    add_log(f"CSV Item {item_num}: Attempting upload of '{filename_from_csv_url}'...")
                    upload_ok = await _upload_file_async(dl_item_path, caption_csv) # This will log/flash
                    if upload_ok:
                        successful_uploads += 1
                else:
                    add_log(f"CSV Item {item_num}: Skipped upload for '{filename_from_csv_url}' (not connected to Telegram).")

            except requests.exceptions.RequestException as e_dl:
                add_log(f"CSV Item {item_num}: Download failed for '{url_from_row}' - {str(e_dl)}")
                flash(f"CSV Item {item_num} Download failed: {e_dl}", "warning")
            except Exception as e_proc:
                add_log(f"CSV Item {item_num}: Error processing '{url_from_row}' - {str(e_proc)}")
                flash(f"CSV Item {item_num} Error: {e_proc}", "warning")

        add_log(f"CSV processing completed. {successful_uploads} of {total_csv_items} items processed for potential upload.")
        flash(f"CSV processing completed. {successful_uploads}/{total_csv_items} items successfully uploaded (if connected).", "success")
        session['csv_overall_progress'] = 100
        return True

    except pd.errors.EmptyDataError:
        add_log("CSV Error: File empty/invalid (pandas EmptyDataError).")
        flash("CSV Error: File empty or invalid.", "danger")
    except Exception as e_csv_main:
        add_log(f"CSV processing failed: {str(e_csv_main)}")
        flash(f"CSV processing failed: {e_csv_main}", "danger")

    session['csv_overall_progress'] = -1
    return False

# --- Flask Routes ---
@app_flask.route('/', methods=['GET', 'POST'])
def login_page():
    global telethon_client
    if request.method == 'POST':
        api_id = request.form.get('api_id')
        api_hash = request.form.get('api_hash')
        session_type = request.form.get('session_type')
        session_name = request.form.get('session_name')
        session_string = request.form.get('session_string')

        session['api_id_form'] = api_id
        session['api_hash_form'] = api_hash
        session['session_type_pref'] = session_type
        session['session_name_form'] = session_name

        if not all([api_id, api_hash]):
            flash("API ID and API Hash are required.", "danger")
            return redirect(url_for('login_page'))
        try:
            int(api_id)
        except ValueError:
            flash("API ID must be an integer.", "danger")
            return redirect(url_for('login_page'))

        session_identifier = None
        if session_type == 'file':
            if not session_name:
                flash("Session File Name required for file session type.", "danger")
                return redirect(url_for('login_page'))
            session_identifier = session_name
            if not os.path.exists(f"{session_name}.session"):
                if not os.path.exists(os.path.join(os.getcwd(), f"{session_name}.session")):
                    flash(f"Error: Session file '{session_name}.session' not found in current directory.", "danger")
                    add_log(f"Error: Session file '{session_name}.session' not found in {os.getcwd()}.")
                    return redirect(url_for('login_page'))
        elif session_type == 'string':
            if not session_string:
                flash("Session String required for string session type.", "danger")
                return redirect(url_for('login_page'))
            session_identifier = session_string
        else:
            flash("Invalid session type selected.", "danger")
            return redirect(url_for('login_page'))

        connect_success = run_async_task(
            _connect_session_async(api_id, api_hash, session_type, session_identifier)
        )

        if connect_success:
            flash(f"Connected as {session.get('user_info', 'Unknown User')}", "success")
            session.pop('api_id_form', None)
            session.pop('api_hash_form', None)
            session.pop('session_name_form', None)
            # session.pop('session_type_pref', None) # Keep type preference
            return redirect(url_for('main_app_page'))
        else:
            flash("Connection failed. Check logs for details.", "danger")
            return redirect(url_for('login_page'))

    if session.get('session_active'):
        return redirect(url_for('main_app_page'))

    return render_template('login.html',
                           api_id=session.get('api_id_form', ''),
                           api_hash=session.get('api_hash_form', ''),
                           session_type_pref=session.get('session_type_pref', 'file'),
                           session_name=session.get('session_name_form', ''),
                           logs=session.get('logs', []))

@app_flask.route('/main')
def main_app_page():
    if not session.get('session_active'):
        # Allow access to main page even if not connected for download-only usage
        # But restrict upload/CSV functionality on the page itself.
        flash("Not connected to Telegram. Some features like upload and CSV processing will be limited.", "warning")
        # We still want to show the page, so don't redirect to login.
        # User info will be 'N/A' or similar.
        session['user_info'] = "N/A (Not Connected to Telegram)"


    downloaded_files = []
    if os.path.exists(DOWNLOAD_FOLDER_UI):
        try:
            downloaded_files = sorted([f for f in os.listdir(DOWNLOAD_FOLDER_UI)
                                       if os.path.isfile(os.path.join(DOWNLOAD_FOLDER_UI, f))])
        except Exception as e:
            add_log(f"Error listing downloaded files: {e}")
            flash(f"Error listing files: {e}", "danger")

    # Clear progress from session after displaying once to prevent stale bars
    upload_progress = session.pop('upload_progress', 0 if 'upload_progress' in session else None)
    download_progress = session.pop('download_progress', 0 if 'download_progress' in session else None)
    csv_overall_progress = session.pop('csv_overall_progress', 0 if 'csv_overall_progress' in session else None)


    return render_template('main_app.html',
                           user_info=session.get('user_info', 'N/A'),
                           logs=session.get('logs', []),
                           downloaded_files=downloaded_files,
                           upload_progress=upload_progress,
                           download_progress=download_progress,
                           csv_overall_progress=csv_overall_progress,
                           DOWNLOAD_FOLDER_UI_basename = os.path.basename(DOWNLOAD_FOLDER_UI),
                           session_active = session.get('session_active', False) # Pass this to template
                           )

@app_flask.route('/disconnect')
def disconnect():
    run_async_task(_disconnect_session_async())
    flash("Session disconnected.", "info")
    session.pop('api_id_form', None)
    session.pop('api_hash_form', None)
    session.pop('session_name_form', None)
    # session.pop('session_type_pref', None) # Keep type preference if desired
    session.pop('user_info', None)
    session['session_active'] = False # Explicitly set to false
    return redirect(url_for('login_page'))

@app_flask.route('/download_link', methods=['POST'])
def download_from_link_route():
    url_val = request.form.get('download_link')
    if not url_val:
        flash("Download Error: Please enter a valid URL.", "danger")
        add_log("Download Error: Empty URL provided.")
        return redirect(url_for('main_app_page'))

    session['download_progress'] = 0 # Initialize
    session.modified = True

    file_name_from_url = url_val.split('/')[-1].split('?')[0]
    file_name_from_url = "".join(c for c in file_name_from_url if c.isalnum() or c in ('.', '_', '-')).strip()
    if not file_name_from_url:
        file_name_from_url = f"downloaded_file_{int(time.time())}"
    download_filepath = os.path.join(DOWNLOAD_FOLDER_UI, file_name_from_url)

    try:
        add_log(f"Starting download from {url_val} to {download_filepath}")
        response = requests.get(url_val, stream=True, timeout=60)
        response.raise_for_status()
        total_size_in_bytes = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        with open(download_filepath, 'wb') as f_download:
            for chunk_data in response.iter_content(chunk_size=8192*4):
                if chunk_data:
                    f_download.write(chunk_data)
                    downloaded_size += len(chunk_data)
                    if total_size_in_bytes > 0:
                        current_progress = (downloaded_size / total_size_in_bytes) * 100
                        if int(current_progress) % 10 == 0 and int(current_progress) != int(session.get('download_progress',0)): # Update less frequently
                            session['download_progress'] = current_progress
                            # session.modified = True # Potentially too frequent, relying on final update
                    # else: # Progress for unknown size
                        # add_log(f"Downloaded {downloaded_size / (1024*1024):.2f} MB...")

        session['download_progress'] = 100
        add_log(f"Download completed: {file_name_from_url}")
        flash(f"Download completed: {file_name_from_url}", "success")
    except requests.exceptions.RequestException as e_req:
        add_log(f"Download failed (Request Error): {str(e_req)}")
        flash(f"Download failed: {e_req}", "danger")
        session['download_progress'] = -1
    except Exception as e_general:
        add_log(f"Download failed (General Error): {str(e_general)}")
        flash(f"Download failed: {e_general}", "danger")
        session['download_progress'] = -1
    session.modified = True
    return redirect(url_for('main_app_page'))


@app_flask.route('/upload_file', methods=['POST'])
def upload_file_route():
    if not session.get('session_active'):
        flash("Not connected to Telegram. Cannot upload.", "danger")
        add_log("Upload attempt failed: Not connected to Telegram.")
        return redirect(url_for('main_app_page'))

    selected_file = request.form.get('file_to_upload')
    caption = request.form.get('caption', '')

    if not selected_file:
        flash("Upload Error: Please select a file.", "danger")
        return redirect(url_for('main_app_page'))

    upload_filepath = os.path.join(DOWNLOAD_FOLDER_UI, selected_file)
    if not os.path.exists(upload_filepath):
        flash(f"Upload Error: File '{selected_file}' not found in '{DOWNLOAD_FOLDER_UI}'.", "danger")
        add_log(f"Upload Error: File '{selected_file}' not found at '{upload_filepath}'.")
        return redirect(url_for('main_app_page'))

    run_async_task(_upload_file_async(upload_filepath, caption))
    return redirect(url_for('main_app_page'))


@app_flask.route('/process_csv', methods=['POST'])
def process_csv_route():
    # Connection check is inside _run_csv_process_async which will flash/log if not connected
    # but still proceed with downloads.

    csv_file = request.files.get('csv_file')
    if not csv_file or not csv_file.filename:
        flash("CSV Error: Please upload a CSV file.", "danger")
        return redirect(url_for('main_app_page'))

    if not csv_file.filename.lower().endswith('.csv'):
        flash("CSV Error: Please upload a valid .csv file.", "danger")
        return redirect(url_for('main_app_page'))

    try:
        csv_content_bytes = csv_file.read()
        flash(f"Processing CSV file: {csv_file.filename}. This may take a while...", "info")
        add_log(f"Received CSV file for processing: {csv_file.filename}")
        # This is an async task but the route itself is sync.
        # The user will see the redirect immediately. Progress updates via logs/next page load.
        run_async_task(_run_csv_process_async(csv_content_bytes))
    except Exception as e:
        add_log(f"Error reading/initiating CSV processing: {e}")
        flash(f"Error processing CSV: {e}", "danger")
        session['csv_overall_progress'] = -1 # Error state

    return redirect(url_for('main_app_page'))

@app_flask.route('/clear_logs', methods=['POST'])
def clear_logs_route():
    session.pop('logs', None)
    flash("Logs cleared.", "info")
    # Redirect back to the page the user was on
    return redirect(request.referrer or url_for('main_app_page'))


# --- Initialize ---
def initialize_application_state():
    add_log("Flask Telethon UI Application starting...")

if __name__ == '__main__':
    initialize_application_state()
    app_flask.run(debug=True, host='0.0.0.0', port=5000)
    # For production, use a WSGI server:
    # gunicorn -w 4 -b 0.0.0.0:5000 app:app_flask
