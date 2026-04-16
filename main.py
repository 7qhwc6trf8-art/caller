import asyncio
import random
from telethon import TelegramClient, events

# --- CONFIG ---
API_ID = 33283287
API_HASH = "bc444c7073eb2e3bb95414bd678a5c25"
OWNER_ID = 8182558373
TARGET_CHAT_NAME = "𝑭𝑰𝑵𝑨𝑳 𝑴𝑨𝑭𝑰𝑨"

START_TRIGGER = "Խաղի գրանցումը սկսված է"
STOP_TRIGGERS = ["Խաղը սկսվում է!", "սթոփ", "stop"]

client = TelegramClient("caller_session", API_ID, API_HASH)

active_tagging_task = None
target_chat_entity = None

async def tagging_worker(chat_entity, text, event):
    try:
        if (event.sender_id == OWNER_ID):
            await event.delete()
        participants = await client.get_participants(chat_entity)
        users = [u for u in participants if not u.bot]
        
        print(f"Starting tag run for {len(users)} users...")
        for user in users:
            if user.username:
                mention = f"@{user.username}"
            else:
                name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                mention = f"<a href='tg://user?id={user.id}'>{name}</a>"
            
            await client.send_message(chat_entity, f"{mention} {text}", parse_mode="html")
            print(f"{name} sucessfully called")
            await asyncio.sleep(random.randint(3, 5))
            
    except asyncio.CancelledError:
        print("Task cancelled.")
    except Exception as e:
        print(f"Worker Error: {e}")

async def stop_tagging():
    global active_tagging_task
    if active_tagging_task and not active_tagging_task.done():
        active_tagging_task.cancel()
        try:
            await active_tagging_task
        except asyncio.CancelledError:
            pass
    active_tagging_task = None

@client.on(events.NewMessage)
async def message_listener(event):
    global active_tagging_task, target_chat_entity

    text = event.raw_text or ""
    sender_id = event.sender_id
    chat_id = event.chat_id

    # --- AUTO-LEARN CHAT ID ---
    # If the owner types the command, we lock onto THIS chat ID immediately
    if text.startswith("կանչել") and sender_id == OWNER_ID:
        print(f"Locked onto Chat ID: {chat_id}")
        target_chat_entity = await event.get_input_chat()
        
        await stop_tagging()
        parts = text.split(maxsplit=1)
        tag_text = parts[1] if len(parts) > 1 else "🔥"
        active_tagging_task = asyncio.create_task(tagging_worker(target_chat_entity, tag_text, event))
        return

    # --- REGULAR TRIGGER LOGIC ---
    if target_chat_entity and chat_id == event.chat_id:
        if text == START_TRIGGER:
            
            print("Auto-start triggered.")
            await stop_tagging()
            active_tagging_task = asyncio.create_task(tagging_worker(target_chat_entity, "արի", event))
            
        elif any(trigger in text for trigger in STOP_TRIGGERS):
            if (event.sender_id == OWNER_ID):
                await event.delete()
            print("Stop triggered.")
            await stop_tagging()

async def main():
    global target_chat_entity
    await client.start()
    
    print("Searching for chat...")
    async for dialog in client.iter_dialogs():
        if dialog.name and TARGET_CHAT_NAME.lower() in dialog.name.lower():
            target_chat_entity = dialog.entity
            print(f"Found chat: {dialog.name} ({dialog.id})")
            break
            
    print("Bot is running. If it doesn't respond, type 'կանչել' in the group.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    client.loop.run_until_complete(main())