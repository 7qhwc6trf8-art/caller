import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import PollAnswer

from config import config
from quiz_manager import QuizManager

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
qm = QuizManager("questions.json")

# Mapping to know which poll belongs to which chat
poll_to_chat: dict[str, int] = {}

async def is_admin(message: types.Message):
    if message.chat.type == "private":
        return True
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    return member.status in ("administrator", "creator")

async def send_next_question(chat_id: int):
    session = qm.get_session(chat_id)
    if not session: return

    if session.current_index >= len(session.questions):
        # Leaderboard logic
        if not session.scores:
            await bot.send_message(chat_id, "🏁 Վիկտորինան ավարտվեց: Մասնակիցներ չեղան:")
        else:
            text = "🏆 **Վիկտորինայի արդյունքները.**\n\n"
            sorted_scores = sorted(session.scores.items(), key=lambda x: x[1], reverse=True)
            for i, (user_id, score) in enumerate(sorted_scores, 1):
                text += f"{i}. Օգտատեր {user_id}: {score} միավոր\n"
            await bot.send_message(chat_id, text, parse_mode="Markdown")
        
        qm.stop_session(chat_id)
        return

    q = session.questions[session.current_index]
    
    poll = await bot.send_poll(
        chat_id=chat_id,
        question=q['question'],
        options=q['options'],
        type="quiz",
        correct_option_id=q['answer'], # Matches your JSON 'answer' key
        is_anonymous=False,
        explanation=q.get('explanation'),
        open_period=config.QUIZ_TIMEOUT
    )
    
    poll_to_chat[poll.poll.id] = chat_id
    session.current_index += 1

@dp.message(Command("startquiz"))
async def cmd_start(message: types.Message):
    if not await is_admin(message): return
    if qm.get_session(message.chat.id):
        return await message.answer("⚠️ Վիկտորինան արդեն ընթացքի մեջ է:")
    
    qm.start_new_session(message.chat.id)
    await message.answer("🇦🇲 Պատրաստվե՛ք: Վիկտորինան սկսվում է...")
    await send_next_question(message.chat.id)

@dp.message(Command("nextquiz"))
async def cmd_next(message: types.Message):
    if not await is_admin(message): return
    await send_next_question(message.chat.id)

@dp.message(Command("stopquiz"))
async def cmd_stop(message: types.Message):
    if not await is_admin(message): return
    qm.stop_session(message.chat.id)
    await message.answer("🛑 Վիկտորինան դադարեցվեց:")

@dp.poll_answer()
async def handle_poll_answer(answer: PollAnswer):
    chat_id = poll_to_chat.get(answer.poll_id)
    if not chat_id: return
    
    session = qm.get_session(chat_id)
    if not session: return

    # Index logic: current_index is incremented AFTER sending
    current_q = session.questions[session.current_index - 1]
    
    if answer.option_ids[0] == current_q['answer']:
        user_id = answer.user.id
        session.scores[user_id] = session.scores.get(user_id, 0) + 1

async def main():
    logger.info("Bot is starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())