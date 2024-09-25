import os
import asyncio

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.fsm.context import FSMContext
from aiogram.filters.command import Command
from aiogram.types import Message, FSInputFile
from aiogram.fsm.state import State, StatesGroup

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')

class SG(StatesGroup):
    update = State()

dp = Dispatcher()
bot = Bot(BOT_TOKEN)

@dp.message(Command('cancel'))
async def cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        'Состояние сброшено. Можете начать заново'
    )

@dp.message(Command('update'))
async def update_table(message: Message, state: FSMContext):
    await state.set_state(SG.update)
    await message.answer(
        'Пришлите таблицу\nЕсли передумали - напишите команду /cancel'
    )

@dp.message(SG.update)
async def update_table_state(message: Message, state: FSMContext):
    await state.clear()
    document = message.document
    await bot.download(document, 'src/input_table.xlsx')
    await message.answer(
        'Таблица успешно обновлена!'
    )

@dp.message(Command('download'))
async def download_table(message: Message):
    await message.answer_document(
        FSInputFile('src/input_table.xlsx')
    )
    await message.answer('Эта таблица используется сейчас парсерами. Изменяйте ее и обновляйте через команду /update')

# @dp.message(Command('logs'))
# async def get_logs(message: Message):
#     paths = [
#         'src/logs/%s' % f for f in ('child.log', 'auchan.log')
#     ]
#     print (paths)
#     for path in paths:
#         await message.answer_document(FSInputFile(path))
#     await message.answer('Все логи успешно доставлены!')

def send_result_table(id):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bot.send_document(str(id), FSInputFile('src/results/result.xlsx')))

def run():
    asyncio.run(dp.start_polling(bot))






