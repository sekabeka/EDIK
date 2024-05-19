import asyncio
import logging
import os
import time

from aiogram.filters.command import Command
from aiogram import Bot, Dispatcher
from aiogram.types import Message, FSInputFile
from aiogram.methods import SendDocument, SendMessage
from aiogram import F
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')

logging.basicConfig(level=logging.INFO, filename='bot/log.log', filemode='w')


dp = Dispatcher()
bot = Bot(BOT_TOKEN)

def update_xlsx(name):
    import openpyxl
    import random
    import pandas as pd
    paths = [
        'child/child.xlsx',
        'auchan/auchan_result.xlsx',
        #'LETU/letu_result.xlsx'
    ]
    products = dict()
    for path in paths:
        try:
            p = pd.read_excel(path, sheet_name='result')
        except:
            try:
                p = pd.read_excel(path, sheet_name='products')
            except:
                break
        data = p.to_dict('list')
        for article, price, remain in zip(data['Артикул'],data['Цена закупки'], data['Остаток']):
            article = article.split('-')[-1]
            products[article] = (price, remain)

    wb = openpyxl.load_workbook(name)

    ws = wb['Загрузка']

    for line in ws.iter_rows(min_row=1):
        article = line[1].value
        try:
            article = article.replace('BN', '')
        except:
            continue

        if article in products.keys():
            price, remain = products.pop(article)
            if price != 'Нет в наличии':
                line[4].value = price
                line[5].value = remain
                formula = line[2].value
                percent = random.uniform(1.1, 1.3)
                if formula is not None:
                    line[3].value = '=(' + formula.replace('=', '').replace(',', '.') + ')' +  '*' + str(percent)
                else:
                    pass
            
            else:
                for idx in range (2, 6):
                    line[idx].value = 0

        else:
            pass

    wb.save(name)

def read(path):
    with open (path, 'r') as file:
        return file.read()


@dp.message(Command('table'))
async def sendTable(message:Message):
    path = 'generalTable.xlsx'
    await message.answer("Дата последней модификации файла: {}".format(time.ctime(os.path.getmtime(path))))
    await message.answer_document(FSInputFile(path, filename='Общая таблица.xlsx'))


@dp.message(F.document.file_name.casefold() == 'общая таблица.xlsx')
async def updateInputTable(message:Message):
    document_name = message.document.file_name
    await message.answer('Я получил Ваш файл: {}'.format(document_name))
    valid_path = "generalTable.xlsx"
    await bot.download(message.document, valid_path)
    await message.answer('Загрузили файл под названием: {}'.format(valid_path))
    await message.answer('Операция выполнена успешно. Таблица обновлена!')


@dp.message(F.content_type.in_({'document'}))
async def test(msg : Message):
    name = msg.document.file_name
    await msg.answer(f'Получили файл {name}')
    await bot.download(msg.document, name)
    update_xlsx(name)
    await msg.answer('Файл готов, смотрите:)')
    await msg.answer_document(FSInputFile(name))
    

@dp.message(Command('child'))
async def letu(message:Message):
    status = read('child/ready.txt')
    await message.answer(status)
    if status == 'Ready': 
        path = 'child/child.xlsx'
        await message.answer('Дата последнего изменения файла: {}'.format(time.ctime(os.path.getmtime(path))))
        await message.answer_document(
            FSInputFile(path)
        )
    else:
        await message.answer('Please Wait:)')

@dp.message(Command('auchan'))
async def letu(message:Message):
    status = read('auchan/ready.txt')
    await message.answer(status)
    if status == 'Ready': 
        path = "auchan/auchan_result.xlsx"
        await message.answer('Дата последнего изменения файла: {}'.format(time.ctime(os.path.getmtime(path))))
        await message.answer_document(
            FSInputFile(path)
        )
    else:
        await message.answer('Please Wait:)')


@dp.message(Command('red'))
async def letu(message:Message):
    status = read("red/ready.txt")
    await message.answer(status)
    if status == 'Ready': 
        path = 'red/red.xlsx'
        await message.answer('Дата последнего изменения файла: {}'.format(time.ctime(os.path.getmtime(path))))
        await message.answer_document(
            FSInputFile(path)
        )
    else:
        await message.answer('Please Wait:)')

async def main() -> None:
    await dp.start_polling(bot)



