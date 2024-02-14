from datetime import datetime
from bitquery import bitquery
from dotenv import load_dotenv
from pg_functions import insert_token_db
import os, json

def get_from_bitquery(query, since, till, limit): 
  load_dotenv()
  #Достаём свой ключь из переменок среды
  AKEY =os.getenv('API_KEY')
  #Преобразуем даты в правлиьный формат
  since_str = datetime.strptime(since, "%Y-%m-%d").strftime("%Y-%m-%d")
  till_str = datetime.strptime(till, "%Y-%m-%d").strftime("%Y-%m-%d")
  #Редактируем запрос
  query = query % (since_str, till_str, limit)
  #Отправляем запрос
  response = bitquery.run_query(AKEY, query)
  #Ответ запроса отправляем в БД
  insert_token_db(since,till,response)
  return response


since = "2024-01-22"
till = "2024-01-27"
limit = 100
#Пример запроса
query = """
query MyQuery {
  ethereum(network: bsc) {
    dexTrades(
      time: {since: "%s", till: "%s"}
      options: {limit: %d, desc: "quoteAmount"}
    ) {
      count
      baseCurrency {
        address
        symbol
        name
      }
      quoteAmount(in: USDT)
      baseAmount
    }
  }
}
"""
print( json.loads(query))
#пример вызова функции
#get_from_bitquery(query, since, till, limit)