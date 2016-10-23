# -*- coding: utf-8 -*-
"""
Created on Sat Oct 22 18:56:32 2016
get data and write it to kafka
@author: ryan
"""
import win_inet_pton
import logging
import json
from flask import(
     Flask,
     jsonify
)
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from googlefinance import getQuotes
from kafka import KafkaProducer

#define logger format
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

app=Flask(__name__)
app.config.from_envvar('ENV_CONFIG_FILE')
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT']
kafka_topic = app.config['CONFIG_KAFKA_TOPIC']


#start the server
producer = KafkaProducer(bootstrap_servers=kafka_broker)
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()



stock_set = set()


#define the function to get stock price by stock name
def get_price(stock_name):
	try:
		#logger.debug('start to get stock price for %s', stock_name)
		stock_price = json.dumps(getQuotes(stock_name))
		#logger.debug('Get the price for %s',stock_name)
		producer.send(topic =kafka_topic, value=stock_price)
		logger.debug('finish write %s to kafka', stock_name)
	except Exception as e:
		logger.error('Failed to get the price for %s',stock_name)


#get_price('AAPL')
#schedule.add_job(get_price, 'interval',['AAPL'],seconds=1,id='AAPL')


def shut_down():
		producer.flush(10)
		producer.close()
		logger.info('shutdown kafka producer')
		schedule.shutdown()
		logger.info('shutdonw schedule')



#get default
@app.route('/', methods=['GET'])
def default():
	return jsonify('ok'),200


#add stock
@app.route('/<stock_name>', methods=['POST'])
def add_stock(stock_name):
	if not stock_name:
		return jsonify({
			'error:stock name is invalid'
			}),400
	if stock_name in stock_set:
		pass
	else:
		stock_set.add(stock_name)
		schedule.add_job(get_price, 'interval',[stock_name],seconds=1,id=stock_name)

	return jsonify(list(stock_set)), 200

#reomve stock
@app.route('/<stock_name>', methods=['DELETE'])
def remove_stock(stock_name):
	if not stock_name:
		return jsonify({
			'error:stock name is invalid'
			}),400
	if stock_name not in stock_set:
		pass
	else:
		stock_set.reomve(stock_name)
        schedule.remove_job(stock_name)
	return jsonify(list(stock_set)), 200


app.run(host ='0.0.0.0', port=80, debug=True)
atexit.register(shut_down)