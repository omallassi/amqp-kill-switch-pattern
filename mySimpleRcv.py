#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from __future__ import print_function
import optparse
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import Delivery

import threading
import time
import httplib
import json

import pybreaker

import plotly
import plotly.graph_objs as go
from plotly.graph_objs import Scatter, Layout
import pandas as pd
import os
import random

class DownstreamService:

    def doStuff(self, num):
	time.sleep(0); #wait for 1 sec to simulate "processing"
	healthcheck = self.healthcheck()

	if healthcheck == "True":
		print('Service is being called - {}'.format(num)) 
	elif healthcheck == "False":
		# Simulate exception
		raise Exception('Downstream Svc Unreachable...')
	else:
		print("unknown value")

    def healthcheck(self):
	f = open('mySimpleRcv.config', 'r')
	line = f.readline().rstrip("\n")
	f.close()

	return line;

    def set_healthcheck(self, value=True):
	f = open('mySimpleRcv.config', 'w')
	f.write("{}".format(value))
	f.close()


class LogListener(pybreaker.CircuitBreakerListener):
	def __init__(self, recvHandler):
		self.recvHandler = recvHandler
		self.t = None

	def state_change(self, cb, old_state, new_state):
		print('CircuitBreaker State is {}'.format(new_state))
		#if cb.current_state == "closed" or cb.current_state == "half-open": 
		#	print('re-opening session')
			#self.rcv.open()
		#	self.rcv = self.container.create_receiver(self.url)
		if cb.current_state == "open":
			print('closing session')
			self.recvHandler.switch_off()	# does not close the underlying TCP connection


class Recv(MessagingHandler):

    def __init__(self, url, count):
        super(Recv, self).__init__(auto_accept=False)
        self.url = url
        self.expected = count
        self.received = 0
	self.unique_received = 0
	self.duplicated = 0
	self.dlq = 0
	self.rejected = 0
	self.breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=10)
	self.stats = {}
	self.stats_filename = "./mySimpleRcvStats.csv"
	if os.path.exists(self.stats_filename):
		os.remove(self.stats_filename)


    def switch_off(self):
	self.rcv.close()
	# need to "manually" reopen the CricuitBreaker so that state_change is called again
	#threading.Timer(self.breaker.reset_timeout, force_state_to_half_open, ()).start()
	threading.Timer(self.breaker.reset_timeout, self.reopen_breaker).start()

    def reopen_breaker(self):
	#check downstream healthcheck service
	healthcheck = self.svc.healthcheck()
	if healthcheck == "True":
		#self.breaker.half_open()
		self.rcv = self.container.create_receiver(self.url)
		print('re-opening session')
	else:
		print('Downstream Circuit Breaker still open...')
		threading.Timer(self.breaker.reset_timeout, self.reopen_breaker).start()
	

    def on_start(self, event):
	self.svc = DownstreamService()
	self.container = event.container
        self.rcv = event.container.create_receiver(self.url)
	self.breaker.add_listeners(LogListener(self))
	print('start scheduler')
	#self.isSwitchedOff = False
	self.start_metrics_logger()
	self.start_monkey()


    def on_message(self, event):
        #if event.message.id and event.message.id < self.received:
            # ignore duplicate message
        #    print("could be a duplicate")
        #if self.expected == 0 or self.received < self.expected:
            #print(event.message.body)
            #print(event.message.delivery_count)
	    try: 
            # for message to be rejected
            #print(event.delivery)
		self.breaker.call(self.svc.doStuff, self.received)
            	self.accept(event.delivery)    # delete message from queue (broker side)
		self.received += 1
		if event.message.delivery_count > 0:
			self.duplicated += 1
		else:
			self.unique_received += 1
            #self.settle(event.delivery)     # do not delete the message from the queue but mark it as Acquired

	    # All events are rejected as if the downstream service was down
	    except: 
            	self.reject(event.delivery)
		self.rejected += 1

	    # exit criteria (could have duplicate messages) 
	    #if self.unique_received == self.expected:
            #    event.receiver.close()
            #    event.connection.close()

    def plot_stats(self):
	df = pd.read_csv(self.stats_filename, names = ['time', 'queue_in', 'queue_out', 'msg_depth', 'dlq_in', 'dlq_out', 'dql_msg_depth'])
	df = df.drop(df.index[[0,1]]) # the first stat is like 1M messages... delete the anomaly

	ingress = go.Scatter(
	          x=df['time'],
	          y=df['queue_in'],
		  mode = 'lines+markers',
    		  name = 'inbound msg/on sec queue')

	outgress = go.Scatter(
	          x=df['time'],
	          y=df['queue_out'],
		  mode = 'lines+markers',
    		  name = 'outbound msg/sec on queue')

	dlq_ingress = go.Scatter(
	          x=df['time'],
	          y=df['dlq_in'],
		  mode = 'lines+markers',
    		  name = 'inbound msg/sec on DLQ')
	
	data = [ingress, outgress, dlq_ingress]
	plotly.offline.plot(data, filename='thtoughtputStats.html')

	#msg_depth = go.Scatter(
	#          x=df['time'],
	#          y=df['msg_depth'],
	#	  mode = 'lines+markers',
    	#	  name = 'Message Depth on queue')

	dlq_msg_depth = go.Scatter(
	          x=df['time'],
	          y=df['dql_msg_depth'],
		  mode = 'lines+markers',
    		  name = 'Message Depth on DLQ')
	
	data = [dlq_msg_depth]
	plotly.offline.plot(data, filename='MessageDepthStats.html')
	

    def display_stats(self):
	print('********************************************************************************************')
	print('Received Messages: {} (of {} expected)'.format(self.received, self.expected))
	print('Replayed Messages: {}'.format(self.duplicated))
	h1 = httplib.HTTPConnection('localhost', 8080)
	h1.request("GET", "/api/latest/queue/default/default/queue1_w_dlq_DLQ")
	r1 = h1.getresponse()
	r1_as_json = json.loads(r1.read())
	self.dlq = r1_as_json[0]['statistics']['queueDepthMessages']
	print('DLQ messages: {}'.format(self.dlq))
	print('Total messages (w/o duplicate) {}'.format(self.unique_received + self.dlq))
    
	self.plot_stats()


    def start_monkey(self):
	is_service_ok = random.choice([True, False])
	self.svc.set_healthcheck(is_service_ok)
	print('****************************************************** Monkey is running = {}'.format(is_service_ok))
	wait_time = random.randint(20, 30)
	threading.Timer(wait_time, self.start_monkey).start()

    def start_metrics_logger(self):
	h1 = httplib.HTTPConnection('localhost', 8080)
	urls = ["/api/latest/queue/default/default/queue1_w_dlq?excludeInheritedContext=true&depth=1", "/api/latest/queue/default/default/queue1_w_dlq_DLQ?excludeInheritedContext=true&depth=1"]
	line = '{}'.format(time.time())
	for url in urls:
		h1.request("GET", url)
		r1 = h1.getresponse()
		r1_as_json = json.loads(r1.read())

		inbound = r1_as_json[0]['statistics']['totalEnqueuedMessages']
		outbound = r1_as_json[0]['statistics']['totalDequeuedMessages']

		line += ',{}'.format(inbound - self.stats.get(url + '-enqueuedMessage', 0))
		line += ',{}'.format(outbound - self.stats.get(url + '-dequeuedMessage', 0))
		line += ',{}'.format(r1_as_json[0]['statistics']['queueDepthMessages'])

		self.stats[url + '-enqueuedMessage'] = inbound
		self.stats[url + '-dequeuedMessage'] = outbound

	line += '\n'

	with open(self.stats_filename, 'a') as f:
    		f.write(line)

#	print(self.breaker.current_state)
#	healthcheck = self.svc.healthcheck()
#	if healthcheck == "open": 
#		if self.isSwitchedOff:
#			print('re-opening session')
#			#self.rcv.open()
#			self.rcv = self.container.create_receiver(self.url)
#			self.isSwitchedOff = False
#	elif healthcheck == "close":
#		if not self.isSwitchedOff:
#			print('closing session')
#			self.rcv.close()	# does not close the underlying TCP connection
#			self.isSwitchedOff = True
#	else:
#		print('unknown value')
	threading.Timer(1, self.start_metrics_logger).start()


parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-a", "--address", default="localhost:5672/examples",
                  help="address from which messages are received (default %default)")
parser.add_option("-m", "--messages", type="int", default=100,
                  help="number of messages to receive; 0 receives indefinitely (default %default)")
opts, args = parser.parse_args()

recv = None
try:
    print('start container')
    recv = Recv(opts.address, opts.messages)
    Container(recv).run()

except KeyboardInterrupt:
    recv.display_stats() 
    pass
