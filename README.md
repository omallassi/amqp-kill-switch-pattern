using the amqp qpid sender 

python simpleSend.py -a localhost:5672/queue1_w_dlq -m 500000


on consumer side
python mySimpleRcv.py -a localhost:5672/queue1_w_dlq -m 500000
