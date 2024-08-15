import pika
import subprocess
import threading
import time

class crawlerThread(threading.Thread):
    def __init__(self, args):
        threading.Thread.__init__(self)
        self.args = args

    def run(self):
        subprocess.run(self.args, capture_output=True, text=True)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

# 指定接收消息的queue
channel.queue_declare(queue='MediaCrawler')


def on_request(ch, method, props, body):
    print(f"crawler request : {body}")
    # 将接收到的字节序列解码为字符串
    body_str = body.decode('utf-8')
    # 现在 body_str 是字符串类型，可以安全地使用 split 方法
    args = body_str.split(" ")
    # 获取虚拟环境中 Python 解释器的路径
    venv_python = "d:/study/grade6/shixi/social/MediaCrawler/venv/Scripts/python.exe"
    args[0] = venv_python
    mycrawler = crawlerThread(args)
    mycrawler.start()
    ch.basic_publish(exchange='',  # 使用默认交换机
                     routing_key=props.reply_to,  # response发送到该queue
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id),  # 使用correlation_id让此response与请求消息对应起来
                     body="ok")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
# 从rpc_queue中取消息，然后使用on_request进行处理
channel.basic_consume(queue='MediaCrawler', on_message_callback=on_request)

print(" [x] Awaiting crawler requests")
channel.start_consuming()