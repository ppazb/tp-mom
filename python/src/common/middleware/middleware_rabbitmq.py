import pika
from .middleware import MessageMiddlewareQueue,MessageMiddlewareExchange,MessageMiddlewareMessageError,MessageMiddlewareDisconnectedError,MessageMiddlewareCloseError


#Funcion auxiliar para evitar repetir codigo en las exception en todas las funciones 
def handle_error(funcion, e):
    if isinstance(e, pika.exceptions.AMQPConnectionError):
        raise MessageMiddlewareDisconnectedError(f"Error {funcion}: {e}")
    
    raise MessageMiddlewareMessageError(f"Error {funcion}: {e}")


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host = host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue = queue_name)

        except Exception as e:
            handle_error("Init", e)

    def send(self, message):
        try:
            self.channel.basic_publish(exchange = '', routing_key = self.queue_name, body = message)

        except Exception as e:
            handle_error("send", e)  

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag = method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag = method.delivery_tag)

            try:
                on_message_callback(body, ack, nack)

            except Exception as e:
                handle_error("Start_consuming - callback", e)  

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback = callback)
            self.channel.start_consuming()

        except Exception as e:
            handle_error("Start_consumiung", e)  

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()

        except Exception as e:
            handle_error("Stop_consuming", e)  

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()

        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error close: {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host = host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange = exchange_name, exchange_type ='direct')

        except Exception as e:
            handle_error("Init", e)  

    def send(self, message):
        routing_keys = self.routing_keys
        try:
            for key in routing_keys:
                self.channel.basic_publish(exchange = self.exchange_name,routing_key= key, body = message)

        except Exception as e:
            handle_error("send", e)  

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag = method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag = method.delivery_tag)

            try:
                on_message_callback(body, ack, nack)

            except Exception as e:
                handle_error("Start_consuming - callback", e)  

        try:
            result = self.channel.queue_declare(queue='', exclusive = True)
            self.queue_name = result.method.queue

            for key in self.routing_keys:
                self.channel.queue_bind(queue = self.queue_name,  exchange = self.exchange_name, routing_key = key)

            self.channel.basic_consume(queue = self.queue_name, on_message_callback = callback)
            self.channel.start_consuming()

        except Exception as e:
            handle_error("start_consuming", e)  

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()

        except Exception as e:
            handle_error("stop_consuming", e)  

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()

        except Exception as e:
            #error especifico, no handleo con func auxiliar para no complejizar la funcion de handle_error
            raise MessageMiddlewareCloseError(f"Error close: {e}")