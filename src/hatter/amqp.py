"""
Object to manage RabbitMQ connection/channels/threads via amqpstorm
"""
import threading
import time
from logging import getLogger
from typing import Optional, Callable

import amqpstorm

# TODO clearcut logging
from hatter.domain import HatterMessage

logger = getLogger(__name__)


class AMQPManager:
    """
    Creates and manages a connection to interact with a RabbitMQ instance, and creates threads as needed to listen to queues.
    """

    def __init__(
            self,
            rabbitmq_host: str,
            rabbitmq_user: str,
            rabbitmq_pass: str,
            rabbitmq_virtual_host: str = '/',
            connection_retries = 3
    ):
        self.termination_commenced = threading.Event()  # set once we have been asked to terminate. Allows for graceful shutdown.
        self.termination_exception: Optional[Exception] = None

        self._rabbitmq_host = rabbitmq_host
        self._rabbitmq_user = rabbitmq_user
        self._rabbitmq_pass = rabbitmq_pass
        self._rabbitmq_virtual_host = rabbitmq_virtual_host
        self._connection_retries = connection_retries

        self.publish_channel: amqpstorm.Channel = None

    def __enter__(self):
        # Go ahead and init a connection.
        self._create_connection()

        # Create a channel for publishing messages. Additional channels will be opened for consumption threads.
        self.publish_channel = self.connection.channel()
        self.publish_channel.confirm_deliveries()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commence_termination()
        if self._connection is not None:
            self._connection.close()

    def _create_connection(self):
        """Create a connection.
        :return:
        """
        attempts = 0
        while True:
            attempts += 1
            if self.termination_commenced.is_set():
                break
            try:
                self._connection = amqpstorm.Connection(
                    self._rabbitmq_host,
                    self._rabbitmq_user,
                    self._rabbitmq_pass,
                    virtual_host = self._rabbitmq_virtual_host
                )
                logger.info("New connection established with RabbitMQ")
                break
            except amqpstorm.AMQPError as e:
                wait_time = min(attempts * 2, 30)
                logger.warning(f"error connecting to RabbitMQ. Waiting {wait_time} seconds.", exc_info = e)
                if attempts > self._connection_retries:
                    raise Exception('max number of retries reached', e)
                time.sleep(wait_time)
            except KeyboardInterrupt:
                break

    @property
    def connection(self) -> amqpstorm.Connection:
        """
        Generally for internal use; provides the amqpstorm `Connection` object, initializing the connection if needed..
        """
        if self.termination_commenced.is_set():
            logger.warning("Attempting to access connection after termination has commenced.")
            # noinspection PyTypeChecker
            return

        if not self._connection or self._connection.is_closed:
            self._create_connection()
        try:
            # Check our connection for errors.
            self._connection.check_for_errors()
            if not self._connection.is_open:
                raise amqpstorm.AMQPConnectionError('connection closed immediately after open attempt')

            return self._connection

        except amqpstorm.AMQPError as e:
            logger.error("error opening connection", exc_info = e)
            raise e

    def publish(self, hatter_message: HatterMessage):
        """
        Publishes the provided message object; exchange and other routing information should be provided within the object itself.
        """
        if hatter_message.destination_exchange is None:
            # TODO handle default exchange situation
            raise ValueError("Destination exchange not specified")

        amqp_message: amqpstorm.Message = amqpstorm.Message(self.publish_channel, hatter_message.data)  # TODO handle props like ttl
        amqp_message.publish(...)  # TODO impl

    def start_listener(self, func: Callable, queue_name: str):
        """
        Spins up a thread, which consumes from queue_name and calls func with any received message.
        """
        try:
            consume_channel = self.connection.channel()
            consume_channel.confirm_deliveries()
            consume_channel.basic.qos(1)

    def consume_all(self):
        """
        Blocks, consuming from all enabled listener threads. Raises an exception on internal, unrecoverable failure.
        """
        logger.info("Consuming from all channels.")

        try:
            # One of three things will interrupt this .wait call.
            # 1. External (k8s, user, etc.) kills the process with a SIGTERM. In this case, a KeyboardInterrupt will be raised and we should set the flag
            #    so everyone shuts down nicely.
            # 2. Internal (connection failure, unrecoverable code error, etc.). In this case, `.wait()` will return True, and we should raise a RuntimeError
            # 3. Internal, intentionally (i.e. a KeyboardInterrupt was raised within a thread). In this case, the termination_exception variable will be
            #    KeyboardInterrupt, and we can exit slightly more quietly
            self.termination_commenced.wait()

            # If here, then 2 or 3 happened.
            if isinstance(self.termination_exception, KeyboardInterrupt):
                logger.info("Exiting gracefully upon internal request.")
                return
            else:
                raise RuntimeError("Consumption failed internally. Death requested.")

        except KeyboardInterrupt:
            # If here, then "1. External" happened.
            logger.info("Consumption interrupted externally. Commencing termination as normal.")
            self.commence_termination()

    def commence_termination(self, exception: Optional[Exception] = None):
        """
        Internally triggers termination to commence. If an exception is provided, this will be used as a reason for the termination, with the assumption that
        we are terminating due to an error.
        """
        self.termination_commenced.set()
        self.termination_exception = exception
