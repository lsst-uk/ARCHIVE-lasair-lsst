from __future__ import print_function
import io
import time
import confluent_kafka
from ast import literal_eval
import fastavro


__all__ = ['EopError', 'AlertConsumer']


class AlertError(Exception):
    """Base class for exceptions in this module.
    """
    pass


class EopError(AlertError):
    """Exception raised when reaching end of partition.

    Parameters
    ----------
    msg : Kafka message
        The Kafka message result from consumer.poll().
    """
    def __init__(self, msg):
        """__init__.

        Args:
            msg:
        """
        self.message = 'EopError'

    def __str__(self):
        """__str__.
        """
        return self.message

class AlertConsumer(object):
    """Creates an alert stream Kafka consumer for a given topic.

    Parameters
    ----------
    topic : `str`
        Name of the topic to subscribe to.
    **kwargs
        Keyword arguments for configuring confluent_kafka.Consumer().
    """

    def __init__(self, topic, **kwargs):
        """__init__.

        Args:
            topic:
            kwargs:
        """
        self.topic = topic
        self.kafka_kwargs = kwargs
        self.raw_msg = None  # added by RDW to keep raw msg

    def __enter__(self):
        """__enter__.
        """
        self.consumer = confluent_kafka.Consumer(**self.kafka_kwargs)
        self.consumer.subscribe([self.topic])
        return self

    def __exit__(self, type, value, traceback):
        """__exit__.

        Args:
            type:
            value:
            traceback:
        """
        # FIXME should be properly handling exceptions here, but we aren't
        self.consumer.close()

    def poll(self, decode=False, verbose=True, timeout=1):
        """Polls Kafka broker to consume topic.

        Parameters
        ----------
        decode : `boolean`
            If True, decodes data from Avro format.
        verbose : `boolean`
            If True, returns every message. If False, only raises EopError.
        """
        msg = self.consumer.poll(timeout=timeout)

        if msg:
            if msg.error():
                raise EopError(msg)
            else:
                self.raw_msg = msg.value()   # added by RDW to keep raw msg
                if verbose is True:
                    if decode is True:
                        return self.decodeMessage(msg)
                    else:
                        ast_msg = literal_eval(str(msg.value(), encoding='utf-8'))
                        return ast_msg
        else:
            try:
                raise EopError(msg)
            except AttributeError:
                pass
        return

    def decodeMessage(self, msg):
        """Decode Avro message.

        Parameters
        ----------
        msg : Kafka message
            The Kafka message result from consumer.poll().

        Returns
        -------
        `dict`
            Decoded message.
        """
        message = msg.value()
        bytes_io = io.BytesIO(message)
        decoded_msg = fastavro.reader(bytes_io)
        return decoded_msg
