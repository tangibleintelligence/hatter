import pytest

from conftest import RMQAggregator
from hatter.domain import HatterMessage


@pytest.mark.asyncio
async def test_simple_publish(rmq_aggregator):
    # Publish a simple message to a queue
    sample_data = "hi there"
    publish_message = HatterMessage(data=sample_data, destination_queue=RMQAggregator.Destination.sample_queue.value)
    await rmq_aggregator.publish(publish_message)

    # Did it arrive?
    received_data = await rmq_aggregator.get(RMQAggregator.Destination.sample_queue)
    assert received_data == sample_data
