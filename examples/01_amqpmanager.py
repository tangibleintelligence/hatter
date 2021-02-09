"""
Example showing raw usage of AMQPManager
"""
import asyncio

from hatter.amqp import AMQPManager


async def main():
    async with AMQPManager("rabbitmq.tangible.ai", "rabbitmq", "rabbitmq-pass", "finicity") as mgr:
        print(mgr)
        await mgr.tst()


if __name__ == "__main__":
    asyncio.run(main())
