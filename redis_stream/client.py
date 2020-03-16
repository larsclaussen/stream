"""
The fairly recent redis stream feature basically knows three query modes

1. fan out messages to multiple clients that multiple consumers can see 
2. time series store, that query messages by time ranges 
3. a stream of messages that can be partitioned to multiple consumers 
   that are processing such messages, so that groups of consumers can only see a 
   subset of the messages
"""

from abc import ABCMeta, abstractmethod
import asyncio
import logging
from typing import Dict, List

from aioredis.errors import ReplyError
from aioredis import create_redis_pool
import namesgenerator
from pydantic import BaseModel

from models import StreamModel
from models import FlowVelocityModel
from models import IOData, FlowInputData, FlowOutputData


logger = logging.getLogger(__name__)


async def exclusive_redis(host, port=6379, db=0):
    """
    use an exclusive connection for blocking operations, like so:

    client = await exclusive_redis(host)
    with await client as cli:
        item = await cli.blpop('key')

    """
    return await create_redis_pool(
        f"redis://:@{host}:{port}/{db}", minsize=1, maxsize=1,
        encoding="utf-8"
    )


class StreamABC(metaclass=ABCMeta):

    def __init__(self, stream_model: BaseModel, **redis_kwargs):
        self._client = None
        self._redis_kwargs = redis_kwargs
        self._is_initialized = False
        self._known_streams = {}  #? Needed?
        self.model = stream_model

    @abstractmethod
    async def initialize(self) -> None:
        """setup related code goes here"""
        pass

    @property
    async def length(self) -> int:
        info = await self.get_info()
        if not info:
            return 0
        return info["length"]

    @property
    async def groups(self) -> int:
        info = await self.get_info()
        if not info:
            return 0
        return info["groups"]

    @property
    async def last_generated_id(self) -> str:
        info = await self.get_info()
        if not info:
            return ""
        return info["last-generated-id"]

    @property
    async def first_entry(self) -> List:
        info = await self.get_info()
        if not info:
            return []
        return info["first-entry"]

    @property
    async def last_entry(self) -> List:
        info = await self.get_info()
        if not info:
            return []
        return info["last-entry"]

    async def get_info(self) -> Dict:
        try:
            return await self._client.xinfo_stream(self.model.name)
        except ReplyError:
            logger.error(f"No stream with name {self.model.name}")
            pass


# class StreamWriterABC(StreamABC):

#     @abstractmethod
#     async def append_to(self, value: float) -> str:
#         """
#         Arguments:
#             value: The value to append to the stream.

#         Returns:
#             id of the stream item.

#         """
#         pass

#     @abstractmethod
#     async def _xadd(
#         self,
#         data: Dict,
#         custom_id: str = "",
#         max_items: int = None,
#         trim_items_exactly: bool = False
#     ):
#         """
#         Wrapper around the the aioredis `xadd()` function

#         Arguments:
#             value: The value to append to the stream.
#             custom_id: Use a custom id for the stream message.
#             max_items: The maximum count of stream items, the rest will
#                be trimmed
#             trim_items_exactly: default False which means the max_items
#                count and the actual count may differ and the trimming is
#                performed only when a whole whole node can be removed.
#                This makes it much more efficient, and it is usually what
#                you want.

#         Returns:
#             id of the stream item.

#         """
#         pass


# class StreamReaderABC(StreamABC):

#     @abstractmethod
#     async def read(self, *args, **kwargs) -> str:
#         """
#         Arguments:
#             value: The value to append to the stream.

#         Returns:
#             id of the stream item.

#         """
#         pass




class BaseStream(StreamABC):

    def __init__(self, stream_model: StreamModel, **redis_kwargs: Dict):
        super().__init__(stream_model, **redis_kwargs)
        
    async def initialize(self):
        if self._is_initialized:
            logger.warning(
                "stream %s is already initialized", self.model.name
            )
            return
        self._client = await exclusive_redis(
            **self._redis_kwargs
        )
        self.id_field_name = f"{self.model.id_type}-id"
        self._is_initialized = True

    @property
    async def stream_name(self):
        return self.model.name

    async def _stream_entry_to_data_model(self, entry):
        return FlowOutputData(
            stream_name=self.model.name, 
            id=entry[0], 
            data=entry[1]
        )


class ConsumerGroup(BaseStream):

    def __init__(
        self, 
        stream_model: StreamModel, 
        group_name: str, 
        start_id: str = "",
        **redis_kwargs: Dict
    ):
        super().__init__(stream_model, **redis_kwargs)
        self.group_name = group_name
        self.consumers = set()
        self.start_id = start_id if start_id else "$"

    async def initialize(self):
        await super().initialize()
        await self._create()

    async def _create(self):
        
        # todo should we support the mkstream option?
        await self._client.xgroup_create(
            self.model.name, self.group_name, latest_id=self.start_id, 
            mkstream=False
        )

    async def add_consumer(self, name: str = ""):
        if not name:
            name = namesgenerator.get_random_name()
        self.consumers.add(name)
        return name

    async def _get_consumer(self, consumer: str = ""):
        if not consumer:
            logger.info("no consumer provided, will auto-generated one...")
            consumer = await self.add_consumer()
            logger.info("auto-generated %s consumer", consumer)
            return consumer

        if consumer not in self.consumers:
            logger.info("consumer %s not yet known, will be added...", consumer)
            self.add_consumer(consumer)
        return consumer
        
    async def listen(self, consumer: str = "", max_items: int = 1): 
        consumer = await self._get_consumer(consumer)
        while True:
            entries = await self._client.xread_group(
                self.group_name, consumer, [self.model.name], timeout=0, count=max_items, 
                latest_ids=[">"], no_ack=True)
            print(f"New entries {entries} for consumer {consumer}")

    async def read(
        self, 
        consumer: str = "", 
        max_items: int = 1, 
        new_items_only: bool = True, 
        historical_start_id: str = ""):
        """
        XREADGROUP is a write command because even if it reads 
        from the stream, the consumer group is modified as a side effect of 
        reading, so it can be only called in master instances.        
        """
        try:
            assert not all((new_items_only, historical_start_id)), "either or not both"
        except AssertionError as err:
            raise AttributeError(err)
        consumer = await self._get_consumer(consumer)

        _id = [">"] if new_items_only else [historical_start_id]

        # entries = await self._client.execute(
        #     "XREADGROUP", "GROUP", self.group_name, consumer, "COUNT", max_items, "STREAMS", 
        #     self.model.name, _id) 
        entries = await self._client.xread_group(
            self.group_name, consumer, [self.model.name], timeout=1, count=max_items, 
            latest_ids=_id, no_ack=False)
        return [await self._stream_entry_to_data_model(entry[1:]) for entry in entries]


# xread_group(group_name, consumer_name, streams, timeout=0, count=None, latest_ids=None, no_ack=False)

# XREADGROUP GROUP mygroup Alice COUNT 1 STREAMS mystream

# should become producer
class FlowVelocityStream(BaseStream):

    def __init__(self, stream_model, **redis_kwargs):
        super().__init__(stream_model, **redis_kwargs)
        
    async def initialize(self):
        if self._is_initialized:
            logger.warning(
                "stream %s is already initialized", self.model.name
            )
            return
        self._client = await exclusive_redis(
            **self._redis_kwargs
        )
        self.id_field_name = f"{self.model.id_type}-id"
        self._is_initialized = True

    async def _xadd(
        self,
        data: Dict,
        custom_id: str = '*',
        max_items: int = None,
        trim_items_exactly: bool = False
    ):
        return await self._client.xadd(
            self.model.name, data, custom_id, 
            max_items, trim_items_exactly
        )

    async def append_to(self, value: float):
        if not self._is_initialized:
            await self.initialize()
        # todo check if we wanna use a input dataclass
        data = {
            self.id_field_name: self.model.id,  
            self.model.field_name: value
        }
        await self._xadd(data)

    # async def read_stream(
    #     self,
    #     max_items: int = None,
    #     start_id: str = "",
    #     stop_id: str = "",
    # ) -> List:
    #     """
    #     reads stream data, in this form very similar to xrange
    #         xrange(stream, start='-', stop='+', count=None)
    #     """
    #     # specifically None to avoid `aioredis` to create `BLOCK` argument
    #     timeout = None
    #     if start_id:
    #         latest_ids = [start_id]
    #     else:
    #         first_entry = await self.first_entry
    #         if not first_entry:
    #             logger.info("No stream data available")
    #             return
    #         latest_ids = [first_entry[0]]
            
    #     entries = await self._client.xread(
    #         [self.model.name],
    #         timeout=timeout,
    #         count=max_items,
    #         latest_ids=latest_ids
    #     )
    #     # _tasks = []
    #     # for entry in entries:
    #     #     _task = asyncio.ensure_future(
    #     #         self._stream_entry_to_data_model(entry)
    #     #         )
    #     #     _tasks.append(_task)
        
    #     # results = asyncio.gather(*_tasks, return_exceptions=True)
    #     # return results
    #     return [await self._stream_entry_to_data_model(entry) for entry in entries]


    async def read_stream(
        self,
        max_items: int = None,
        start_id: str = "",
        stop_id: str = "",
        reverse: bool = False
    ) -> List:
        """
        reads stream data, in this form very similar to xrange
            xrange(stream, start='-', stop='+', count=None)
        """
        # specifically None to avoid `aioredis` to create `BLOCK` argument
        
        start_id = start_id if start_id else "-"    
        stop_id = stop_id if stop_id else "+"    
        if reverse:
            entries = await self._client.xrevrange(
                self.model.name,
                start=stop_id,
                stop=start_id,
                count=max_items
            )
        else:
            entries = await self._client.xrange(
                self.model.name,
                start=start_id,
                stop=stop_id,
                count=max_items
            )
        # _tasks = []
        # for entry in entries:
        #     _task = asyncio.ensure_future(
        #         self._stream_entry_to_data_model(entry)
        #         )
        #     _tasks.append(_task)
        
        # results = asyncio.gather(*_tasks, return_exceptions=True)
        # return results
        return [await self._stream_entry_to_data_model(entry) for entry in entries]



    # todo        
    async def listen(
        self,
        listen_for: int = 0  # forever
    ) -> List:
        """
        """
        # specifically None to avoid `aioredis` to create `BLOCK` argument
        entries = await self._client.xread(
            [self.model.name],
            timeout=timeout,
            count=cnt_items,
            latest_ids=latest_ids
        )
        tasks = [asyncio.ensure_future(
            self._entry_to_data_model(entry)) for entry in entries]
        results = asyncio.gather(*tasks, return_exceptions=True)
        return results


# from client import FlowVelocityStream
# from client import ConsumerGroup
# from models import FlowVelocityModel
# from models import TestModel
# redis_kw = {"host": "redis"}
# # m = FlowVelocityModel(id=12)
# tm = TestModel(id=3)
# f = FlowVelocityStream(tm, **redis_kw)
# await f.initialize()
# tm = TestModel(id=3)
# c = ConsumerGroup(tm, 'puddel', **redis_kw)
# await c.initialize()
# await c.listen()
