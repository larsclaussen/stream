from abc import ABCMeta, abstractmethod
import asyncio
import logging
from typing import Dict, List

from aioredis.errors import ReplyError
from aioredis import create_redis_pool
from pydantic import BaseModel
from .models import StreamModel
from .models import FlowVelocityModel
from .models import IOData, FlowInputData, FlowOutputData


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

    @abstractmethod
    async def append_to(self, value: float) -> str:
        """
        Arguments:
            value: The value to append to the stream.

        Returns:
            id of the stream item.

        """
        pass

    @abstractmethod
    async def _xadd(
        self,
        data: Dict,
        custom_id: str = "",
        max_items: int = None,
        trim_items_exactly: bool = False
    ):
        """
        Wrapper around the the aioredis `xadd()` function

        Arguments:
            value: The value to append to the stream.
            custom_id: Use a custom id for the stream message.
            max_items: The maximum count of stream items, the rest will
               be trimmed
            trim_items_exactly: default False which means the max_items
               count and the actual count may differ and the trimming is
               performed only when a whole whole node can be removed.
               This makes it much more efficient, and it is usually what
               you want.

        Returns:
            id of the stream item.

        """
        pass

    @property
    async def length(self) -> int:
        info = await self._get_info()
        if not info:
            return 0
        return info["length"]

    # todo
    @property
    async def groups(self):
        info = await self._get_info()
        return info["groups"]

    @property
    async def last_generated_id(self) -> str:
        info = await self._get_info()
        if not info:
            return ""
        return info["last-generated-id"]

    @property
    async def first_entry(self) -> List:
        info = await self._get_info()
        if not info:
            return []
        return info["first-entry"]

    @property
    async def last_entry(self) -> List:
        info = await self._get_info()
        if not info:
            return []
        return info["last-entry"]

    async def _get_info(self) -> Dict:
        try:
            return await self._client.xinfo_stream(self.model.name)
        except ReplyError:
            logger.error(f"No stream with name {self.model.name}")
            pass


class FlowVelocityStream(StreamABC):

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
        self.id_field_name = f"{self.model.id_type}-{self.model.id}"
        self._is_initialized = True

    async def _xadd(
        self,
        data: Dict,
        custom_id: str = "",
        max_items: int = None,
        trim_items_exactly: bool = False
    ):
        return await self._client.xadd(self.model.name, **data)

    async def append_to(self, value: float):
        if not self._is_initialized:
            await self.initialize()
        data = FlowInputData(
            id_input=IOData(
                self.id_field_name, self.model.id 
            ),
            value_input=IOData(
                self.model.field_name, value
            )
        )
        await self._xadd(data.dict())

    async def read(
        self,
        cnt_items: int = None,
        start_id: str = "",
        block_for: int = 0
    ) -> List:
        """

        :param cnt_items:
        :param start_id:
        :param block_for:
        :return:
        """
        # specifically None to avoid `aioredis` to create `BLOCK` argument
        timeout = block_for if block_for else None
        latest_ids = [start_id] if start_id else None
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

    async def _entry_to_data_model(self, entry):
        return FlowOutputData(entry)
        
        
