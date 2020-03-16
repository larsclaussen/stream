from collections import OrderedDict
from pydantic import BaseModel


class StreamModel(BaseModel):
    name: str
    id_type: str
    id: int
    field_name: str


class TestModel(StreamModel):
    name = "test_stream"
    id_type = "test"
    id: int
    field_name = "test"


class FlowVelocityModel(StreamModel):
    name = "flow_stream"
    id_type = "simulation"
    id: int
    field_name = "s1"


class IData(BaseModel):
    identifier: str
    value: float


class IOData(BaseModel):
    field_name: str
    value: float


class FlowInputData(BaseModel):
    id_input: IOData
    value_input: IOData


class FlowOutputData(BaseModel):
    stream_name: str
    id: str
    data: OrderedDict
