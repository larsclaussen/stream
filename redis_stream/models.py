from pydantic import BaseModel


class StreamModel(BaseModel):
    name: str
    id_type: str
    id: int
    field_name: str


class FlowVelocityModel(StreamModel):
    name = "flow_stream"
    id_type = "simulation"
    id: int
    field_name = "s1"


class IOData(BaseModel):
    field_name: str
    value: float


class FlowInputData(BaseModel):
    id_input: IOData
    value_input: IOData


class FlowOutputData(BaseModel):
    id: str
    id_output: IOData
    value_output: IOData
