"""
Comm Manager and Comm Handlers
"""
from typing import Dict
from kernel_sidecar.handlers import Handler


class CommHandler(Handler):
    def __init__(self, target_name: str, comm_id: str):
        self.target_name = target_name
        self.comm_id = comm_id


class CommManager:
    _target_name_to_handler_cls = {}
    
    def __init__(self, target_name_to_handler_cls: Dict[str, CommHandler] = {}):
        self.comms = {}