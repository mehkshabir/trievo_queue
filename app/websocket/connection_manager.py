import json
import logging
from typing import Dict, List, Optional
from fastapi import WebSocket

logger = logging.getLogger("trievo.websocket")


class WebSocketManager:
    """
    Manages active WebSocket client connections from the Trievo dashboard.
    Supports partition-based broadcasting and global event fanout.
    """

    def __init__(self):
        # partition -> list of WebSockets
        self.active_connections: Dict[str, List[WebSocket]] = {}
        # global connections
        self.global_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket, partition: Optional[str] = None):
        await websocket.accept()
        if partition:
            clean_part = partition.strip().lower()
            if clean_part not in self.active_connections:
                self.active_connections[clean_part] = []
            self.active_connections[clean_part].append(websocket)
        else:
            self.global_connections.append(websocket)
        logger.info("WebSocket connected (partition=%s). Total global=%d", partition, len(self.global_connections))

    def disconnect(self, websocket: WebSocket, partition: Optional[str] = None):
        if partition:
            clean_part = partition.strip().lower()
            if clean_part in self.active_connections and websocket in self.active_connections[clean_part]:
                self.active_connections[clean_part].remove(websocket)
        if websocket in self.global_connections:
            self.global_connections.remove(websocket)
        logger.info("WebSocket disconnected (partition=%s)", partition)

    async def broadcast(self, message: dict, partition: Optional[str] = None):
        """Broadcast JSON payload to relevant clients."""
        payload_str = json.dumps(message)
        targets: List[WebSocket] = list(self.global_connections)

        if partition:
            clean_part = partition.strip().lower()
            if clean_part in self.active_connections:
                targets.extend(self.active_connections[clean_part])

        dead_sockets: List[WebSocket] = []
        for ws in set(targets):
            try:
                await ws.send_text(payload_str)
            except Exception:
                dead_sockets.append(ws)

        for ws in dead_sockets:
            self.disconnect(ws, partition)


_ws_manager: Optional[WebSocketManager] = None


def get_ws_manager() -> WebSocketManager:
    global _ws_manager
    if _ws_manager is None:
        _ws_manager = WebSocketManager()
    return _ws_manager
