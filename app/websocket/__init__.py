"""
WebSocket connection manager for real-time dashboard updates.
"""

from app.websocket.connection_manager import WebSocketManager, get_ws_manager

__all__ = ["WebSocketManager", "get_ws_manager"]
