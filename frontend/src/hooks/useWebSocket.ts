import { useEffect, useRef } from 'react'
import { useStore } from '../store/useStore'

const WS_URL = (import.meta.env.VITE_WS_URL || 'ws://localhost:8000') + '/ws'

export function useWebSocket() {
  const ws = useRef<WebSocket | null>(null)
  const { setWsConnected, pushEvent } = useStore()

  useEffect(() => {
    function connect() {
      try {
        ws.current = new WebSocket(WS_URL)
        ws.current.onopen = () => {
          setWsConnected(true)
          // Heartbeat
          const hb = setInterval(() => ws.current?.send('ping'), 30_000)
          ws.current!.onclose = () => { clearInterval(hb); setWsConnected(false); setTimeout(connect, 3000) }
        }
        ws.current.onmessage = (e) => {
          try { pushEvent(JSON.parse(e.data)) } catch {}
        }
        ws.current.onerror = () => ws.current?.close()
      } catch { setTimeout(connect, 5000) }
    }
    connect()
    return () => ws.current?.close()
  }, [])
}
