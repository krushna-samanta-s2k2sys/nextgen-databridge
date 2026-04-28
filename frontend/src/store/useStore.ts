import { create } from 'zustand'

interface User { sub: string; role: string }

interface AppStore {
  user: User | null
  token: string | null
  sidebarOpen: boolean
  wsConnected: boolean
  liveEvents: any[]
  setUser: (u: User | null) => void
  setToken: (t: string | null) => void
  setSidebar: (v: boolean) => void
  setWsConnected: (v: boolean) => void
  pushEvent: (e: any) => void
  clearEvents: () => void
}

export const useStore = create<AppStore>((set, get) => ({
  user: null,
  token: localStorage.getItem('df_token'),
  sidebarOpen: true,
  wsConnected: false,
  liveEvents: [],

  setUser: u => set({ user: u }),
  setToken: t => {
    if (t) localStorage.setItem('df_token', t)
    else localStorage.removeItem('df_token')
    set({ token: t })
  },
  setSidebar: v => set({ sidebarOpen: v }),
  setWsConnected: v => set({ wsConnected: v }),
  pushEvent: e => set(s => ({ liveEvents: [e, ...s.liveEvents].slice(0, 100) })),
  clearEvents: () => set({ liveEvents: [] }),
}))
