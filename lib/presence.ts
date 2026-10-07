/**
 * Melacak apakah ADA browser yang sedang melihat panel (tab aktif).
 *
 * Dipakai untuk memutuskan apakah notifikasi Web Push perlu dikirim: kalau
 * pengguna sedang menatap panel, cukup toast + suara di halaman; kalau tidak,
 * kirim push supaya tetap masuk ke HP/desktop.
 *
 * State disimpan di `globalThis` karena route handler Next dibundel terpisah
 * dari `server.ts` (lihat penjelasan singleton di README).
 */
type Visibility = 'visible' | 'hidden'

interface PresenceStore {
  clients: Map<string, Visibility>
}

const store: PresenceStore =
  (globalThis as unknown as { __waPresence?: PresenceStore }).__waPresence ??
  ((globalThis as unknown as { __waPresence?: PresenceStore }).__waPresence = {
    clients: new Map(),
  })

export function registerClient(socketId: string) {
  store.clients.set(socketId, 'hidden')
}

export function setVisibility(socketId: string, visibility: Visibility) {
  store.clients.set(socketId, visibility)
}

export function removeClient(socketId: string) {
  store.clients.delete(socketId)
}

/** Jumlah tab yang sedang tampil (visible) di semua perangkat. */
export function visibleClientCount() {
  let count = 0
  for (const value of store.clients.values()) if (value === 'visible') count += 1
  return count
}

export function connectedClientCount() {
  return store.clients.size
}
