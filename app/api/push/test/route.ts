import { handle, ok } from '@/lib/api'
import { sendPushToAll } from '@/lib/push'
import { APP_SHORT_NAME } from '@/lib/branding'

export const dynamic = 'force-dynamic'

/** POST /api/push/test — kirim notifikasi percobaan ke semua perangkat terdaftar. */
export async function POST() {
  return handle(async () => {
    const result = await sendPushToAll({
      title: APP_SHORT_NAME,
      body: 'Notifikasi percobaan berhasil 🎉 Panel siap mengabari pesan baru.',
      url: '/',
      tag: 'wa-controller-test',
    })
    return ok(result)
  })
}
