import { spawn, type ChildProcess } from 'child_process'
import dotenv from 'dotenv'

dotenv.config()

// Callback appelé quand le tunnel tombe, pour déclencher la reconnexion
let onTunnelDied: (() => void) | null = null

export function setOnTunnelDied(cb: () => void) {
  onTunnelDied = cb
}

export async function createSSHTunnel(): Promise<ChildProcess> {
  return new Promise((resolve, reject) => {
    console.log('🔄 Établissement du tunnel SSH...')

    let resolved = false

    // Lancer le tunnel SSH avec keep-alive pour éviter les déconnexions
    const sshProcess = spawn('ssh', [
      '-L', '20184:postgresql-06f0920e-o76f27c90.database.cloud.ovh.net:20184',
      '-o', 'ProxyCommand=ssh -W %h:%p -p 22586 maline@141.94.169.160',
      '-o', 'ServerAliveInterval=60',
      '-o', 'ServerAliveCountMax=3',
      '-o', 'ExitOnForwardFailure=yes',
      '-p', '22586',
      'maline@srvmlnprdws00.adm.production',
      '-N'
    ], {
      stdio: ['ignore', 'pipe', 'pipe']
    })

    sshProcess.stdout.on('data', (data: Buffer) => {
      console.log(`SSH stdout: ${data}`)
    })

    sshProcess.stderr.on('data', (data: Buffer) => {
      const message = data.toString()
      console.log(`SSH: ${message}`)

      if (!resolved && !message.includes('Permission denied') && !message.includes('Connection refused')) {
        setTimeout(() => {
          if (!resolved) {
            resolved = true
            console.log('✅ Tunnel SSH établi')
            resolve(sshProcess)
          }
        }, 2000)
      }
    })

    sshProcess.on('error', (err: Error) => {
      console.error('❌ Erreur processus SSH:', err)
      if (!resolved) {
        resolved = true
        reject(err)
      }
    })

    sshProcess.on('exit', (code: number | null) => {
      if (!resolved) {
        resolved = true
        reject(new Error(`SSH process exited with code ${code}`))
      } else {
        // Le tunnel est tombé après avoir été établi -> déclencher reconnexion
        console.error(`❌ Tunnel SSH tombé (code ${code}) — reconnexion automatique...`)
        if (onTunnelDied) onTunnelDied()
      }
    })

    // Timeout de sécurité
    setTimeout(() => {
      if (!resolved) {
        if (sshProcess.exitCode === null) {
          resolved = true
          console.log('✅ Tunnel SSH actif (timeout)')
          resolve(sshProcess)
        } else {
          resolved = true
          reject(new Error('Failed to establish SSH tunnel within timeout'))
        }
      }
    }, 5000)
  })
}

export function closeSSHTunnel(sshProcess: ChildProcess | null) {
  if (sshProcess && sshProcess.exitCode === null) {
    console.log('🔄 Fermeture du tunnel SSH...')
    sshProcess.kill()
    console.log('✅ Tunnel SSH fermé')
  }
}
