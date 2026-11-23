const http = require('node:http')
const fs = require('node:fs/promises')
const path = require('node:path')
const express = require('express')
const { Server } = require('socket.io')
const os = require('node:os')

const SMSCODE_TOKEN = '152b19a7ca3aced18af731a757607513483fea01ee52d92f51ca09d14344eba8' || ''
const DEFAULT_UPSTREAM = 'https://api.smscode.gg/v2'
const MAX_BODY_BYTES = 65536

// Cache danh sách Mỹ trong 30 ngày
let usCountryCache = null
let usCountryCacheExpiresAt = 0

const STATIC_FILES = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/index.html', ['index.html', 'text/html; charset=utf-8']],
  ['/smscode.js', ['smscode.js', 'text/javascript; charset=utf-8']],
  ['/searchable-select.js', ['searchable-select.js', 'text/javascript; charset=utf-8']],
  ['/order-time.js', ['order-time.js', 'text/javascript; charset=utf-8']],
  ['/polling-registry.js', ['polling-registry.js', 'text/javascript; charset=utf-8']],
])

class HttpError extends Error {
  constructor(status, code, message) {
    super(message)
    this.status = status
    this.code = code
  }
}

function sendJson(response, status, payload) {
  const body = JSON.stringify(payload)
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    'cache-control': 'no-store',
  })
  response.end(body)
}

function errorPayload(code, message) {
  return { success: false, error: { code, message } }
}

function validateSingleInteger(searchParams, name, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const values = searchParams.getAll(name)
  if (values.length !== 1 || !/^\d+$/.test(values[0])) {
    throw new HttpError(400, 'VALIDATION_ERROR', `${name} phải là số nguyên hợp lệ.`)
  }

  const value = Number(values[0])

  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new HttpError(400, 'VALIDATION_ERROR', `${name} nằm ngoài phạm vi cho phép.`)
  }

  return value
}

function rejectUnknownQuery(searchParams, allowed) {
  for (const key of searchParams.keys()) {
    if (!allowed.has(key)) {
      throw new HttpError(400, 'VALIDATION_ERROR', `Tham số không được hỗ trợ: ${key}.`)
    }
  }
}

function buildQuery(values) {
  const query = new URLSearchParams()

  for (const [key, value] of Object.entries(values)) {
    query.set(key, String(value))
  }

  return query.toString()
}

function matchApiRoute(method, url) {
  if (url.pathname === '/api/catalog/countries') {
    if (method !== 'GET') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Phương thức không được hỗ trợ.')
    }

    rejectUnknownQuery(url.searchParams, new Set())

    return {
      upstreamPath: '/catalog/countries',
      filterUsCountries: true,
    }
  }

  if (url.pathname === '/api/catalog/services') {
    if (method !== 'GET') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Phương thức không được hỗ trợ.')
    }

    rejectUnknownQuery(url.searchParams, new Set(['country_id']))

    const countryId = validateSingleInteger(url.searchParams, 'country_id')

    return {
      upstreamPath: `/catalog/services?${buildQuery({ country_id: countryId })}`,
    }
  }

  if (url.pathname === '/api/catalog/products') {
    if (method !== 'GET') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Phương thức không được hỗ trợ.')
    }

    const allowed = new Set([
      'country_id',
      'platform_id',
      'sort',
      'limit',
      'page',
    ])

    rejectUnknownQuery(url.searchParams, allowed)

    const countryId = validateSingleInteger(url.searchParams, 'country_id')
    const platformId = validateSingleInteger(url.searchParams, 'platform_id')

    const sortValues = url.searchParams.getAll('sort')
    const sort = sortValues.length === 0 ? 'price_asc' : sortValues[0]

    if (
      sortValues.length > 1 ||
      ![
        'price_asc',
        'price_desc',
        'available_asc',
        'available_desc',
        'name_asc',
        'name_desc',
      ].includes(sort)
    ) {
      throw new HttpError(400, 'VALIDATION_ERROR', 'sort không hợp lệ.')
    }

    const limit = url.searchParams.has('limit')
      ? validateSingleInteger(url.searchParams, 'limit', { max: 10000 })
      : 10000

    const page = url.searchParams.has('page')
      ? validateSingleInteger(url.searchParams, 'page')
      : 1

    return {
      upstreamPath: `/catalog/products?${buildQuery({
        country_id: countryId,
        platform_id: platformId,
        sort,
        limit,
        page,
      })}`,
    }
  }

  if (url.pathname === '/api/orders/create') {
    if (method !== 'POST') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Phương thức không được hỗ trợ.')
    }

    rejectUnknownQuery(url.searchParams, new Set())

    return {
      upstreamPath: '/orders/create',
      hasBody: true,
    }
  }

  if (url.pathname.startsWith('/api/orders/')) {
    if (method !== 'GET') {
      throw new HttpError(405, 'METHOD_NOT_ALLOWED', 'Phương thức không được hỗ trợ.')
    }

    const match = url.pathname.match(/^\/api\/orders\/(\d+)$/)

    if (!match) {
      throw new HttpError(400, 'VALIDATION_ERROR', 'Mã đơn không hợp lệ.')
    }

    const orderId = Number(match[1])

    if (!Number.isSafeInteger(orderId) || orderId < 1) {
      throw new HttpError(400, 'VALIDATION_ERROR', 'Mã đơn không hợp lệ.')
    }

    rejectUnknownQuery(url.searchParams, new Set())

    return {
      upstreamPath: `/orders/${orderId}`,
    }
  }

  return null
}

async function readJsonBody(request, maxBytes = MAX_BODY_BYTES) {
  const contentType = request.headers['content-type'] || ''

  if (!contentType.toLowerCase().startsWith('application/json')) {
    throw new HttpError(400, 'VALIDATION_ERROR', 'Content-Type phải là application/json.')
  }

  const chunks = []
  let size = 0

  for await (const chunk of request) {
    size += chunk.length

    if (size > maxBytes) {
      throw new HttpError(413, 'PAYLOAD_TOO_LARGE', 'Dữ liệu gửi lên quá lớn.')
    }

    chunks.push(chunk)
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'))
  } catch {
    throw new HttpError(400, 'VALIDATION_ERROR', 'JSON không hợp lệ.')
  }
}

function validateCreateBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    throw new HttpError(400, 'VALIDATION_ERROR', 'Dữ liệu tạo đơn không hợp lệ.')
  }

  const keys = Object.keys(body)

  if (keys.some(key => !['product_id', 'quantity'].includes(key))) {
    throw new HttpError(400, 'VALIDATION_ERROR', 'Dữ liệu tạo đơn chứa trường không được hỗ trợ.')
  }

  if (!Number.isSafeInteger(body.product_id) || body.product_id < 1) {
    throw new HttpError(400, 'VALIDATION_ERROR', 'product_id không hợp lệ.')
  }

  if (!Number.isSafeInteger(body.quantity) || body.quantity < 1 || body.quantity > 20) {
    throw new HttpError(400, 'VALIDATION_ERROR', 'quantity phải từ 1 đến 20.')
  }

  return {
    product_id: body.product_id,
    quantity: body.quantity,
  }
}

async function proxyToSmscode(request, response, route, config) {
  // Nếu đã có cache Mỹ thì không gọi API danh sách quốc gia nữa.
  if (route.filterUsCountries) {
    if (usCountryCache && Date.now() < usCountryCacheExpiresAt) {
      return sendJson(response, 200, usCountryCache)
    }
  }

  const headers = {
    Authorization: `Bearer ${config.token}`,
  }

  const options = {
    method: request.method,
    headers,
  }

  if (route.hasBody) {
    const idempotencyKey = request.headers['idempotency-key']

    if (!idempotencyKey || !/^[A-Za-z0-9_-]{1,128}$/.test(idempotencyKey)) {
      throw new HttpError(400, 'VALIDATION_ERROR', 'Idempotency-Key không hợp lệ.')
    }

    const body = validateCreateBody(await readJsonBody(request))

    headers['Content-Type'] = 'application/json'
    headers['Idempotency-Key'] = idempotencyKey

    options.body = JSON.stringify(body)
  }

  const controller = new AbortController()
  options.signal = controller.signal

  const timeout = setTimeout(() => controller.abort(), 30000)

  let upstream

  try {
    upstream = await config.fetchImpl(
      `${config.upstreamBaseUrl}${route.upstreamPath}`,
      options
    )
  } catch {
    throw new HttpError(502, 'UPSTREAM_ERROR', 'Không thể kết nối đến SMSCode.')
  } finally {
    clearTimeout(timeout)
  }

  const contentType = upstream.headers.get('content-type') || ''

  if (!contentType.toLowerCase().includes('application/json')) {
    throw new HttpError(502, 'UPSTREAM_ERROR', 'SMSCode trả về dữ liệu không hợp lệ.')
  }

  let payload

  try {
    payload = await upstream.json()
  } catch {
    throw new HttpError(502, 'UPSTREAM_ERROR', 'SMSCode trả về JSON không hợp lệ.')
  }

  // Chỉ giữ lại quốc gia Mỹ.
  if (route.filterUsCountries && upstream.ok) {
    const extractCountries = value => {
      if (Array.isArray(value)) return value

      if (!value || typeof value !== 'object') return []

      for (const key of ['data', 'countries', 'items', 'results']) {
        if (Array.isArray(value[key])) return value[key]
      }

      if (value.data && typeof value.data === 'object') {
        return extractCountries(value.data)
      }

      return []
    }

    const isUs = country => {
      const iso = String(
        country?.iso2 ||
        country?.iso ||
        country?.code ||
        country?.country_code ||
        ''
      ).toUpperCase()

      const name = String(
        country?.name ||
        country?.country_name ||
        country?.title ||
        ''
      ).toLowerCase()

      return (
        ['US', 'USA'].includes(iso) ||
        name.includes('united states') ||
        name === 'usa'
      )
    }

    const countries = extractCountries(payload).filter(isUs)

    if (Array.isArray(payload)) {
      payload = countries
    } else if (Array.isArray(payload.data)) {
      payload.data = countries
    } else if (Array.isArray(payload.countries)) {
      payload.countries = countries
    } else if (Array.isArray(payload.items)) {
      payload.items = countries
    } else if (Array.isArray(payload.results)) {
      payload.results = countries
    } else if (payload.data && typeof payload.data === 'object') {
      payload.data = countries
    }

    if (countries.length) {
      usCountryCache = payload
      usCountryCacheExpiresAt = Date.now() + 30 * 24 * 60 * 60 * 1000
    }
  }

  sendJson(response, upstream.status, payload)
}

function createApp({
  upstreamBaseUrl = DEFAULT_UPSTREAM,
  token = SMSCODE_TOKEN,
  fetchImpl = fetch,
  rootDir = __dirname,
} = {}) {
  const config = {
    upstreamBaseUrl,
    token,
    fetchImpl,
    rootDir,
  }

  return async function app(request, response) {
    try {
      const url = new URL(request.url, 'http://127.0.0.1')

      if (url.pathname.startsWith('/api/')) {
        const route = matchApiRoute(request.method, url)

        if (!route) {
          return sendJson(
            response,
            404,
            errorPayload('NOT_FOUND', 'API route không tồn tại.')
          )
        }

        return await proxyToSmscode(request, response, route, config)
      }

      if (!['GET', 'HEAD'].includes(request.method)) {
        return sendJson(
          response,
          405,
          errorPayload('METHOD_NOT_ALLOWED', 'Phương thức không được hỗ trợ.')
        )
      }

      const file = STATIC_FILES.get(url.pathname)

      if (!file) {
        return sendJson(
          response,
          404,
          errorPayload('NOT_FOUND', 'Không tìm thấy tài nguyên.')
        )
      }

      const contents = await fs.readFile(path.join(rootDir, file[0]))

      response.writeHead(200, {
        'content-type': file[1],
        'content-length': contents.length,
        'x-content-type-options': 'nosniff',
      })

      response.end(request.method === 'HEAD' ? undefined : contents)

    } catch (error) {
      const status = error instanceof HttpError ? error.status : 500
      const code = error instanceof HttpError ? error.code : 'INTERNAL_ERROR'

      const message = error instanceof HttpError
        ? error.message
        : 'Lỗi máy chủ nội bộ.'

      if (!response.headersSent) {
        sendJson(response, status, errorPayload(code, message))
      } else {
        response.destroy()
      }
    }
  }
}

function openUrl(url) {
  if (process.env.NO_OPEN === '1') return

  const { exec } = require('node:child_process')
  const { platform } = require('node:os')

  if (platform() === 'win32') {
    exec(`cmd.exe /c start "" "${url}"`)
  } else if (platform() === 'darwin') {
    exec('open "$URL"', {
      env: { ...process.env, URL: url },
    })
  } else {
    exec('xdg-open "$URL"', {
      env: { ...process.env, URL: url },
    })
  }
}

function configuredPort() {
  if (process.env.PORT === undefined || process.env.PORT === '') {
    return 3000
  }

  const port = Number(process.env.PORT)

  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error('PORT phải là số nguyên từ 0 đến 65535.')
  }

  return port
}

function startServer({
  port = configuredPort(),
  openBrowser = process.env.NO_OPEN !== '1',
} = {}) {
  return new Promise((resolve, reject) => {
    const app = express()

    app.use(createApp())

    const server = http.createServer(app)
    const io = new Server(server)

    const roomDevices = new Map()
    const roomSettings = new Map()
    const roomRevisions = new Map()

    const publishDevices = roomId => {
      const devices = [
        ...(roomDevices.get(roomId)?.entries() || []),
      ].map(([socketId, deviceId]) => ({
        deviceId,
        socketId,
        online: true,
      }))

      io.to(roomId).emit('room-devices', devices)

      console.log(`Tổng thiết bị đang kết nối: ${devices.length}`)
    }

    const sanitizeSettings = (input = {}) => {
      if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null
      }

      const integerOrNull = value => {
        const number = Number(value)

        return Number.isSafeInteger(number) && number > 0
          ? number
          : null
      }

      const countryId = integerOrNull(input.countryId)
      const productId = integerOrNull(input.productId)

      const intervalValue = Number(input.interval)

      const interval = Number.isFinite(intervalValue)
        ? Math.max(1, Math.min(60, Math.round(intervalValue)))
        : 5

      const callingCode = String(input.callingCode || '')
        .replace(/[^0-9]/g, '')
        .slice(0, 5)

      const countryName = String(input.countryName || '')
        .replace(/[<>]/g, '')
        .slice(0, 80)

      return {
        provider: 'smscode',
        countryId,
        countryName,
        callingCode,
        productId,
        interval,
      }
    }

    io.on('connection', socket => {

      // Thiết bị tham gia phòng
      socket.on('join-room', ({ roomId, deviceId } = {}) => {
        if (
          roomId !== 'room-main' ||
          typeof deviceId !== 'string' ||
          !/^[\w-]{8,80}$/.test(deviceId)
        ) {
          return
        }

        if (socket.data.roomId) {
          socket.leave(socket.data.roomId)

          roomDevices.get(socket.data.roomId)?.delete(socket.id)

          publishDevices(socket.data.roomId)
        }

        socket.join(roomId)

        socket.data.roomId = roomId
        socket.data.deviceId = deviceId

        if (!roomDevices.has(roomId)) {
          roomDevices.set(roomId, new Map())
        }

        roomDevices.get(roomId).set(socket.id, deviceId)

        const savedSettings = roomSettings.get(roomId) || null

        socket.emit('room-settings', {
          settings: savedSettings,
          revision: roomRevisions.get(roomId) || 0,
          senderDeviceId: null,
        })

        publishDevices(roomId)
      })

      // Đồng bộ cấu hình hai chiều
      socket.on('room-settings', payload => {
        if (
          socket.data.roomId !== payload.roomId ||
          socket.data.deviceId !== payload.senderDeviceId
        ) {
          return
        }

        const settings = sanitizeSettings(payload.settings)

        if (!settings) return

        const revision = (roomRevisions.get(payload.roomId) || 0) + 1

        roomSettings.set(payload.roomId, settings)
        roomRevisions.set(payload.roomId, revision)

        socket.to(payload.roomId).emit('room-settings', {
          settings,
          revision,
          senderDeviceId: socket.data.deviceId,
        })
      })

      // Điều khiển thiết bị trong phòng
      socket.on('room-command', command => {
        if (
          socket.data.roomId !== command.roomId ||
          socket.data.deviceId !== command.senderDeviceId
        ) {
          return
        }

        if (
          !/^[\w-]{8,80}$/.test(command.commandId || '') ||
          !['GET_PHONE', 'BUY_ALL', 'CLEAR_ALL'].includes(command.type)
        ) {
          return
        }

        const event = {
          roomId: command.roomId,
          commandId: command.commandId,
          type: command.type,
          senderDeviceId: command.senderDeviceId,
          timestamp: Date.now(),
        }

        io.to(command.roomId).emit('room-command', event)
      })

      // Khi thiết bị ngắt kết nối
      socket.on('disconnect', () => {
        const roomId = socket.data.roomId

        if (!roomId) return

        roomDevices.get(roomId)?.delete(socket.id)

        publishDevices(roomId)
      })
    })

    server.once('error', reject)

    server.listen(port, '0.0.0.0', () => {
      server.off('error', reject)

      const address = server.address()
      const url = `http://127.0.0.1:${address.port}`

      console.log(`Listening on ${url}`)

      for (const interfaces of Object.values(os.networkInterfaces())) {
        for (const address of interfaces || []) {
          if (address.family === 'IPv4' && !address.internal) {
            console.log(
              `Phone/LAN: http://${address.address}:${server.address().port}`
            )
          }
        }
      }

      if (openBrowser) openUrl(url)

      resolve(server)
    })
  })
}

if (require.main === module) {
  startServer().catch(error => {
    console.error(`Không thể khởi động server: ${error.message}`)
    process.exitCode = 1
  })
}

module.exports = {
  createApp,
  startServer,
}
