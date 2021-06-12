import { request as httpRequest } from 'http'
import decimal from 'decimal'

const decimalFields = new Set(['price', 'dividend', 'cost', 'qty', 'gain'])

export function get (path, { port }) {
  return request({ port, path, method: 'GET' }).then(res => res.data)
}

export function put (path, body, { port }) {
  return request({ port, path, body, method: 'PUT' })
}

export function del (path, body, { port }) {
  return request({ port, path, body, method: 'DELETE' })
}

function reviver (k, v) {
  return decimalFields.has(k) ? decimal(v) : v
}

function request (opts) {
  return new Promise((resolve, reject) => {
    if (opts.body) {
      opts.body = JSON.stringify(opts.body)
      opts.headers = {
        'Content-Type': 'application/json',
        'Content-Length': opts.body.length
      }
    }
    const req = httpRequest(opts, res => {
      let data = ''
      res.setEncoding('utf8')
      res.on('data', chunk => (data += chunk))
      res.on('end', () => {
        const ct = res.headers['content-type']
        if (ct && data && ct.includes('application/json')) {
          try {
            data = JSON.parse(data, reviver)
          } catch (err) {
            return reject(err)
          }
        }
        res.data = data
        if (res.statusCode >= 400) {
          const err = new Error(res.statusMessage)
          err.res = res
          return reject(err)
        }
        resolve(res)
      })
    })
    req.on('error', reject)
    if (opts.body) req.write(opts.body)
    req.end()
  })
}
