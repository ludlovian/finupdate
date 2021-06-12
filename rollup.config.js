import resolve from '@rollup/plugin-node-resolve'
import replace from '@rollup/plugin-replace'

export default {
  input: 'src/index.mjs',
  external: [
    'fs/promises',
    'stream/promises',
    'mime/lite.js',
    '@googleapis/sheets',
    '@googleapis/drive',
    '@google-cloud/storage'
  ],
  plugins: [
    resolve(),
    replace({
      preventAssignment: true,
      values: {
        __VERSION__: process.env.npm_package_version
      }
    })
  ],
  output: {
    file: 'dist/finupdate.mjs',
    format: 'esm',
    sourcemap: false,
    banner: '#!/usr/bin/env node'
  }
}
