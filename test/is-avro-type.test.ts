import { describe, expect, test } from '@jest/globals'
import * as avro from 'avsc'
import { isAvroType } from '../src'

describe('isAvroType (realm-proof avro.Type detection)', () => {
  test('accepts a real avro.Type', () => {
    const t = avro.Type.forSchema({ type: 'record', name: 'r', fields: [{ name: 'a', type: 'string' }] })
    expect(isAvroType(t)).toBe(true)
  })

  test('accepts a cross-realm avro.Type — structural, so a Type from a different avsc copy passes', () => {
    // Simulates a Type produced by a second avsc instance: structurally an avro Type
    // (has toBuffer/fromBuffer) but NOT `instanceof` this module\'s avro.Type.
    const crossRealm = { toBuffer() {}, fromBuffer() {}, createResolver() {} }
    expect(crossRealm instanceof avro.Type).toBe(false)
    expect(isAvroType(crossRealm)).toBe(true)
  })

  test('rejects a plain JSON Schema object', () => {
    expect(isAvroType({ type: 'object', properties: {} })).toBe(false)
  })

  test('rejects null / undefined / primitives', () => {
    expect(isAvroType(null)).toBe(false)
    expect(isAvroType(undefined)).toBe(false)
    expect(isAvroType('string')).toBe(false)
  })
})
