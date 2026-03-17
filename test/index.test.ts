import { describe, expect, test, beforeAll, beforeEach, jest } from '@jest/globals'
import { ERROR, GlueSchemaRegistry, SchemaCompatibilityType, SchemaType } from '../src'
import * as avro from 'avsc'
import * as GlueClientMock from './__mocks__/@aws-sdk/client-glue'

jest.mock('@aws-sdk/client-glue')

interface TestType {
  demo: string
}

interface TestTypeV2 {
  demo: string
  v2demo: string
}

const testschema = avro.Type.forSchema({
  type: 'record',
  name: 'property',
  namespace: 'de.meinestadt.test',
  fields: [{ name: 'demo', type: 'string', default: 'Hello World' }],
})

const testschemaV2 = avro.Type.forSchema({
  type: 'record',
  name: 'property',
  namespace: 'de.meinestadt.test',
  fields: [
    { name: 'demo', type: 'string', default: 'Hello World' },
    { name: 'v2demo', type: 'string', default: 'Meinestadt' },
  ],
})

// const sdkmock = SDKMock.getInstance()

// valid message with gzip compressed content
const compressedHelloWorld =
  '0305b7912285527d42de88eee389a763225f789c93f048cdc9c95728cf2fca495104001e420476'

const messageWithNotExistingSchema =
  '030500000000000000000000000000000000789c93f048cdc9c95728cf2fca495104001e420476'

// valid message with uncompressed content
const uncompressedHelloWorld = '0300b7912285527d42de88eee389a763225f1848656c6c6f20776f726c6421'

// message with wrong magic byte
const malformedMessage = '0000b7912285527d42de88eee389a763225f1848656c6c6f20776f726c6421'

// message with wrong compression byte
const malformedCompression = '0301b7912285527d42de88eee389a763225f1848656c6c6f20776f726c6421'

describe('schema management', () => {
  let schemaregistry: GlueSchemaRegistry
  beforeAll(async () => {
    GlueClientMock.GlueClient.mockClear()
    schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
  })
  test('create schema', async () => {
    // sdkmock.mockedCreateSchema.mockResolvedValue({})
    GlueClientMock.CreateSchemaCommand.mockResolvedValue({
      $metadata: {
        httpStatusCode: 200,
      },
    })
    await schemaregistry.createSchema({
      schema: JSON.stringify(testschemaV2),
      schemaName: 'Testschema',
      compatibility: SchemaCompatibilityType.BACKWARD,
      type: SchemaType.AVRO,
    })
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
  })
})

describe('coverage regression tests for index.ts', () => {
  beforeEach(async () => {
    GlueClientMock.reset()
    GlueClientMock.clear()
  })

  test('updateGlueClient replaces underlying client', async () => {
    const callsBeforeUpdate = GlueClientMock.GlueClient.mock.calls.length
    const schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    const callsAfterConstruct = GlueClientMock.GlueClient.mock.calls.length
    expect(callsAfterConstruct).toBe(callsBeforeUpdate + 1)
    schemaregistry.updateGlueClient({
      region: 'eu-west-1',
    })
    expect(GlueClientMock.GlueClient.mock.calls.length).toBe(callsAfterConstruct + 1)
  })

  test('analyze returns invalid schema id when payload is too short for uuid', async () => {
    const schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    const tooShortMessage = Buffer.from([GlueSchemaRegistry.HEADER_VERSION, 0])
    const result = await schemaregistry.analyzeMessage(tooShortMessage)
    expect(result.valid).toBe(false)
    expect(result.error).toBe(ERROR.INVALID_SCHEMA_ID)
  })

  test('decode throws when Avro payload has no avro.Type consumer schema', async () => {
    const schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      DataFormat: 'AVRO',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      SchemaArn: 'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/Testschema',
      SchemaDefinition: JSON.stringify(testschema),
    })

    await expect(
      schemaregistry.decode<TestType>(Buffer.from(compressedHelloWorld, 'hex')),
    ).rejects.toThrow('Avro decode requires an avro.Type consumer schema')
  })

})

describe('serde with compression', () => {
  let schemaregistry: GlueSchemaRegistry
  let schemaId: string

  beforeEach(async () => {
    schemaregistry = new GlueSchemaRegistry(
      'testregistry',
      {
        region: 'eu-central-1',
      },
      1,
    )
    GlueClientMock.clear()
  })

  test('serialization', async () => {
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
    })
    schemaId = await schemaregistry.register({
      schema: JSON.stringify(testschema),
      schemaName: 'Testschema',
      type: SchemaType.AVRO,
    })
    const bindata = await schemaregistry.encode(schemaId, {
      demo: 'Hello world!',
    })
    const binmessage = bindata.toString('hex')
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
    expect(binmessage).toBe(compressedHelloWorld)
  })

  test('deserialization with newly registered schema', async () => {
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
    })
    schemaId = await schemaregistry.register({
      schema: JSON.stringify(testschema),
      schemaName: 'Testschema',
      type: SchemaType.AVRO,
    })
    const binmessage = compressedHelloWorld
    const object = await schemaregistry.decode<TestType>(Buffer.from(binmessage, 'hex'), testschema)
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
    expect(object.demo).toBe('Hello world!')
  })

  test('deserialization with schema from registry', async () => {
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      SchemaArn: 'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/Testschema',
      SchemaDefinition: JSON.stringify(testschema),
    })
    const binmessage = compressedHelloWorld
    const object = await schemaregistry.decode<TestType>(Buffer.from(binmessage, 'hex'), testschema)
    expect(GlueClientMock.GetSchemaVersionCommand).toHaveBeenCalledTimes(1)
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
    expect(object.demo).toBe('Hello world!')
  })

  test('deserialization of 100 messages with 2 schemas in parallel', async () => {
    // even though we test with two different schema ids in the test messages, we can always return the same schema as the schemas are identical, dispite the ID.
    // the two different IDs are needed for testing the caching behavior
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      SchemaArn: 'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/Testschema',
      SchemaDefinition: JSON.stringify(testschema),
    })

    const binmessage = compressedHelloWorld
    const binmessage2 =
      '0305a7912285527d42de88eee389a763225f789cd3f248cdc9c95728484d4c4e4d2bcd5128cf2fca4951040059ba07ed'

    // create 100 promises to decode the same message in parallel
    const messages = Array.from({ length: 100 }, (_, i) => (i % 2 === 1 ? binmessage2 : binmessage))
    const promises = messages.map((m) =>
      schemaregistry.decode<TestType>(Buffer.from(m, 'hex'), testschema),
    )

    const results = await Promise.all(promises)
    expect(results.length).toBe(100)
    // expect that all results are the same
    results.forEach((result, i) => {
      const expected = i % 2 === 1 ? 'Hello peaceful world!' : 'Hello world!'
      expect(result.demo).toBe(expected)
    })
    expect(GlueClientMock.GetSchemaVersionCommand).toHaveBeenCalledTimes(2)
    expect(GlueClientMock.send).toHaveBeenCalledTimes(2)
  })
})

describe('serde with schema evolution', () => {
  let schemaregistry: GlueSchemaRegistry
  beforeAll(async () => {
    schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    GlueClientMock.reset()
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
    })
  })
  beforeEach(async () => {
    GlueClientMock.clear()
  })
  test('deserialization with schema evolution', async () => {
    const schemaId = await schemaregistry.register({
      schema: JSON.stringify(testschema),
      schemaName: 'Testschema',
      type: SchemaType.AVRO,
    })
    const binmessage = compressedHelloWorld
    const object = await schemaregistry.decode<TestTypeV2>(
      Buffer.from(binmessage, 'hex'),
      testschemaV2,
    )
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
    expect(object.demo).toBe('Hello world!')
    expect(schemaId).toBe('b7912285-527d-42de-88ee-e389a763225f')
    expect(object.v2demo).toBe('Meinestadt')
  })

  test('deserialization with cache', async () => {
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      SchemaArn: 'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/Testschema',
      SchemaDefinition: JSON.stringify(testschema),
    })

    const binmessage = compressedHelloWorld
    const object = await schemaregistry.decode<TestTypeV2>(
      Buffer.from(binmessage, 'hex'),
      testschemaV2,
    )
    // expect to have no calls to the schema registry as the schema should be cached from the previos test
    expect(GlueClientMock.send).toHaveBeenCalledTimes(0)
    expect(object.demo).toBe('Hello world!')
    expect(object.v2demo).toBe('Meinestadt')
  })
})

describe('serde without compression', () => {
  let schemaregistry: GlueSchemaRegistry

  beforeAll(async () => {
    schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    GlueClientMock.reset()
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
    })
  })
  beforeEach(async () => {
    GlueClientMock.clear()
  })
  test('serialization', async () => {
    const schemaId = await schemaregistry.register({
      schema: JSON.stringify(testschema),
      schemaName: 'Testschema',
      type: SchemaType.AVRO,
    })
    const bindata = await schemaregistry.encode(
      schemaId,
      {
        demo: 'Hello world!',
      },
      {
        compress: false,
      },
    )
    const binmessage = bindata.toString('hex')
    // expect that mockRegisterSchemaVersion got called only once, otherwise the cache wouldn't work
    expect(GlueClientMock.RegisterSchemaVersionCommand).toHaveBeenCalledTimes(1)
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
    expect(binmessage).toBe(uncompressedHelloWorld)
  })

  test('deserialization', async () => {
    const schemaId = await schemaregistry.register({
      schema: JSON.stringify(testschema),
      schemaName: 'Testschema',
      type: SchemaType.AVRO,
    })
    const binmessage = uncompressedHelloWorld
    const object = await schemaregistry.decode<TestType>(Buffer.from(binmessage, 'hex'), testschema)
    // expect that mockRegisterSchemaVersion was not called, otherwise the cache wouldn't work
    expect(GlueClientMock.RegisterSchemaVersionCommand).toHaveBeenCalledTimes(0)
    expect(object.demo).toBe('Hello world!')
  })
})

describe('test analyze message', () => {
  beforeAll(async () => {
    GlueClientMock.reset()
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      SchemaArn: 'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/Testschema',
      SchemaDefinition: JSON.stringify(testschema),
    })
  })
  test('analyze should succeed for a valid message', async () => {
    const schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    const result = await schemaregistry.analyzeMessage(Buffer.from(compressedHelloWorld, 'hex'))
    expect(result.valid).toBe(true)
    expect(result.compression).toBe(GlueSchemaRegistry.COMPRESSION_ZLIB)
    expect(result.schemaId).toBe('b7912285-527d-42de-88ee-e389a763225f')
    expect(result.schema?.SchemaArn).toBe(
      'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/Testschema',
    )
  })
  test('analyze should not succeed for an invalid message', async () => {
    const schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    const result = await schemaregistry.analyzeMessage(Buffer.from(malformedMessage, 'hex'))
    expect(result.valid).toBe(false)
    expect(result.error).toBe(ERROR.INVALID_HEADER_VERSION)
  })
  test('analyze should not succeed for an invalid compression type', async () => {
    const schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    const result = await schemaregistry.analyzeMessage(Buffer.from(malformedCompression, 'hex'))
    expect(result.valid).toBe(false)
    expect(result.error).toBe(ERROR.INVALID_COMPRESSION)
  })
  test('analyze should throw an error if the schema does not exist', async () => {
    const schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      Status: 'FAILURE',
      $metadata: {
        httpStatusCode: 200,
        requestId: '12345678901234567890123456789012',
      },
    })
    const result = await schemaregistry.analyzeMessage(
      Buffer.from(messageWithNotExistingSchema, 'hex'),
    )
    expect(result.valid).toBe(false)
    expect(result.error).toBe(ERROR.INVALID_SCHEMA)
  })
})

describe('test error cases', () => {
  let schemaregistry: GlueSchemaRegistry
  beforeAll(async () => {
    GlueClientMock.reset()
  })
  beforeEach(async () => {
    schemaregistry = new GlueSchemaRegistry('testregistry', {
      region: 'eu-central-1',
    })
    GlueClientMock.clear()
  })
  test('exception if header is wrong', async () => {
    const binmessage = malformedMessage
    expect.assertions(1)
    try {
      await schemaregistry.decode<TestType>(Buffer.from(binmessage, 'hex'), testschema)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
      expect(error.message).toMatch('Only header version 3 is supported, received 0')
    }
  })
  test('exception compression byte is wrong', async () => {
    const binmessage = malformedCompression
    expect.assertions(1)
    try {
      await schemaregistry.decode<TestType>(Buffer.from(binmessage, 'hex'), testschema)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
      expect(error.message).toMatch('Only compression type 0 and 5 are supported, received 1')
    }
  })
})

// --- JSON Schema Tests ---

// Schema with $id used for the concurrent-decode race condition test.
// Defined at module scope so the same object reference is reused across
// beforeAll / test, keeping the WeakMap-based consumerValidatorCache working.
const jsonSchemaWithId = {
  $id: 'https://example.com/concurrent-test-schema',
  type: 'object',
  properties: {
    demo: { type: 'string' },
  },
  required: ['demo'],
  additionalProperties: false,
}

interface JsonTestType {
  demo: string
}

interface JsonTestTypeV2 {
  demo: string
  v2demo: string
}

const jsonTestSchema = {
  type: 'object',
  properties: {
    demo: { type: 'string', default: 'Hello World' },
  },
  required: ['demo'],
  additionalProperties: false,
}

const jsonTestSchemaV2 = {
  type: 'object',
  properties: {
    demo: { type: 'string', default: 'Hello World' },
    v2demo: { type: 'string', default: 'Meinestadt' },
  },
  required: ['demo'],
  additionalProperties: false,
}

describe('JSON Schema serde with compression', () => {
  let schemaregistry: GlueSchemaRegistry
  let schemaId: string

  beforeEach(async () => {
    schemaregistry = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' }, 1)
    GlueClientMock.clear()
  })

  test('serialization roundtrip', async () => {
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
    })
    schemaId = await schemaregistry.register({
      schema: JSON.stringify(jsonTestSchema),
      schemaName: 'JsonTestschema',
      type: SchemaType.JSON,
    })
    const bindata = await schemaregistry.encode(schemaId, { demo: 'Hello world!' })

    expect(bindata.readInt8(0)).toBe(GlueSchemaRegistry.HEADER_VERSION)
    expect(bindata.readInt8(1)).toBe(GlueSchemaRegistry.COMPRESSION_ZLIB)

    const object = await schemaregistry.decode<JsonTestType>(bindata, jsonTestSchema)
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
    expect(object.demo).toBe('Hello world!')
  })

  test('serialization without compression', async () => {
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
    })
    schemaId = await schemaregistry.register({
      schema: JSON.stringify(jsonTestSchema),
      schemaName: 'JsonTestschema',
      type: SchemaType.JSON,
    })
    const bindata = await schemaregistry.encode(schemaId, { demo: 'Hello world!' }, { compress: false })

    expect(bindata.readInt8(0)).toBe(GlueSchemaRegistry.HEADER_VERSION)
    expect(bindata.readInt8(1)).toBe(GlueSchemaRegistry.COMPRESSION_DEFAULT)

    const content = bindata.subarray(18).toString('utf-8')
    expect(JSON.parse(content)).toEqual({ demo: 'Hello world!' })

    const object = await schemaregistry.decode<JsonTestType>(bindata, jsonTestSchema)
    expect(object.demo).toBe('Hello world!')
  })

  test('validation rejects invalid data on encode', async () => {
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
    })
    schemaId = await schemaregistry.register({
      schema: JSON.stringify(jsonTestSchema),
      schemaName: 'JsonTestschema',
      type: SchemaType.JSON,
    })
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await expect(schemaregistry.encode(schemaId, { demo: 123 } as any)).rejects.toThrow(
      'JSON Schema validation failed',
    )
  })
})

describe('JSON Schema serde with schema evolution', () => {
  let schemaregistry: GlueSchemaRegistry

  beforeAll(async () => {
    schemaregistry = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' })
    GlueClientMock.reset()
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
    })
  })

  beforeEach(async () => {
    GlueClientMock.clear()
  })

  test('deserialization with consumer schema applies defaults', async () => {
    const schemaId = await schemaregistry.register({
      schema: JSON.stringify(jsonTestSchema),
      schemaName: 'JsonTestschema',
      type: SchemaType.JSON,
    })
    const bindata = await schemaregistry.encode(schemaId, { demo: 'Hello world!' })
    const object = await schemaregistry.decode<JsonTestTypeV2>(bindata, jsonTestSchemaV2)
    expect(object.demo).toBe('Hello world!')
    expect(object.v2demo).toBe('Meinestadt')
  })

  test('deserialization without consumer schema', async () => {
    const schemaId = await schemaregistry.register({
      schema: JSON.stringify(jsonTestSchema),
      schemaName: 'JsonTestschema',
      type: SchemaType.JSON,
    })
    const bindata = await schemaregistry.encode(schemaId, { demo: 'Hello world!' })
    const object = await schemaregistry.decode<JsonTestType>(bindata)
    expect(object.demo).toBe('Hello world!')
  })
})

describe('JSON Schema decode from Glue registry', () => {
  let encodedMessage: Buffer

  beforeAll(async () => {
    GlueClientMock.reset()
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
    })
    const encoder = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' })
    const schemaId = await encoder.register({
      schema: JSON.stringify(jsonTestSchema),
      schemaName: 'JsonTestschema',
      type: SchemaType.JSON,
    })
    encodedMessage = await encoder.encode(schemaId, { demo: 'Hello world!' })
  })

  beforeEach(async () => {
    GlueClientMock.reset()
    GlueClientMock.clear()
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      DataFormat: 'JSON',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
      SchemaVersionId: 'b7912285-527d-42de-88ee-e389a763225f',
      SchemaArn: 'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/JsonTestschema',
      SchemaDefinition: JSON.stringify(jsonTestSchema),
    })
  })

  test('decode fetches JSON schema from Glue and deserializes', async () => {
    const decoder = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' })
    const result = await decoder.decode<JsonTestType>(encodedMessage, jsonTestSchema)
    expect(result.demo).toBe('Hello world!')
    expect(GlueClientMock.GetSchemaVersionCommand).toHaveBeenCalledTimes(1)
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
  })

  test('decode with schema evolution from Glue', async () => {
    const decoder = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' })
    const result = await decoder.decode<JsonTestTypeV2>(encodedMessage, jsonTestSchemaV2)
    expect(result.demo).toBe('Hello world!')
    expect(result.v2demo).toBe('Meinestadt')
  })

  test('decode rejects data incompatible with consumer schema', async () => {
    const decoder = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' })
    const strictConsumerSchema = {
      type: 'object',
      properties: {
        demo: { type: 'string' },
        v2demo: { type: 'string' },
      },
      required: ['demo', 'v2demo'],
      additionalProperties: false,
    }
    await expect(
      decoder.decode<JsonTestTypeV2>(encodedMessage, strictConsumerSchema),
    ).rejects.toThrow('JSON Schema validation failed')
    expect(GlueClientMock.GetSchemaVersionCommand).toHaveBeenCalledTimes(1)
  })

  test('decode throws when avro.Type consumer schema is passed for a JSON message', async () => {
    const decoder = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' })
    await expect(
      decoder.decode<JsonTestType>(encodedMessage, testschema),
    ).rejects.toThrow('JSON decode requires a JSON Schema consumer, not an avro.Type')
    expect(GlueClientMock.GetSchemaVersionCommand).toHaveBeenCalledTimes(1)
  })
})

describe('JSON Schema concurrent decode with $id (race condition guard)', () => {
  // getSchemaForGlueId re-checks schemaCache after awaiting loadGlueSchema.
  //
  // Why: loadGlueSchema deduplicates in-flight Glue requests via
  // runningGlueSchemaLoads — all concurrent callers share the same Promise.
  // When it resolves, every caller resumes in the microtask queue. Without
  // the re-check, each caller would call ajv.compile(parsed) on its own
  // JSON.parse'd copy of the schema. Ajv registers schemas by $id in an
  // internal store; the second compile of a schema carrying $id throws:
  //   "schema with key or id '...' already exists"
  // The re-check ensures only the first caller to resume compiles and caches
  // the schema; subsequent callers hit the cache and skip compilation.
  let encodedMessage: Buffer

  beforeAll(async () => {
    GlueClientMock.reset()
    GlueClientMock.RegisterSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      SchemaVersionId: 'c8912285-527d-42de-88ee-e389a763225f',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
    })
    const encoder = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' })
    const schemaId = await encoder.register({
      schema: JSON.stringify(jsonSchemaWithId),
      schemaName: 'JsonSchemaWithId',
      type: SchemaType.JSON,
    })
    encodedMessage = await encoder.encode(schemaId, { demo: 'Hello world!' })
  })

  test('concurrent decodes of a $id schema do not throw duplicate-id errors', async () => {
    GlueClientMock.reset()
    GlueClientMock.clear()
    GlueClientMock.GetSchemaVersionCommand.mockResolvedValue({
      VersionNumber: 1,
      Status: 'AVAILABLE',
      DataFormat: 'JSON',
      $metadata: { httpStatusCode: 200, requestId: '12345678901234567890123456789012' },
      SchemaVersionId: 'c8912285-527d-42de-88ee-e389a763225f',
      SchemaArn: 'arn:aws:glue:eu-central-1:123456789012:schema/testregistry/JsonSchemaWithId',
      SchemaDefinition: JSON.stringify(jsonSchemaWithId),
    })

    // Fresh registry — no cached schema — so the Glue fetch is triggered.
    // All decode calls are created synchronously before the mock's setTimeout
    // fires, so every caller reaches `await loadGlueSchema` while the single
    // shared Promise is still pending and shares it via runningGlueSchemaLoads.
    const decoder = new GlueSchemaRegistry('testregistry', { region: 'eu-central-1' }, 1)
    const results = await Promise.all(
      Array.from({ length: 10 }, () => decoder.decode<JsonTestType>(encodedMessage)),
    )

    expect(results).toHaveLength(10)
    results.forEach((r) => expect(r.demo).toBe('Hello world!'))
    // Confirm only one Glue call was made despite 10 concurrent requests
    expect(GlueClientMock.GetSchemaVersionCommand).toHaveBeenCalledTimes(1)
    expect(GlueClientMock.send).toHaveBeenCalledTimes(1)
  })
})
