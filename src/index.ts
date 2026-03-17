import * as crypto from 'crypto'
import * as uuid from 'uuid'
import * as avro from 'avsc'
import * as zlib from 'zlib'
import * as gluesdk from '@aws-sdk/client-glue'
import Ajv, { ValidateFunction } from 'ajv'

export enum SchemaType {
  AVRO = 'AVRO',
  JSON = 'JSON',
}
export interface RegisterSchemaProps {
  type: SchemaType
  schemaName: string
  schema: string
}
export enum SchemaCompatibilityType {
  NONE = 'NONE',
  BACKWARD = 'BACKWARD',
  BACKWARD_ALL = 'BACKWARD_ALL',
  DISABLED = 'DISABLED',
  FORWARD = 'FORWARD',
  FORWARD_ALL = 'FORWARD_ALL',
  FULL = 'FULL',
  FULL_ALL = 'FULL_ALL',
}
export interface CreateSchemaProps {
  type: SchemaType
  schemaName: string
  compatibility: SchemaCompatibilityType
  schema: string
}
export interface EncodeProps {
  compress: boolean
}

export interface CachedSchemaInfo {
  type: SchemaType
  avroType?: avro.Type
  jsonSchema?: object
  validator?: ValidateFunction
}

export enum ERROR {
  NO_ERROR = 0,
  INVALID_HEADER_VERSION = 1,
  INVALID_COMPRESSION = 2,
  INVALID_SCHEMA_ID = 3,
  INVALID_SCHEMA = 4,
}

export type AnalyzeMessageResult = {
  /**
   * true if the message is valid
   */
  valid: boolean
  /**
   * the error code, if valid is false, otherwise undefined
   */
  error?: ERROR
  /** the original exception, if available */
  exception?: unknown
  /**
   * the header version
   */
  headerversion?: number
  /**
   * the compression type, may be 0 (none) or 5 (gzip)
   */
  compression?: number
  /**
   * the uuid of the schema
   */
  schemaId?: string
  /**
   * the glue schema
   */
  schema?: gluesdk.GetSchemaVersionResponse
}

class PromiseDispatcher {
  private active = 0
  private queue: Array<() => void> = []
  constructor(private readonly limit: number) {}

  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.active >= this.limit) {
      await new Promise<void>((resolve) => this.queue.push(resolve))
    }
    this.active++
    try {
      return await fn()
    } finally {
      this.active--
      const next = this.queue.shift()
      if (next) next()
    }
  }
}

export class GlueSchemaRegistry {
  /*
  This class aims to be compatible with the java serde implementation from AWS.
  https://github.com/awslabs/aws-glue-schema-registry/blob/master/serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/SerializationDataEncoder.java
  https://github.com/awslabs/aws-glue-schema-registry/blob/master/common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java
  */
  private gc: gluesdk.GlueClient
  public readonly registryName: string

  private glueSchemaIdCache: {
    [hash: string]: string
  }
  private schemaCache: {
    [key: string]: CachedSchemaInfo
  }

  private runningGlueSchemaLoads = new Map<string, Promise<gluesdk.GetSchemaVersionResponse>>()
  private limiter: PromiseDispatcher
  private ajv: Ajv
  private consumerAjv: Ajv
  private consumerValidatorCache = new WeakMap<object, ValidateFunction>()

  /**
   * Constructs a GlueSchemaRegistry
   *
   * @param registryName - name of the Glue registry you want to use
   * @param props - optional AWS properties that are used when constructing the Glue object from the AWS SDK
   * @param maxConcurrentGlueCalls - optional maximum number of concurrent calls to the Glue service. Defaults to 1.
   */
  constructor(registryName: string, props: gluesdk.GlueClientConfig, maxConcurrentGlueCalls = 1) {
    this.gc = new gluesdk.GlueClient(props)
    this.registryName = registryName
    this.glueSchemaIdCache = {}
    this.schemaCache = {}
    this.limiter = new PromiseDispatcher(Math.max(1, maxConcurrentGlueCalls))
    this.ajv = new Ajv({ useDefaults: true, allErrors: true })
    this.consumerAjv = new Ajv({ useDefaults: true, allErrors: true })
  }

  /**
   * Updates the Glue client. Useful if you need to update the credentials, for example.
   *
   * @param props settings for the AWS Glue client
   */
  updateGlueClient(props: gluesdk.GlueClientConfig) {
    this.gc = new gluesdk.GlueClient(props)
  }

  private async loadGlueSchema(schemaId: string) {
    const existing = this.runningGlueSchemaLoads.get(schemaId)
    if (existing) return existing
    const p = this.limiter.run(() =>
      this.gc.send(
        new gluesdk.GetSchemaVersionCommand({
          SchemaVersionId: schemaId,
        }),
      ),
    )

    this.runningGlueSchemaLoads.set(schemaId, p)

    try {
      const res = await p
      return res
    } finally {
      this.runningGlueSchemaLoads.delete(schemaId)
    }
  }

  /**
   *
   * Creates a new schema in the glue schema registry.
   *
   * Throws if a SchemaVersionStatus in the response equals 'FAILURE'.
   * @param props
   * @returns the id of the created schema version
   */
  async createSchema(props: CreateSchemaProps) {
    const res = await this.limiter.run(() =>
      this.gc.send(
        new gluesdk.CreateSchemaCommand({
          DataFormat: props.type,
          Compatibility: props.compatibility,
          SchemaName: props.schemaName,
          SchemaDefinition: props.schema,
          RegistryId: { RegistryName: this.registryName },
        }),
      ),
    )
    if (res.SchemaVersionStatus === 'FAILURE') throw new Error('Schema registration failure')
    return res.SchemaVersionId
  }

  /**
   * Registers a new version of an existing schema.
   * Returns the id of the existing schema version if a similar version already exists.
   *
   * @param props - the details about the schema
   * @returns {string} the id of the schema version
   * @throws if the schema does not exist
   * @throws if the Glue compatibility check fails
   */
  async register(props: RegisterSchemaProps): Promise<string> {
    const hash = crypto.createHash('SHA256').update(props.schemaName + '.' + props.schema)
    const hashString = hash.digest('hex').toString()
    const cachehit = this.glueSchemaIdCache[hashString]
    if (cachehit) {
      return cachehit
    }
    const schema = await this.gc.send(
      new gluesdk.RegisterSchemaVersionCommand({
        SchemaDefinition: props.schema,
        SchemaId: {
          RegistryName: this.registryName,
          SchemaName: props.schemaName,
        },
      }),
    )
    if (!schema.SchemaVersionId) throw new Error('Schema does not have SchemaVersionId')
    if (schema.Status === 'FAILURE') throw new Error('Schema registration failure')
    this.glueSchemaIdCache[hashString] = schema.SchemaVersionId
    // store the schema in cache to avoid another glue lookup when it's used
    if (props.type === SchemaType.JSON) {
      const jsonSchema = JSON.parse(props.schema)
      this.schemaCache[schema.SchemaVersionId] = {
        type: SchemaType.JSON,
        jsonSchema,
        validator: this.ajv.compile(jsonSchema),
      }
    } else {
      const avroSchema = avro.Type.forSchema(JSON.parse(props.schema))
      this.schemaCache[schema.SchemaVersionId] = {
        type: SchemaType.AVRO,
        avroType: avroSchema,
      }
    }
    return schema.SchemaVersionId
  }

  static COMPRESSION_DEFAULT = 0
  static COMPRESSION_ZLIB = 5
  static HEADER_VERSION = 3
  private static HEADER_VERSION_BYTE = GlueSchemaRegistry.initByteBuffer(
    GlueSchemaRegistry.HEADER_VERSION,
  ) // default version 3
  private static COMPRESSION_DEFAULT_BYTE = GlueSchemaRegistry.initByteBuffer(
    GlueSchemaRegistry.COMPRESSION_DEFAULT, // no compression
  )
  private static COMPRESSION_ZLIB_BYTE = GlueSchemaRegistry.initByteBuffer(
    GlueSchemaRegistry.COMPRESSION_ZLIB,
  )

  /**
   * Encode the object with a specific glue schema version
   *
   * @param glueSchemaId - UUID of the Glue schema version that should be used to encode the message
   * @param object - the object to encode
   * @param props - optional encoding options
   * @returns - a Buffer containing the binary message
   */
  async encode<T>(glueSchemaId: string, object: T, props?: EncodeProps) {
    const ZLIB_COMPRESS_FUNC = (buf: Buffer): Promise<Buffer> => {
      return new Promise((resolve, reject) => {
        zlib.deflate(buf, (err, data) => {
          if (err) {
            reject(err)
          } else {
            resolve(data)
          }
        })
      })
    }
    const NO_COMPRESS_FUNC = (buf: Buffer): Promise<Buffer> =>
      new Promise((resolve) => {
        resolve(buf)
      })
    const schemaInfo = await this.getSchemaForGlueId(glueSchemaId)
    // construct the message binary
    let buf: Buffer
    if (schemaInfo.type === SchemaType.JSON) {
      // Clone to avoid mutating the caller's object (ajv useDefaults modifies in place)
      const payload = JSON.parse(JSON.stringify(object))
      if (schemaInfo.validator) {
        const valid = schemaInfo.validator(payload)
        if (!valid) {
          throw new Error(
            `JSON Schema validation failed: ${this.ajv.errorsText(schemaInfo.validator.errors)}`,
          )
        }
      }
      buf = Buffer.from(JSON.stringify(payload), 'utf-8')
    } else {
      buf = schemaInfo.avroType!.toBuffer(object)
    }
    let compression_func = ZLIB_COMPRESS_FUNC
    let compressionbyte = GlueSchemaRegistry.COMPRESSION_ZLIB_BYTE
    if (props && !props.compress) {
      compression_func = NO_COMPRESS_FUNC
      compressionbyte = GlueSchemaRegistry.COMPRESSION_DEFAULT_BYTE
    }
    const output = Buffer.concat([
      GlueSchemaRegistry.HEADER_VERSION_BYTE,
      compressionbyte,
      this.UUIDstringToByteArray(glueSchemaId),
      await compression_func(buf),
    ])
    return output
  }

  /**
   * Analyze the binary message to determine if it is valid and if so, what schema version it was encoded with.
   *
   * @param message - the binary message to analyze
   * @returns - an object containing the analysis results @see AnalyzeMessageResult
   */
  async analyzeMessage(message: Buffer): Promise<AnalyzeMessageResult> {
    const headerversion = message.readInt8(0)
    if (headerversion !== GlueSchemaRegistry.HEADER_VERSION) {
      return {
        valid: false,
        error: ERROR.INVALID_HEADER_VERSION,
      }
    }
    const compression = message.readInt8(1)
    if (
      compression !== GlueSchemaRegistry.COMPRESSION_DEFAULT &&
      compression !== GlueSchemaRegistry.COMPRESSION_ZLIB
    ) {
      return {
        valid: false,
        error: ERROR.INVALID_COMPRESSION,
      }
    }
    try {
      const producerSchemaId = uuid.stringify(message, 2)
      try {
        const producerschema = await this.loadGlueSchema(producerSchemaId)
        if (!producerschema) throw new Error('Schema not found')
        if (producerschema.Status === 'FAILURE') {
          return {
            valid: false,
            error: ERROR.INVALID_SCHEMA,
          }
        }
        return {
          valid: true,
          headerversion,
          compression,
          schemaId: producerSchemaId,
          schema: producerschema,
        }
      } catch (e) {
        return {
          valid: false,
          exception: e,
          error: ERROR.INVALID_SCHEMA,
        }
      }
    } catch (e) {
      return {
        valid: false,
        exception: e,
        error: ERROR.INVALID_SCHEMA_ID,
      }
    }
  }

  /**
   * Decode a message with a specific schema.
   *
   * @param message - Buffer with the binary encoded message
   * @param consumerschema - The schema to decode with. For Avro messages, pass an avro.Type.
   *   For JSON Schema messages, pass a JSON Schema object (optional — omit to skip consumer validation).
   * @returns - the deserialized message as object
   */
  async decode<T>(message: Buffer, consumerschema?: avro.Type | object): Promise<T> {
    const headerversion = message.readInt8(0)
    const compression = message.readInt8(1)
    if (headerversion !== GlueSchemaRegistry.HEADER_VERSION) {
      throw new Error(
        `Only header version ${GlueSchemaRegistry.HEADER_VERSION} is supported, received ${headerversion}`,
      )
    }
    if (
      compression !== GlueSchemaRegistry.COMPRESSION_DEFAULT &&
      compression !== GlueSchemaRegistry.COMPRESSION_ZLIB
    ) {
      throw new Error(`Only compression type 0 and 5 are supported, received ${compression}`)
    }
    const ZLIB_UNCOMPRESS_FUNC = (buf: Buffer): Promise<Buffer> => {
      return new Promise((resolve, reject) => {
        zlib.inflate(buf, (err, data) => {
          if (err) {
            reject(err)
          } else {
            resolve(data)
          }
        })
      })
    }
    const NO_UNCOMPRESS_FUNC = (buf: Buffer): Promise<Buffer> =>
      new Promise((resolve) => {
        resolve(buf)
      })
    const producerSchemaId = uuid.stringify(message, 2)
    const schemaInfo = await this.getSchemaForGlueId(producerSchemaId)
    const content = Buffer.from(message.subarray(18))
    let handlecompression = NO_UNCOMPRESS_FUNC
    if (compression === GlueSchemaRegistry.COMPRESSION_ZLIB) {
      handlecompression = ZLIB_UNCOMPRESS_FUNC
    }
    const decompressed = await handlecompression(content)

    if (schemaInfo.type === SchemaType.JSON) {
      const data = JSON.parse(decompressed.toString('utf-8'))
      if (consumerschema && !(consumerschema instanceof avro.Type)) {
        // Validate with consumer JSON schema; useDefaults fills in defaults for schema evolution
        const validate = this.getConsumerValidator(consumerschema)
        const valid = validate(data)
        if (!valid) {
          throw new Error(`JSON Schema validation failed: ${this.consumerAjv.errorsText(validate.errors)}`)
        }
      }
      return data as T
    } else {
      // Avro path requires an avro.Type consumer schema
      if (!consumerschema || !(consumerschema instanceof avro.Type)) {
        throw new Error('Avro decode requires an avro.Type consumer schema')
      }
      const resolver = this.getResolver(schemaInfo.avroType!, consumerschema)
      return consumerschema.fromBuffer(decompressed, resolver)
    }
  }

  private getConsumerValidator(schema: object): ValidateFunction {
    const cached = this.consumerValidatorCache.get(schema)
    if (cached) return cached
    const validator = this.consumerAjv.compile(schema)
    this.consumerValidatorCache.set(schema, validator)
    return validator
  }

  private async getSchemaForGlueId(id: string): Promise<CachedSchemaInfo> {
    if (this.schemaCache[id]) return this.schemaCache[id]
    const response = await this.loadGlueSchema(id)
    // Re-check after awaiting: concurrent callers share the same deduplicated
    // runningGlueSchemaLoads promise, so when it resolves all of them resume in
    // the microtask queue. The first to run will populate schemaCache; the rest
    // must return that result instead of calling ajv.compile / avro.Type.forSchema
    // again — Ajv throws "schema with key or id already exists" on a second
    // compile of any schema that carries an $id field.
    if (this.schemaCache[id]) return this.schemaCache[id]
    if (!response.SchemaDefinition) throw new Error('Glue returned undefined schema definition')
    const parsed = JSON.parse(response.SchemaDefinition)
    // Determine schema type from Glue DataFormat, default to AVRO for backward compatibility
    const dataFormat = response.DataFormat
    if (dataFormat === 'JSON') {
      const info: CachedSchemaInfo = {
        type: SchemaType.JSON,
        jsonSchema: parsed,
        validator: this.ajv.compile(parsed),
      }
      this.schemaCache[id] = info
      return info
    } else {
      const avroType = avro.Type.forSchema(parsed)
      const info: CachedSchemaInfo = {
        type: SchemaType.AVRO,
        avroType,
      }
      this.schemaCache[id] = info
      return info
    }
  }

  private UUIDstringToByteArray(id: string) {
    const idasbytes = uuid.parse(id)
    return new Uint8Array(idasbytes)
  }
  private getResolver(producerschema: avro.Type, consumerschema: avro.Type) {
    return consumerschema.createResolver(producerschema)
  }
  private static initByteBuffer(value: number) {
    return Buffer.from([value])
  }
}
