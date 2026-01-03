import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { CompressionTypes, ConsumerConfig, Kafka, Partitioners, Producer, RecordMetadata } from 'kafkajs'
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino'
import { v4 } from 'uuid'
import { KafkaConfig, KafkaConsumptionType, getEmitterConfig } from './config/kafka.config'
import { KafkaConsumer } from './kafka-consumer'

@Injectable()
export class KafkaClientService implements OnModuleInit, OnModuleDestroy {
  private kafkaClient: Kafka
  private readonly config: KafkaConfig
  private producer: Producer
  private readonly consumers: Record<string, KafkaConsumer> = {}

  constructor(
    private readonly configService: ConfigService,
    @InjectPinoLogger(KafkaClientService.name)
    private readonly logger: PinoLogger,
  ) {
    this.config = getEmitterConfig(this.configService, this.logger)
  }

  onModuleInit() {
    this.logger.info('Connecting to kafaka brokers: %s', this.config.brokers)
    this.kafkaClient = new Kafka({ brokers: this.config.brokers })
    this.logger.info('KafkaClientService created with config: %o', this.config)
  }

  private readonly shutdownTimeoutMs = 30000 // 30 seconds default timeout

  async onModuleDestroy() {
    const consumers = Object.values(this.consumers)

    if (consumers.length > 0) {
      // Step 1: Pause all consumers to stop fetching new messages
      this.logger.info('Pausing all kafka consumers to stop fetching new messages')
      await Promise.all(consumers.map((consumer) => consumer.pause()))

      // Step 2: Wait for active handlers to complete, then disconnect
      this.logger.info('Waiting for active message handlers to complete before disconnecting')
      await Promise.all(consumers.map((consumer) => consumer.gracefulDisconnect(this.shutdownTimeoutMs)))
      this.logger.info('All kafka consumers disconnected')
    }

    this.logger.info('Disconnecting from kafka - closing producer')
    if (this.producer) {
      await this.producer.disconnect()
    }
  }

  async getProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = this.kafkaClient.producer({
        createPartitioner: Partitioners.LegacyPartitioner,
      })
      this.logger.info('Creating producer connection to Kafka')
    }
    await this.producer.connect()
    return this.producer
  }

  async getConsumer(topic: string): Promise<KafkaConsumer> {
    let kafkaConsumer: KafkaConsumer = this.consumers[topic]
    if (!kafkaConsumer) {
      const groupId =
        this.config.consumptionType === KafkaConsumptionType.shared
          ? `${this.config.groupId}-${topic}`
          : generateBroadcastGroupId(`${this.config.groupId}-${topic}`)
      const consumerConfig: ConsumerConfig = {
        allowAutoTopicCreation: true,
        groupId,
        maxBytes: this.config.maxBytes,
        maxBytesPerPartition: this.config.maxBytesPerPartition,
        sessionTimeout: this.config.consumerSessionTimeout,
        heartbeatInterval: this.config.consumerHeartbeatInterval,
        rebalanceTimeout: this.config.consumerRebalanceTimeout,
        maxWaitTimeInMs: this.config.consumerMaxWaitTimeInMs,
      }

      this.logger.debug('Creating consumer for topic: %s, with config: %o', topic, consumerConfig)
      const consumer = this.kafkaClient.consumer(consumerConfig)
      kafkaConsumer = new KafkaConsumer(consumer, topic, this.logger)
      this.consumers[topic] = kafkaConsumer
    }
    return kafkaConsumer
  }

  async sendMessages({ messages, topic }: { messages: object[]; topic: string }): Promise<RecordMetadata[]> {
    const serializedMessages = messages.map((value) => ({
      value: JSON.stringify(value),
    }))

    // Calculate total message size in bytes
    const totalSize = serializedMessages.reduce((total, msg) => {
      return total + Buffer.byteLength(msg.value || '', 'utf8')
    }, 0)

    // Conditionally apply compression based on threshold
    const shouldCompress =
      this.config.compressionThreshold !== undefined && totalSize >= this.config.compressionThreshold

    const sendOptions: {
      messages: Array<{ value: string }>
      topic: string
      compression?: CompressionTypes
    } = {
      messages: serializedMessages,
      topic,
    }

    if (shouldCompress) {
      sendOptions.compression = this.config.compressionType || CompressionTypes.GZIP
      this.logger.debug(
        'Applying compression (%s) to message batch of size %d bytes (threshold: %d)',
        sendOptions.compression,
        totalSize,
        this.config.compressionThreshold,
      )
    }

    return await (await this.getProducer()).send(sendOptions)
  }
}
function generateBroadcastGroupId(prefix: string): string {
  const shortUuid = v4().split('-')[0]
  return `${prefix}-${shortUuid}`
}
