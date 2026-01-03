/* eslint-disable @typescript-eslint/no-explicit-any */
import { ContextInfo, ContextService } from '@nest-me-up/common/dist/context'
import { Injectable, OnApplicationBootstrap } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { RecordMetadata } from 'kafkajs'
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino'
import { v4 } from 'uuid'
import { KafkaClientService, KafkaMessageMetadata } from '../kafka-client'
import { KafkaConsumer, KafkaConsumerHandler } from '../kafka-client/kafka-consumer'
import { MessageData } from './message-data.dto'
import { MessageEmitter } from './message-emitter'

interface KafkaMessagingConfig {
  retries: number
  emitters: Record<string, EmitterSettingsConfig>
  consumer: {
    delay: number
  }
}
@Injectable()
export class KafkaMessagingService implements KafkaConsumerHandler<MessageData>, OnApplicationBootstrap {
  private readonly consumers: Record<string, KafkaConsumer> = {}
  //TODO use ZodSchema to validate the message
  private readonly emitters: Record<string, MessageEmitter<any>[]> = {}
  private readonly config: KafkaMessagingConfig
  private readonly serviceName
  private onApplicationBootstrapDone: boolean = false
  private readonly consumerConnectDelay: number = 90000
  constructor(
    private readonly configService: ConfigService,
    @InjectPinoLogger(KafkaMessagingService.name)
    private readonly logger: PinoLogger,
    private readonly kafkaClient: KafkaClientService,
    private readonly contextService: ContextService,
  ) {
    this.config = configService.get<KafkaMessagingConfig>('kafka')
    if (!this.config) {
      throw new Error('Missing kafka in configuration')
    }
    this.serviceName = configService.get('serviceName') || 'unknown'
    this.config.retries = this.config.retries || 0
    this.config.emitters = this.config.emitters || {}
    if (!this.config.consumer) {
      this.config.consumer = { delay: 90000 }
    }
    this.config.consumer.delay = this.config.consumer.delay || 90000
  }

  async createTopicEmitter<T>(config: { topic: string; emitterName: string }): Promise<MessageEmitter<T>> {
    if (this.hasEmitterWithName(config.emitterName)) {
      throw Error(`Emitter with name ${config.emitterName} already exists`)
    }
    this.logger.info('Registering topic emitter on topic: %s', config.topic)
    const retryTopicName = this.getRetryTopicName(config.topic, config.emitterName)
    const retries = this.config.emitters[config.emitterName]?.retries || this.config.retries
    const emitter = new MessageEmitter<T>(config.emitterName, config.topic, retries)
    this.cacheEmitter({ topic: config.topic, emitter })
    this.cacheEmitter({ topic: retryTopicName, emitter })

    this.logger.info('Starting consumers for topic topic: %s, retry topic: %s', config.topic, retryTopicName)
    await this.getKafkaConsumer(config.topic)
    await this.getKafkaConsumer(retryTopicName)
    return emitter
  }

  private hasEmitterWithName(name: string) {
    return Object.values(this.emitters).some((emitterArray) => emitterArray.some((emitter) => emitter.name === name))
  }

  private cacheEmitter({ topic, emitter }: { topic: string; emitter: MessageEmitter<any> }) {
    if (!this.emitters[topic]) {
      this.emitters[topic] = []
    }
    this.emitters[topic].push(emitter)
  }

  async sendMessage({ message, topic }: { message: object; topic: string }): Promise<RecordMetadata[]> {
    return this.sendMessages({ messages: [message], topic })
  }

  async sendMessages({ messages, topic }: { messages: object[]; topic: string }): Promise<RecordMetadata[]> {
    const contextInfo = this.contextService.getContext()
    const transactionId = contextInfo?.transactionId || v4()
    const sessionId = contextInfo?.sessionId || v4()
    const enrichedMessages: MessageData[] = messages.map((message) => ({
      message,
      transactionId,
      sessionId,
    }))
    return this.kafkaClient.sendMessages({ messages: enrichedMessages, topic })
  }

  private async getKafkaConsumer(topic: string): Promise<KafkaConsumer> {
    let consumer: KafkaConsumer = this.consumers[topic]
    if (!consumer) {
      consumer = await this.kafkaClient.getConsumer(topic)
      this.consumers[topic] = consumer
      if (this.consumerConnectDelay === 0) {
        this.logger.info('consumerConnectDelay is zero, Connecting consumer to topic asynchronously: %s', topic)
        consumer
          .connect(this)
          .then(() => {
            this.logger.debug('Connected consumer to topic: %s', topic)
          })
          .catch((error) => {
            this.logger.error(error, 'Failed to connect consumer to topic: %s', topic)
          })
      } else if (this.onApplicationBootstrapDone) {
        this.logger.error('Kafka consumer created after onApplicationBootstrap, connecting to topic: %s', topic)
        try {
          await consumer.connect(this)
          this.logger.debug('Connected consumer to topic: %s', topic)
        } catch (error) {
          this.logger.error(error, 'Failed to connect consumer to topic: %s', topic)
          throw error
        }
      } else {
        this.logger.info(
          'Kafka consumer created before onApplicationBootstrap, will delay connecting to topic: %s',
          topic,
        )
      }
    }
    return consumer
  }

  async handleMessage({
    messageData,
    metadata,
    topic,
    heartbeat,
  }: {
    messageData: MessageData
    metadata: KafkaMessageMetadata
    topic: string
    heartbeat: () => Promise<void>
  }): Promise<boolean> {
    const emitters: MessageEmitter<any>[] = this.emitters[topic]
    if (emitters) {
      this.logger.debug(
        `Emitters for topic %s: %o`,
        topic,
        emitters.map((emitter) => emitter.name),
      )
      await Promise.all(
        emitters.map((emitter) => this.handleEmit({ emitter, messageData, metadata, heartbeat, topic })),
      )
    }
    return true
  }

  private async handleEmit({
    emitter,
    messageData,
    metadata,
    heartbeat,
    topic,
  }: {
    emitter: MessageEmitter<any>
    messageData: MessageData
    metadata: KafkaMessageMetadata
    heartbeat: () => Promise<void>
    topic: string
  }): Promise<boolean> {
    try {
      messageData.retryCount ??= 0
      const { message } = messageData
      const contextInfo: ContextInfo = {
        transactionId: messageData.transactionId || v4(),
        sessionId: messageData.sessionId || v4(),
        tenantId: message ? (message as Record<string, unknown>['tenantId'] as string) : undefined,
        projectId: message ? (message as Record<string, unknown>['projectId'] as string) : undefined,
        userId: message ? (message as Record<string, unknown>['userId'] as string) : undefined,
        internalTransactionId: v4(),
      }
      this.logger.debug(
        {
          contextUserId: contextInfo.userId,
          contextTransactionId: contextInfo.transactionId,
          contextTenantId: contextInfo.tenantId,
          contextProjectId: contextInfo.projectId,
          contextSessionId: contextInfo.sessionId,
          contextInternalTransactionId: contextInfo.internalTransactionId,
        },
        'Got message from topic: %s, original-topic: %s, message: %o, metadata: %o, emitter-name: %s',
        topic,
        emitter.topic,
        message,
        metadata,
        emitter.name,
      )
      await this.contextService.runWithContext(contextInfo, async () => {
        await emitter.emit({ message, metadata, heartbeat })
      })
    } catch (error) {
      if (messageData.retryCount < emitter.retries) {
        await this.handleRetry(error, messageData, emitter)
      } else {
        await this.handleDeadLetter(error, messageData, emitter)
      }
      return false
    }
    return true
  }

  private async handleDeadLetter(error: unknown, messageData: MessageData, emitter: MessageEmitter<any>) {
    const dlqTopic = `${emitter.topic}-${this.serviceName}-${emitter.name}-dlq`
    this.logger.error(
      error,
      'Kafka Emitter processing failed %s (%d retries out of %d), will send to dead letter queue: %s',
      emitter.name,
      messageData.retryCount,
      emitter.retries,
      dlqTopic,
    )
    messageData.retryCount++
    messageData.error = error
    return this.kafkaClient.sendMessages({ messages: [messageData], topic: dlqTopic })
  }

  private async handleRetry(error: unknown, message: MessageData, emitter: MessageEmitter<any>) {
    const retryTopic = this.getRetryTopicName(emitter.topic, emitter.name)
    this.logger.info(
      error,
      'Kafka Emitter processing failed for %s (%d retries out of %d), will retry using topic: %s',
      emitter.name,
      message.retryCount,
      emitter.retries,
      retryTopic,
    )
    message.retryCount++
    return this.kafkaClient.sendMessages({ messages: [message], topic: retryTopic })
  }

  private getRetryTopicName(topic: string, emitterName: string) {
    return `${topic}-${this.serviceName}-${emitterName}`
  }

  async onApplicationBootstrap() {
    if (this.consumerConnectDelay > 0) {
      this.logger.info('Kafka emitter service starting up, sleeping for 5 seconds to allow all consumers to be created')
      setTimeout(async () => {
        this.logger.info('Kafka emitter service starting up, connecting to all consumers')
        await this.connectAllConsumers()
        this.onApplicationBootstrapDone = true
      }, this.consumerConnectDelay)
    } else {
      this.logger.info('Kafka emitter service starting up, connecting to all consumers on request')
      this.onApplicationBootstrapDone = true
    }
  }

  private async connectAllConsumers() {
    const promises = []
    for (const consumerObj of Object.entries(this.consumers)) {
      const topic = consumerObj[0]
      const consumer = consumerObj[1]
      promises.push(
        (async () => {
          try {
            await consumer.connect(this)
            this.logger.debug('Connected consumer to topic: %s', topic)
          } catch (error) {
            this.logger.error(error, 'Failed to connect consumer to topic: %s', topic)
          }
        })(),
      )
    }
    // Wait for all promises to resolve even if one fails
    await Promise.allSettled(promises)
  }
}

class EmitterSettingsConfig {
  retries: number
}
