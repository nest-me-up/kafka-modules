import { Consumer } from 'kafkajs'
import { PinoLogger } from 'nestjs-pino'
import { KafkaMessageMetadata } from './kafka-client.interface'

export class KafkaConsumer {
  private activeHandlers = 0

  constructor(
    private readonly consumer: Consumer,
    private readonly topic: string,
    private readonly logger: PinoLogger,
  ) {}

  async connect<T>(messageHandler: KafkaConsumerHandler<T>) {
    await this.consumer.connect()
    await this.consumer.subscribe({ topic: this.topic })
    await this.consumer.run({
      eachMessage: async (payload) => {
        this.activeHandlers++
        try {
          const message: Record<string, unknown> = JSON.parse(payload.message.value.toString())
          const metadata: KafkaMessageMetadata = {
            topic: payload.topic,
            messageLocation: `${payload.topic}-${payload.partition}-${payload.message.offset}`,
          }
          this.logger.debug(
            {
              contextTransactionId: message.transactionId,
            },
            'Kafka Consumer topic: %s, received message: %o',
            this.topic,
            message,
          )
          if (message) {
            await messageHandler.handleMessage({
              messageData: message as T,
              metadata,
              topic: this.topic,
              heartbeat: payload.heartbeat,
            })
          }
        } catch (error) {
          this.logger.error(error, 'Kafka Consumer could not process message')
        } finally {
          this.activeHandlers--
        }
      },
    })
  }

  async pause() {
    try {
      this.consumer.pause([{ topic: this.topic }])
      this.logger.info('Paused consumer for topic: %s', this.topic)
    } catch (error) {
      this.logger.warn(error, 'Failed to pause consumer for topic: %s', this.topic)
    }
  }

  getActiveHandlerCount(): number {
    return this.activeHandlers
  }

  async gracefulDisconnect(timeoutMs: number = 30000): Promise<void> {
    const startTime = Date.now()
    const checkInterval = 100

    while (this.activeHandlers > 0) {
      const elapsed = Date.now() - startTime
      if (elapsed >= timeoutMs) {
        this.logger.warn(
          'Graceful shutdown timeout reached for topic: %s, forcing disconnect with %d active handlers',
          this.topic,
          this.activeHandlers,
        )
        break
      }

      this.logger.debug(
        'Waiting for %d active handlers to complete for topic: %s (elapsed: %dms)',
        this.activeHandlers,
        this.topic,
        elapsed,
      )
      await new Promise((resolve) => setTimeout(resolve, checkInterval))
    }

    if (this.activeHandlers === 0) {
      this.logger.info('All handlers completed for topic: %s, disconnecting', this.topic)
    }

    return this.consumer.disconnect()
  }

  async disconnect() {
    return this.consumer.disconnect()
  }
}
export interface KafkaConsumerHandler<T> {
  handleMessage(data: {
    messageData: T
    metadata: KafkaMessageMetadata
    topic: string
    heartbeat: () => Promise<void>
  }): Promise<boolean>
}
