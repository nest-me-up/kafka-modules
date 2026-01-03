/* eslint-disable @typescript-eslint/no-explicit-any */
import { createContextServiceMock, getLoggerMock } from '@nest-me-up/common'
import { ConfigService } from '@nestjs/config'
import { KafkaClientService, KafkaConsumer, KafkaConsumerHandler } from '../kafka-client'
import { KafkaMessagingService } from './kafka-messaging.service'
import { MessageData } from './message-data.dto'

class DataMessage {
  name: string
  kind: MessageKind
  payload: unknown
  transactionId?: string
  sessionId?: string
}

enum MessageKind {
  created = 'created',
  updated = 'updated',
  deleted = 'deleted',
  sync = 'sync',
}
describe('Kafka Emitter Tests', () => {
  const configServiceMock = {
    get: jest.fn().mockImplementation((key) => {
      if (key === 'kafka') {
        return {
          retries: 1,
          consumer: {
            delay: 0,
          },
        }
      } else if (key === 'serviceName') {
        return 'service'
      }
      return undefined
    }),
  } as unknown as ConfigService
  const loggerMock = getLoggerMock()

  let consumerMock: KafkaConsumer
  let kafkaClientMock: KafkaClientService
  let service: KafkaMessagingService
  const contextServiceMock = createContextServiceMock()

  beforeEach(async () => {
    jest.useFakeTimers()
    consumerMock = {
      connect: jest.fn().mockResolvedValue(undefined),
    } as unknown as KafkaConsumer
    kafkaClientMock = {
      getConsumer: jest.fn().mockReturnValue(consumerMock),
      sendMessages: jest.fn(),
    } as unknown as KafkaClientService
    service = new KafkaMessagingService(configServiceMock, loggerMock, kafkaClientMock, contextServiceMock)
    await service.onApplicationBootstrap()
    jest.runAllTimers()
    await Promise.resolve()
    jest.useRealTimers()
  })

  describe('Test sending messages to topic', () => {
    it('test send single message', async () => {
      const message: DataMessage = {
        name: 'name',
        kind: MessageKind.created,
        payload: {},
      }
      await service.sendMessage({ message, topic: 'topic-name' })
      expect(kafkaClientMock.sendMessages).toHaveBeenCalledWith({
        messages: [expect.objectContaining({ message })],
        topic: 'topic-name',
      })
    })

    it('test send multiple message', async () => {
      const messages: DataMessage[] = [
        {
          name: 'name',
          kind: MessageKind.created,
          payload: {},
        },
        {
          name: 'name2',
          kind: MessageKind.updated,
          payload: {},
        },
      ]
      await service.sendMessages({ messages, topic: 'topic-name' })
      expect(kafkaClientMock.sendMessages).toHaveBeenCalledWith({
        messages: [
          expect.objectContaining({ message: messages[0] }),
          expect.objectContaining({ message: messages[1] }),
        ],
        topic: 'topic-name',
      })
    })
  })

  describe('Test consuming messages', () => {
    it('creating emitters with same name should fail', async () => {
      await service.createTopicEmitter({ topic: 'topic', emitterName: 'emitter1' })
      await expect(service.createTopicEmitter({ topic: 'topic1', emitterName: 'emitter1' })).rejects.toThrow()
    })
    it('creating emitters with different names should succeed', async () => {
      const emitter1 = await service.createTopicEmitter({ topic: 'topic1', emitterName: 'emitter1' })
      const emitter2 = await service.createTopicEmitter({ topic: 'topic2', emitterName: 'emitter2' })
      expect(emitter1).toBeTruthy()
      expect(emitter2).toBeTruthy()
    })
  })
  describe('Test consuming messages', () => {
    it('creating emitters', async () => {
      const emitter1 = await service.createTopicEmitter({ topic: 'topic', emitterName: 'emitter1' })
      const emitter2 = await service.createTopicEmitter({ topic: 'topic', emitterName: 'emitter2' })
      expect(kafkaClientMock.getConsumer).toHaveBeenCalledWith('topic')
      expect(consumerMock.connect).toHaveBeenCalled()
      expect(emitter1).toBeTruthy()
      expect(emitter2).toBeTruthy()
      expect(emitter1).not.toBe(emitter2)
    })

    it('test two emitters', async () => {
      let messageHandler: KafkaConsumerHandler<MessageData>
      kafkaClientMock.getConsumer = jest.fn().mockReturnValue({
        connect: (_messageHandler: KafkaConsumerHandler<MessageData>): Promise<void> => {
          messageHandler = _messageHandler
          return Promise.resolve()
        },
      })
      const emitter1 = await service.createTopicEmitter({ topic: 'topic2', emitterName: 'emitter1' })
      let onCalled1 = false
      let messageReceived1: any
      emitter1.on(async function (message) {
        onCalled1 = true
        messageReceived1 = message
      })
      const emitter2 = await service.createTopicEmitter({ topic: 'topic2', emitterName: 'emitter2' })
      let onCalled2 = false
      let messageReceived2: any
      emitter2.on(async function (message) {
        onCalled2 = true
        messageReceived2 = message
      })

      const message: DataMessage = {
        name: 'name',
        kind: MessageKind.created,
        payload: {},
      }
      if (messageHandler) {
        await messageHandler.handleMessage({
          messageData: { message } as MessageData,
          metadata: { topic: 'topic2', messageLocation: 'topic2-0-0' },
          topic: 'topic2',
          heartbeat: jest.fn(),
        })
      }

      expect(messageReceived1?.message).toEqual(message)
      expect(onCalled1).toBeTruthy()
      expect(messageReceived2?.message).toEqual(message)
      expect(onCalled2).toBeTruthy()
    })
    it('test one emitter two listeners', async () => {
      let messageHandler: KafkaConsumerHandler<MessageData>
      kafkaClientMock.getConsumer = jest.fn().mockReturnValue({
        connect: (_messageHandler: KafkaConsumerHandler<MessageData>): Promise<void> => {
          messageHandler = _messageHandler
          return Promise.resolve()
        },
      })
      const emitter = await service.createTopicEmitter({ topic: 'topic2', emitterName: 'emitter' })
      let onCalled1 = false
      let messageReceived1: any
      emitter.on(async function (message) {
        onCalled1 = true
        messageReceived1 = message
      })
      let onCalled2 = false
      let messageReceived2: any
      emitter.on(async function (message) {
        onCalled2 = true
        messageReceived2 = message
      })

      const message: DataMessage = {
        name: 'name',
        kind: MessageKind.created,
        payload: {},
      }
      expect(messageHandler).toBeDefined()
      if (messageHandler) {
        await messageHandler.handleMessage({
          messageData: { message } as MessageData,
          metadata: { topic: 'topic2', messageLocation: 'topic2-0-0' },
          topic: 'topic2',
          heartbeat: jest.fn(),
        })
      }

      expect(messageReceived1?.message).toEqual(message)
      expect(onCalled1).toBeTruthy()
      expect(messageReceived2?.message).toEqual(message)
      expect(onCalled2).toBeTruthy()
    })
    it('test emitters on different topics', async () => {
      let messageHandler: KafkaConsumerHandler<MessageData>
      kafkaClientMock.getConsumer = jest.fn().mockReturnValue({
        connect: (_messageHandler: KafkaConsumerHandler<MessageData>): Promise<void> => {
          messageHandler = _messageHandler
          return Promise.resolve()
        },
      })
      const emitter1 = await service.createTopicEmitter({ topic: 'topic2', emitterName: 'emitter1' })
      let onCalled1 = false
      let messageReceived1: any
      emitter1.on(async function (message) {
        onCalled1 = true
        messageReceived1 = message
      })
      const emitter2 = await service.createTopicEmitter({ topic: 'topic3', emitterName: 'emitter2' })
      let onCalled2 = false
      let messageReceived2: any
      emitter2.on(async function (message) {
        onCalled2 = true
        messageReceived2 = message
      })

      const message: DataMessage = {
        name: 'name',
        kind: MessageKind.created,
        payload: {},
      }
      if (messageHandler) {
        await messageHandler.handleMessage({
          messageData: { message } as MessageData,
          metadata: { topic: 'topic2', messageLocation: 'topic2-0-0' },
          topic: 'topic2',
          heartbeat: jest.fn(),
        })
      }

      expect(messageReceived1?.message).toEqual(message)
      expect(onCalled1).toBeTruthy()
      expect(messageReceived2?.message).toBeFalsy()
      expect(onCalled2).toBeFalsy()
    })
  })
})
