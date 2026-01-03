import { KafkaMessagingService } from './kafka-messaging.service'
import { MessageEmitter } from './message-emitter'

export function createMockKafkaMessaging<T>(): {
  kafkaMessagingMock: KafkaMessagingService
  emitter: MessageEmitter<T>
} {
  const emitter = new MessageEmitter<T>('mock', 'mock', 0)
  const kafkaMessagingMock: KafkaMessagingService = {
    emitDataMessagesToTopic: jest.fn(),
    createTopicEmitter: jest.fn(() => Promise.resolve(emitter)),
    emitDataMessageToTopic: jest.fn(),
  } as unknown as KafkaMessagingService
  return { emitter, kafkaMessagingMock }
}
