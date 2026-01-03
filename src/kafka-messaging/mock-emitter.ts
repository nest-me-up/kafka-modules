import { KafkaMessagingService } from './kafka-messaging.service'
import { MessageEmitter } from './message-emitter'

export function createMockKafkaMessaging(): { kafkaMessagingMock: KafkaMessagingService; emitter: MessageEmitter } {
  const emitter = new MessageEmitter('mock', 'mock', 0)
  const kafkaMessagingMock: KafkaMessagingService = {
    emitDataMessagesToTopic: jest.fn(),
    createTopicEmitter: jest.fn(() => Promise.resolve(emitter)),
    emitDataMessageToTopic: jest.fn(),
  } as unknown as KafkaMessagingService
  return { emitter, kafkaMessagingMock }
}
