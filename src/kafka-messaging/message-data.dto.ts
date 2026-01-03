import { KafkaMessageMetadata } from '../kafka-client'

export class MessageData {
  message: object
  transactionId: string
  sessionId: string
  retryCount?: number = 0
  error?: unknown
  metadata?: KafkaMessageMetadata
}
