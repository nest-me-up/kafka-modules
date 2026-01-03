import { KafkaMessageMetadata } from '../kafka-client'

export type MessageCallback = (messageData: {
  message: object
  metadata: KafkaMessageMetadata
  heartbeat: () => Promise<void>
}) => Promise<void>

export class MessageEmitter {
  private callbacks: MessageCallback[] = []
  constructor(
    public readonly name: string,
    public readonly topic: string,
    public readonly retries: number,
  ) {}

  on(callback: MessageCallback) {
    this.callbacks.push(callback)
  }
  async emit(messageData: { message: object; metadata: KafkaMessageMetadata; heartbeat: () => Promise<void> }) {
    if (this.callbacks.length > 0) {
      await Promise.all(this.callbacks.map((callback) => callback(messageData)))
    }
  }
}
