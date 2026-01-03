import { KafkaMessageMetadata } from '../kafka-client'

export type MessageCallback<T> = (messageData: {
  message: T
  metadata: KafkaMessageMetadata
  heartbeat: () => Promise<void>
}) => Promise<void>

export class MessageEmitter<T> {
  private callbacks: MessageCallback<T>[] = []
  constructor(
    public readonly name: string,
    public readonly topic: string,
    public readonly retries: number,
  ) {}

  on(callback: MessageCallback<T>) {
    this.callbacks.push(callback)
  }
  async emit(messageData: { message: T; metadata: KafkaMessageMetadata; heartbeat: () => Promise<void> }) {
    if (this.callbacks.length > 0) {
      await Promise.all(this.callbacks.map((callback) => callback(messageData)))
    }
  }
}
