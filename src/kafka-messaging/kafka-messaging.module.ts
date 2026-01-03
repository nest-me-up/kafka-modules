import { ContextModule } from '@nest-me-up/common/dist/context'
import { Module } from '@nestjs/common'
import { KafkaClientModule } from '../kafka-client'
import { KafkaMessagingService } from './kafka-messaging.service'

@Module({
  imports: [KafkaClientModule, ContextModule],
  providers: [KafkaMessagingService],
  exports: [KafkaMessagingService],
})
export class KafkaMessagingModule {}
