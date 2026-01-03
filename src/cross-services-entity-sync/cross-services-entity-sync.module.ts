import { Module } from '@nestjs/common'
import { KafkaMessagingModule } from '../kafka-messaging'
import { EntityNotificationsConsumer } from './entity-notifications-consumer'
import { EntityNotificationsProducer } from './entity-notifications-producer'
import { ContextModule } from '@nest-me-up/common'

export const SYNC_RUNNING_INDEX_COLUMN = 'sync_update_sequence'
@Module({
  imports: [KafkaMessagingModule, ContextModule],
  providers: [EntityNotificationsProducer, EntityNotificationsConsumer],
  exports: [EntityNotificationsProducer, EntityNotificationsConsumer],
})
export class CrossServicesEntitySyncModule {}
