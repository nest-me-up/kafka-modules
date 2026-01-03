import { Module } from '@nestjs/common'
import { ConfigModule } from '@nestjs/config'
import { ContextModule, KafkaEmitterModule } from '../'
import { createLoggerModule } from '../logger'
import { EntityNotificationsConsumer } from './entity-notifications-consumer'
import { EntityNotificationsProducer } from './entity-notifications-producer'

export const SYNC_RUNNING_INDEX_COLUMN = 'sync_update_sequence'
@Module({
  imports: [createLoggerModule(), ConfigModule, KafkaEmitterModule, ContextModule],
  providers: [EntityNotificationsProducer, EntityNotificationsConsumer],
  exports: [EntityNotificationsProducer, EntityNotificationsConsumer],
})
export class CrossServicesEntitySyncModule {}
