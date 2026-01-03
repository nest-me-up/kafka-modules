import { Injectable } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { InjectDataSource } from '@nestjs/typeorm'
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino'
import { DataSource, EntitySubscriberInterface, EventSubscriber, InsertEvent, UpdateEvent } from 'typeorm'
import { DataMessage, KafkaEmitterService, MessageKind } from '../'
import { SYNC_RUNNING_INDEX_COLUMN } from './cross-services-entity-sync.module'
import { checkSchemaDependencies } from './entity-notifications.utils'

@Injectable()
@EventSubscriber()
export class EntityNotificationsProducer implements EntitySubscriberInterface<any> {
  private readonly entityNameToTopic: Record<string, string> = {}
  private readonly serviceName
  constructor(
    private readonly kafkaEmitterService: KafkaEmitterService,
    @InjectPinoLogger(EntityNotificationsProducer.name)
    private readonly logger: PinoLogger,
    @InjectDataSource()
    private readonly dataSource: DataSource,
    private readonly configService: ConfigService,
  ) {
    this.serviceName = this.configService.get<string>('serviceName')
    dataSource.manager.connection.subscribers.push(this)
  }

  private connection() {
    return this.dataSource.manager.connection
  }

  private manager() {
    return this.dataSource.manager
  }

  async beforeInsert(event: InsertEvent<any>): Promise<any> {
    const entityName = event.metadata.name
    if (this.entityNameToTopic[entityName]) {
      event.entity[SYNC_RUNNING_INDEX_COLUMN] = 0
    }
    return event
  }

  async afterInsert(event: InsertEvent<any>): Promise<any> {
    const entityName = event.metadata.name
    if (this.entityNameToTopic[entityName]) {
      await this.emitEntityToTopic({ ...event }, MessageKind.created)
    }
    return event
  }

  async beforeUpdate(event: UpdateEvent<any>): Promise<any> {
    const entityName = event.metadata.name
    if (this.entityNameToTopic[entityName]) {
      const currentValue: number = parseInt(event.entity[SYNC_RUNNING_INDEX_COLUMN], 10) || 0
      event.entity[SYNC_RUNNING_INDEX_COLUMN] = currentValue + 1
    }
    return event
  }

  async afterUpdate(event: UpdateEvent<any>): Promise<any> {
    const kind: MessageKind = event.entity['deleted'] ? MessageKind.deleted : MessageKind.updated
    const entityName = event.metadata.name
    if (this.entityNameToTopic[entityName]) {
      const entity = { ...event.databaseEntity, ...event.entity }
      if (!entity['id']) {
        //for now we should avoid using bulk updates where cross-service entity sync is enabled
        //this is because we don't have a way to know which entity is being updated
        //and we don't want to emit too many messages
        this.logger.error(
          'EntityNotificationsProducer bulk update is not currently supported, detected on entity: %s, received entity: %o',
          entityName,
          entity,
        )
      }
      await this.emitEntityToTopic({ ...event, entity }, kind)
    }
    return event
  }

  private async emitEntityToTopic(event: InsertEvent<any> | UpdateEvent<any>, kind: MessageKind) {
    const entity = event.entity
    const entityName = event.metadata.name
    if (this.entityNameToTopic[entityName]) {
      const message: DataMessage = {
        name: entityName,
        kind: kind,
        payload: entity,
      }
      await this.kafkaEmitterService.emitDataMessageToTopic(message, this.entityNameToTopic[entityName])
    }
  }

  async registerEntity(config: { entityClass: any; topic?: string }) {
    const entityMetadata = this.connection().getMetadata(config.entityClass)
    const entityName = entityMetadata.name
    const tableName = entityMetadata.tableName
    const topic = config.topic || `DBSYNC_${this.serviceName}__${entityName}`
    await checkSchemaDependencies(tableName, this.manager(), this.logger)
    this.logger.info(
      'registering entity for DB notifications producer, entity: %s, table: %s, to topic: %s',
      entityName,
      tableName,
      topic,
    )
    this.entityNameToTopic[entityName] = topic
  }

  async syncEntityTable(entityName: string) {
    try {
      const metadata = this.connection().getMetadata(entityName)
      if (!this.entityNameToTopic[metadata.name]) {
        throw new Error(`Entity ${metadata.name} is not registered for sync. Call registerEntity first.`)
      }

      const entities = await this.manager().getRepository(entityName).createQueryBuilder().getMany()
      let count = 0

      for (const entity of entities) {
        await this.emitEntityToTopic(
          {
            entity,
            metadata,
          } as InsertEvent<any>,
          MessageKind.sync,
        )
        count++
      }

      this.logger.info('Synced %d records for %s', count, entityName)
      return count
    } catch (error) {
      this.logger.error(error, 'Sync failed for %s', entityName)
      throw error
    }
  }
}
