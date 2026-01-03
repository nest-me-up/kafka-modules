import { Injectable } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { InjectDataSource } from '@nestjs/typeorm'
import { InjectPinoLogger, PinoLogger } from 'nestjs-pino'
import { DataSource } from 'typeorm'
import { v4 } from 'uuid'
import { ContextInfo, ContextService, DataMessage, KafkaEmitterService, MessageKind } from '../'
import { SYNC_RUNNING_INDEX_COLUMN } from './cross-services-entity-sync.module'
import { checkSchemaDependencies } from './entity-notifications.utils'

@Injectable()
export class EntityNotificationsConsumer {
  private readonly registeredEntitiesToColumns: Record<string, string[]> = {}
  private readonly notificationListeners: Map<string, ((message: DataMessage) => Promise<void>)[]> = new Map()
  private readonly serviceName: string
  constructor(
    private readonly kafkaEmitterService: KafkaEmitterService,
    @InjectPinoLogger(EntityNotificationsConsumer.name)
    private readonly logger: PinoLogger,
    @InjectDataSource()
    private readonly dataSource: DataSource,
    private readonly configService: ConfigService,
    private readonly contextService: ContextService,
  ) {
    this.serviceName = this.configService.get<string>('serviceName')
  }

  private connection() {
    return this.dataSource.manager.connection
  }

  private manager() {
    return this.dataSource.manager
  }

  async registerNotificationListener({
    entityClass,
    listener,
  }: {
    entityClass: any
    listener: (message: DataMessage) => Promise<void>
  }) {
    const entityMetadata = this.connection().getMetadata(entityClass)
    const entityName = entityMetadata.name
    let listeners = this.notificationListeners.get(entityName)
    if (!listeners) {
      listeners = []
      this.notificationListeners.set(entityName, listeners)
    }
    listeners.push(listener)
  }

  async registerEntity(config: { entityClass: any; topic?: string; orginServiceName?: string }) {
    if (!config.topic && !config.orginServiceName) {
      throw new Error('either topic or orginServiceName must be provided')
    }
    const entityMetadata = this.connection().getMetadata(config.entityClass)
    const entityName = entityMetadata.name
    const tableName = entityMetadata.tableName
    const columns = entityMetadata.ownColumns.map((c) => c.propertyName)
    const topic = config.topic || `DBSYNC_${config.orginServiceName}__${entityName}`
    await checkSchemaDependencies(tableName, this.manager(), this.logger)
    this.logger.info(
      'registering entity for DB notifications consumer, entity: %s, table: %s, to topic: %s',
      entityName,
      tableName,
      topic,
    )
    this.registeredEntitiesToColumns[entityName] = columns
    const emitter = await this.kafkaEmitterService.createTopicEmitter({ topic, emitterName: topic })
    emitter.on(async (message) => await this.handleNotification(message))
  }

  private async handleNotification(message: DataMessage): Promise<void> {
    const entityName = message.name
    await this.contextService.runWithContext(
      {
        tenantId: message?.payload['tenant_id'],
        transactionId: v4(),
      } as ContextInfo,
      async () => {
        await this._handleNotification(message, entityName)
      },
    )
  }
  private async _handleNotification(message: DataMessage, entityName: string) {
    try {
      if (!this.registeredEntitiesToColumns[entityName]) {
        this.logger.error('entity %s is not registered', entityName)
        return
      }

      const entityColumns = this.registeredEntitiesToColumns[entityName]
      const entity = this.filteredEntity(message.payload, entityColumns)
      const tenantId = entity['tenant_id'] || entity['tenantId']
      const syncRunningIndex = entity[SYNC_RUNNING_INDEX_COLUMN]

      if (!entity['id'] || !tenantId) {
        this.logger.error('Missing id or tenantId, skipping: %o', entity)
        return
      }
      if (syncRunningIndex === undefined || syncRunningIndex === null) {
        this.logger.error('Missing %s value, skipping: %o', SYNC_RUNNING_INDEX_COLUMN, entity)
        return
      }

      if (message.kind === MessageKind.created) {
        await this.manager().createQueryBuilder().insert().into(entityName).values([entity]).execute()
      } else if (message.kind === MessageKind.updated) {
        const result = await this.manager()
          .createQueryBuilder()
          .update(entityName)
          .set(entity)
          .where('id = :id', { id: entity['id'] })
          .andWhere('tenant_id = :tenantId', { tenantId })
          .andWhere(`${SYNC_RUNNING_INDEX_COLUMN} < :syncRunningIndex`, { syncRunningIndex })
          .execute()

        if (result.affected === 0) {
          //get current syncRunningIndex
          const currentSyncRunningIndexResult = await this.manager()
            .createQueryBuilder()
            .select(`${SYNC_RUNNING_INDEX_COLUMN}`)
            .from(entityName, 'entity')
            .where('entity.id = :id', { id: entity['id'] })
            .andWhere('entity.tenant_id = :tenantId', { tenantId })
            .getRawOne()
          const currentSyncRunningIndex = currentSyncRunningIndexResult?.[SYNC_RUNNING_INDEX_COLUMN] ?? 'not found'
          this.logger.debug(
            'No rows updated for entity %s with id %s and tenantId %s (syncRunningIndex: %s, currentSyncRunningIndex: %s)',
            entityName,
            entity['id'],
            tenantId,
            syncRunningIndex,
            currentSyncRunningIndex,
          )
        }
      } else if (message.kind === MessageKind.deleted) {
        const result = await this.manager()
          .createQueryBuilder()
          .update(entityName)
          .set({
            deleted: true,
            [SYNC_RUNNING_INDEX_COLUMN]: syncRunningIndex,
          })
          .where('id = :id', { id: entity['id'] })
          .andWhere('tenant_id = :tenantId', { tenantId })
          .andWhere(`${SYNC_RUNNING_INDEX_COLUMN} < :syncRunningIndex`, { syncRunningIndex })
          .execute()
        if (result.affected === 0) {
          this.logger.debug(
            'No rows updated for deleted entity %s with id %s and tenantId %s (syncRunningIndex: %s)',
            entityName,
            entity['id'],
            tenantId,
            syncRunningIndex,
          )
        }
      } else if (message.kind === MessageKind.sync) {
        const constraintColumns = ['id']
        const entityMetadata = this.connection().getMetadata(entityName)
        const columnNames = entityColumns.map((propertyName) => {
          const column = entityMetadata.findColumnWithPropertyName(propertyName)
          return column?.databaseName || propertyName
        })
        await this.manager()
          .createQueryBuilder()
          .insert()
          .into(entityName)
          .values([entity])
          .orUpdate(
            columnNames.filter((col) => !constraintColumns.includes(col)),
            constraintColumns,
          )
          .execute()
      }
      const listeners = this.notificationListeners.get(entityName)
      if (listeners) {
        await Promise.all(listeners.map((listener) => listener(message)))
      }
    } catch (error) {
      this.logger.error(error, 'error handling entity notification persistance for: %o', message)
    }
  }

  private filteredEntity(entity, entityColumns: string[]) {
    return Object.fromEntries(Object.entries(entity).filter(([key]) => entityColumns.includes(key)))
  }
}
