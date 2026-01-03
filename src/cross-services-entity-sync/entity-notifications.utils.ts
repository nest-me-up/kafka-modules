import { PinoLogger } from 'nestjs-pino'
import { EntityManager } from 'typeorm'
import { SYNC_RUNNING_INDEX_COLUMN } from './cross-services-entity-sync.module'

export async function checkSchemaDependencies(tableName: string, manager: EntityManager, logger: PinoLogger) {
  const mandatoryColumns = ['updated', 'created', 'deleted', 'id', 'tenant_id', SYNC_RUNNING_INDEX_COLUMN]
  const query = manager
    .createQueryBuilder()
    .addSelect('column_name')
    .from('information_schema.columns', 'columns')
    .where('table_name = :tableName', { tableName })
    .andWhere('column_name in (:...mandatoryColumns)', { mandatoryColumns })

  const result = await query.getRawMany()
  logger.debug('checking schema dependencies for table %s, result: %o', tableName, result)

  if (result.length !== mandatoryColumns.length) {
    throw new Error(
      `table ${tableName} is missing some of the needed columns: ${mandatoryColumns?.join(', ')}, has: ${result
        ?.map((r) => `'${r.column_name}'`)
        .join(', ')}`,
    )
  }
}
