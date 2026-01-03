import { ConfigService } from '@nestjs/config'
import { CompressionTypes } from 'kafkajs'
import { PinoLogger } from 'nestjs-pino'

export class KafkaConfig {
  broker_host: string
  broker_port: number
  brokers: string[] //will be created from broker_host and broker_port
  group: string
  groupId: string //will be created from group and topic
  maxBytes: number
  maxBytesPerPartition: number
  consumerSessionTimeout: number
  consumerHeartbeatInterval: number
  consumerMaxWaitTimeInMs: number
  consumerRebalanceTimeout: number
  consumptionType: KafkaConsumptionType
  compressionThreshold?: number // Size in bytes above which to compress messages
  compression: string
  compressionType?: CompressionTypes // Compression type will be created from compression string, to use (defaults to GZIP)
}

export enum KafkaConsumptionType {
  shared = 'shared',
  broadcast = 'broadcast',
}

export function getEmitterConfig(configService: ConfigService, logger: PinoLogger) {
  const config = configService.get<KafkaConfig>('kafka')
  if (!config) {
    throw new Error('Missing kafka in configuration')
  }

  config.brokers = Array.isArray(config.broker_host)
    ? config.broker_host.map((host) => `${host}:${config.broker_port}`)
    : [`${config.broker_host}:${config.broker_port}`]
  config.groupId = `${config.group}-consumer`

  config.maxBytes = config.maxBytes || 10485760
  config.maxBytesPerPartition = config.maxBytesPerPartition || 10485760
  config.consumerSessionTimeout = config.consumerSessionTimeout || 2 * 60 * 1000
  config.consumerHeartbeatInterval = config.consumerHeartbeatInterval || 3000
  config.consumerMaxWaitTimeInMs = config.consumerMaxWaitTimeInMs || 100
  config.consumerRebalanceTimeout = config.consumerRebalanceTimeout || 10000
  config.consumptionType = config.consumptionType || KafkaConsumptionType.shared
  config.compressionThreshold = config.compressionThreshold || 786432 // 768 KB - above this size, compress messages
  config.compressionType = config.compression //if compression enabled use the compression type from the config, otherwise use GZIP
    ? (CompressionTypes[config.compression as keyof typeof CompressionTypes] as CompressionTypes)
    : CompressionTypes.GZIP

  //validate consumer heartbeat interval
  if (config.consumerHeartbeatInterval > config.consumerSessionTimeout) {
    logger.error(
      'KafkaConfig: consumerSessionTimeout must be greater than consumerHeartbeatInterval. config: %o',
      config,
    )
    throw new Error('KafkaConfig: consumerSessionTimeout must be greater than consumerHeartbeatInterval')
  }
  if (
    config.consumerHeartbeatInterval < (config.consumerSessionTimeout / 3) * 0.8 ||
    config.consumerHeartbeatInterval > (config.consumerSessionTimeout / 3) * 1.2
  ) {
    logger.warn('KafkaConfig: consumerHeartbeatInterval should be around 1/3 of consumerSessionTimeout')
  }

  //validate rebalance timeout
  //should be larger than the heartbeat but smaller than the session timeout
  if (config.consumerRebalanceTimeout < config.consumerHeartbeatInterval) {
    logger.error(
      'KafkaConfig: consumerRebalanceTimeout must be greater than consumerHeartbeatInterval. config: %o',
      config,
    )
    throw new Error('KafkaConfig: consumerRebalanceTimeout must be greater than consumerHeartbeatInterval')
  }
  if (config.consumerRebalanceTimeout > config.consumerSessionTimeout) {
    logger.error('KafkaConfig: consumerRebalanceTimeout must be less than consumerSessionTimeout. config: %o', config)
    throw new Error('KafkaConfig: consumerRebalanceTimeout must be less than consumerSessionTimeout')
  }

  //validate consumerMaxWaitTimeInMs
  //should be larger than the heartbeat but smaller than the session timeout
  if (config.consumerMaxWaitTimeInMs > config.consumerHeartbeatInterval) {
    logger.error('KafkaConfig: consumerMaxWaitTimeInMs must be less than consumerHeartbeatInterval. config: %o', config)
    throw new Error('KafkaConfig: consumerMaxWaitTimeInMs must be less than consumerHeartbeatInterval')
  }
  if (config.consumerMaxWaitTimeInMs > config.consumerSessionTimeout) {
    logger.error('KafkaConfig: consumerMaxWaitTimeInMs must be less than consumerSessionTimeout. config: %o', config)
    throw new Error('KafkaConfig: consumerMaxWaitTimeInMs must be less than consumerSessionTimeout')
  }

  if (config.maxBytesPerPartition > config.maxBytes) {
    logger.error('KafkaConfig: maxBytesPerPartition must be less than maxBytes. config: %o', config)
    throw new Error('KafkaConfig: maxBytesPerPartition must be less than maxBytes')
  }
  return config
}
