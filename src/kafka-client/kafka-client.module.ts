import { Module } from '@nestjs/common'
import { KafkaClientService } from './kafka-client.service'

@Module({
  imports: [],
  providers: [KafkaClientService],
  exports: [KafkaClientService],
})
export class KafkaClientModule {}
