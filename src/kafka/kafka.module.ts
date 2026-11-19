import { Module } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard.js';
import { RolesGuard } from '../auth/guards/roles.guard.js';
import { JwtVerifyService } from '../auth/services/jwt-verify.service.js';
import { KafkaAdminService } from './kafka-admin.service.js';
import { KafkaController } from './kafka.controller.js';

// E3, paso 2: el administrador de Kafka. KAFKA_BROKERS sale del .env (o del compose).
@Module({
  controllers: [KafkaController],
  providers: [KafkaAdminService, JwtAuthGuard, RolesGuard, JwtVerifyService],
})
export class KafkaModule {}