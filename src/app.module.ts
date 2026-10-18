import { Module } from '@nestjs/common';
import { AdminController } from './admin/admin.controller.js';
import { AdminService } from './admin/admin.service.js';
import { DatabaseService } from './database/database.service.js';
import { HealthController } from './health/health.controller.js';
import { HealthService } from './health/health.service.js';
import { RabbitMqManagementService } from './rabbitmq/rabbitmq-management.service.js';

@Module({
  controllers: [AdminController, HealthController],
  providers: [AdminService, DatabaseService, HealthService, RabbitMqManagementService],
})
export class AppModule {}
