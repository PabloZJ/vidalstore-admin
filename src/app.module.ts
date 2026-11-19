import { Module } from '@nestjs/common';
import { AdminController } from './admin/admin.controller.js';
import { AdminService } from './admin/admin.service.js';
import { RabbitAdminService } from './admin/rabbit-admin.service.js';
import { DatabaseService } from './database/database.service.js';
import { HealthController } from './health/health.controller.js';
import { HealthService } from './health/health.service.js';
import { JwtAuthGuard } from './auth/guards/jwt-auth.guard.js';
import { RolesGuard } from './auth/guards/roles.guard.js';
import { JwtVerifyService } from './auth/services/jwt-verify.service.js';
import { KafkaModule } from './kafka/kafka.module.js';

@Module({
  imports: [KafkaModule],
  controllers: [AdminController, HealthController],
  providers: [
    AdminService,
    RabbitAdminService,
    DatabaseService,
    HealthService,
    JwtAuthGuard,
    RolesGuard,
    JwtVerifyService,
  ],
})
export class AppModule {}
