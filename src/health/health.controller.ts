import { Controller, Get } from '@nestjs/common';
import { HealthService } from './health.service.js';

@Controller()
export class HealthController {
  constructor(private readonly health: HealthService) {}
  @Get() check() { return this.health.check(); }
  @Get('salud') salud() { return this.health.check(); }
}
