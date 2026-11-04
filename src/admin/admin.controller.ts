import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  Param,
  Post,
  UseGuards,
} from '@nestjs/common';
import { RabbitAdminService } from './rabbit-admin.service.js';
import { AdminService } from './admin.service.js';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard.js';
import { RolesGuard } from '../auth/guards/roles.guard.js';
import { Roles } from '../auth/decorators/roles.decorator.js';
import { CrearColaDto } from './dto/crear-cola.dto.js';
import { CrearExchangeDto } from './dto/crear-exchange.dto.js';
import { BindingDto } from './dto/binding.dto.js';

@Controller('v1/admin')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('administradores')
export class AdminController {
  constructor(
    private readonly admin: RabbitAdminService,
    private readonly adminService: AdminService,
  ) {}

  // --- Colas -----------------------------------------------------------------

  @Get('colas')
  listarColas() {
    return this.admin.listarColas();
  }

  @Post('colas')
  crearCola(@Body() dto: CrearColaDto) {
    return this.admin.crearCola(dto);
  }

  @Delete('colas/:nombre')
  borrarCola(@Param('nombre') nombre: string) {
    return this.admin.borrarCola(nombre);
  }

  // --- Exchanges -------------------------------------------------------------

  @Get('exchanges')
  listarExchanges() {
    return this.admin.listarExchanges();
  }

  @Post('exchanges')
  crearExchange(@Body() dto: CrearExchangeDto) {
    return this.admin.crearExchange(dto);
  }

  @Delete('exchanges/:nombre')
  borrarExchange(@Param('nombre') nombre: string) {
    return this.admin.borrarExchange(nombre);
  }

  // --- Bindings (se identifican por la terna, no por un id en la URL) --------

  @Post('bindings')
  crearBinding(@Body() dto: BindingDto) {
    return this.admin.crearBinding(dto);
  }

  @Delete('bindings')
  @HttpCode(200)
  borrarBinding(@Body() dto: BindingDto) {
    return this.admin.borrarBinding(dto);
  }

  // --- Auditoría y mensajes muertos (DB) ------------------------------------

  @Get('auditoria')
  auditoria() {
    return this.adminService.audit();
  }

  @Get('mensajes-muertos')
  mensajesMuertos() {
    return this.adminService.deadLetters();
  }
}
