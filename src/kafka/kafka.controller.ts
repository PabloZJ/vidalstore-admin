import { Body, Controller, Delete, Get, HttpCode, Param, Patch, Post, Query, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard.js';
import { RolesGuard } from '../auth/guards/roles.guard.js';
import { Roles } from '../auth/decorators/roles.decorator.js';
import { ConfigsTopicDto } from './dto/configs-topic.dto.js';
import { CrearTopicDto } from './dto/crear-topic.dto.js';
import { AumentarParticionesDto } from './dto/particiones.dto.js';
import { RebobinarDto } from './dto/rebobinar.dto.js';
import { KafkaAdminService } from './kafka-admin.service.js';

/**
 * Las 7 rutas de Kafka de la E3, con las mismas reglas que las de RabbitMQ: token de Cognito y grupo
 * administradores. No las expone el gateway: se usan por el tunel de Session Manager a gestion. El controlador
 * no conoce el cliente de Kafka: todo pasa por KafkaAdminService.
 */
@Controller('v1/admin/kafka')
@UseGuards(JwtAuthGuard, RolesGuard)
@Roles('administradores')
export class KafkaController {
  constructor(private readonly kafka: KafkaAdminService) {}

  @Get('topics')
  listarTopics() {
    return this.kafka.listarTopics();
  }

  @Post('topics')
  crearTopic(@Body() dto: CrearTopicDto) {
    return this.kafka.crearTopic(dto);
  }

  @Patch('topics/:nombre/config')
  cambiarConfig(@Param('nombre') nombre: string, @Body() dto: ConfigsTopicDto) {
    return this.kafka.cambiarConfig(nombre, dto);
  }

  // 200 y no 201: no crea un recurso nuevo, cambia uno que ya existe.
  @Post('topics/:nombre/particiones')
  @HttpCode(200)
  aumentarParticiones(@Param('nombre') nombre: string, @Body() dto: AumentarParticionesDto) {
    return this.kafka.aumentarParticiones(nombre, dto.particiones);
  }

  @Delete('topics/:nombre')
  borrarTopic(@Param('nombre') nombre: string, @Query('confirmar') confirmar?: string) {
    return this.kafka.borrarTopic(nombre, confirmar);
  }

  @Get('grupos')
  listarGrupos() {
    return this.kafka.listarGrupos();
  }

  @Post('grupos/:id/offsets')
  @HttpCode(200)
  rebobinar(@Param('id') id: string, @Body() dto: RebobinarDto) {
    return this.kafka.rebobinar(id, dto);
  }
}