import { Body, Controller, Delete, Get, Param, Post } from '@nestjs/common';
import { AdminService } from './admin.service.js';

@Controller('v1/admin')
export class AdminController {
  constructor(private readonly admin: AdminService) {}
  @Get('colas') listQueues() { return this.admin.listQueues(); }
  @Post('colas') createQueue(@Body() body: { nombre: string; durable?: boolean }) { return this.admin.createQueue(body); }
  @Delete('colas/:nombre') deleteQueue(@Param('nombre') name: string) { return this.admin.deleteQueue(name); }
  @Get('exchanges') listExchanges() { return this.admin.listExchanges(); }
  @Post('exchanges') createExchange(@Body() body: { nombre: string; tipo: string; durable?: boolean }) { return this.admin.createExchange(body); }
  @Delete('exchanges/:nombre') deleteExchange(@Param('nombre') name: string) { return this.admin.deleteExchange(name); }
  @Get('colas/:nombre/bindings') listBindings(@Param('nombre') name: string) { return this.admin.listBindings(name); }
  @Post('exchanges/:nombre/publicar') publish(@Param('nombre') name: string, @Body() body: { routingKey: string; payload: string }) { return this.admin.publish(name, body); }
  @Get('auditoria') audit() { return this.admin.audit(); }
  @Get('mensajes-muertos') deadLetters() { return this.admin.deadLetters(); }
}
