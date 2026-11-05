import {
  ConflictException,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  ServiceUnavailableException,
} from '@nestjs/common';
import amqp, { type ChannelModel, type Channel, type ConfirmChannel } from 'amqplib';
import type { CrearColaDto } from './dto/crear-cola.dto.js';
import type { CrearExchangeDto } from './dto/crear-exchange.dto.js';
import type { BindingDto } from './dto/binding.dto.js';

@Injectable()
export class RabbitAdminService implements OnModuleDestroy {
  private readonly log = new Logger(RabbitAdminService.name);
  private readonly url: string;
  private readonly urlAdmin: string;
  private readonly autorizacion: string;
  private conexion: ChannelModel | null = null;

  constructor() {
    this.url = process.env.RABBITMQ_URL ?? `amqp://${process.env.RABBITMQ_USER ?? 'vidalstore'}:${process.env.RABBITMQ_PASS ?? 'vidalstore'}@${process.env.RABBITMQ_HOST ?? 'rabbitmq'}:5672`;
    const host = process.env.RABBITMQ_HOST ?? 'rabbitmq';
    const mgmtPort = process.env.RABBITMQ_MGMT_PORT ?? '15672';
    this.urlAdmin = process.env.RABBITMQ_MGMT_URL ?? `http://${host}:${mgmtPort}`;
    const usuario = process.env.RABBITMQ_USER ?? 'vidalstore';
    const clave   = process.env.RABBITMQ_PASS ?? 'vidalstore';
    this.autorizacion = 'Basic ' + Buffer.from(`${usuario}:${clave}`).toString('base64');
  }

  async onModuleDestroy(): Promise<void> {
    await this.conexion?.close();
    this.conexion = null;
  }

  // --- Colas -----------------------------------------------------------------

  async crearCola(dto: CrearColaDto): Promise<{ cola: string; mensajes: number; consumidores: number }> {
    const argumentos: Record<string, unknown> = {
      'x-queue-type': dto.tipo === 'clasica' ? 'classic' : 'quorum',
    };
    if (dto.dlx) {
      argumentos['x-dead-letter-exchange']    = dto.dlx;
      argumentos['x-dead-letter-routing-key'] = dto.dlxRoutingKey ?? dto.nombre;
    }
    if (dto.maxLargo) {
      argumentos['x-max-length'] = dto.maxLargo;
      argumentos['x-overflow']   = 'reject-publish';
    }

    return this.enUnCanal(async (canal) => {
      const r = await canal.assertQueue(dto.nombre, {
        durable:   dto.durable ?? true,
        arguments: argumentos,
      });
      this.log.log(`cola declarada: ${r.queue}`);
      return { cola: r.queue, mensajes: r.messageCount, consumidores: r.consumerCount };
    });
  }

  async borrarCola(nombre: string): Promise<{ cola: string; mensajesBorrados: number }> {
    return this.enUnCanal(async (canal) => {
      const r = await canal.deleteQueue(nombre, { ifUnused: false, ifEmpty: false });
      this.log.warn(`cola borrada: ${nombre}, se perdieron ${r.messageCount} mensajes`);
      return { cola: nombre, mensajesBorrados: r.messageCount };
    });
  }

  async listarColas(): Promise<unknown[]> {
    const columnas = 'name,type,state,messages_ready,messages_unacknowledged,consumers,node';
    return this.pedirALaApi(`/api/queues/%2F?columns=${columnas}`);
  }

  // --- Exchanges -------------------------------------------------------------

  async crearExchange(dto: CrearExchangeDto): Promise<{ exchange: string; tipo: string }> {
    return this.enUnCanal(async (canal) => {
      const r = await canal.assertExchange(dto.nombre, dto.tipo, { durable: dto.durable ?? true });
      this.log.log(`exchange declarado: ${r.exchange} (${dto.tipo})`);
      return { exchange: r.exchange, tipo: dto.tipo };
    });
  }

  async borrarExchange(nombre: string): Promise<{ exchange: string }> {
    return this.enUnCanal(async (canal) => {
      await canal.deleteExchange(nombre, { ifUnused: false });
      this.log.warn(`exchange borrado: ${nombre}`);
      return { exchange: nombre };
    });
  }

  async listarExchanges(): Promise<unknown[]> {
    return this.pedirALaApi('/api/exchanges/%2F?columns=name,type,durable');
  }

  // --- Bindings --------------------------------------------------------------

  async crearBinding(dto: BindingDto): Promise<BindingDto> {
    return this.enUnCanal(async (canal) => {
      await canal.bindQueue(dto.cola, dto.exchange, dto.routingKey);
      this.log.log(`binding: ${dto.exchange} --${dto.routingKey || '(vacia)'}--> ${dto.cola}`);
      return dto;
    });
  }

  async borrarBinding(dto: BindingDto): Promise<BindingDto> {
    return this.enUnCanal(async (canal) => {
      await canal.unbindQueue(dto.cola, dto.exchange, dto.routingKey);
      this.log.warn(`binding eliminado: ${dto.exchange} -/-> ${dto.cola}`);
      return dto;
    });
  }

  async republicar(exchange: string, routingKey: string, payload: string, headers: Record<string, string>): Promise<void> {
    const conexion = await this.conectar();
    let canal: ConfirmChannel | null = null;
    try {
      canal = await conexion.createConfirmChannel();
      const canalConfirmaciones = canal;
      const publicado = canalConfirmaciones.publish(exchange, routingKey, Buffer.from(payload), {
        persistent: true,
        contentType: 'application/json',
        headers,
      });
      if (!publicado) throw new ServiceUnavailableException('RabbitMQ no acepto el mensaje para reproceso');
      await canalConfirmaciones.waitForConfirms();
      this.log.log(`mensaje reprocesado: ${exchange} --${routingKey}-->`);
    } catch (error) {
      throw this.traducir(error);
    } finally {
      await canal?.close().catch(() => undefined);
    }
  }

  // --- Plomeria interna ------------------------------------------------------

  /**
   * Abre un canal, corre la operación y lo cierra siempre.
   * El canal muere cuando el broker rechaza una operación; reutilizarlo entre
   * peticiones dejaría el servicio inservible tras el primer error.
   * La conexión TCP sí se reutiliza (es lo caro).
   */
  private async enUnCanal<T>(operacion: (canal: Channel) => Promise<T>): Promise<T> {
    const conexion = await this.conectar();
    let canal: Channel | null = null;
    try {
      canal = await conexion.createChannel();
      // Sin este handler, un error de broker sube como "unhandled error event"
      // y tumba el proceso de Node completo.
      canal.on('error', () => undefined);
      return await operacion(canal);
    } catch (e) {
      throw this.traducir(e);
    } finally {
      try {
        await canal?.close();
      } catch {
        // El canal ya venía cerrado por el broker. No hay nada que cerrar.
      }
    }
  }

  private async conectar(): Promise<ChannelModel> {
    if (this.conexion) return this.conexion;
    try {
      this.conexion = await amqp.connect(this.url);
      this.conexion.on('error', (e) => this.log.error(`conexion caida: ${e.message}`));
      this.conexion.on('close', () => { this.conexion = null; });
      return this.conexion;
    } catch (e) {
      let causa = e instanceof Error ? e.message : String(e);
      if (!causa && e && typeof e === 'object' && 'code' in e) causa = String((e as Record<string, unknown>).code);
      throw new ServiceUnavailableException(`no hay conexion con RabbitMQ: ${causa}`);
    }
  }

  /** Traduce errores AMQP al código HTTP correcto. */
  private traducir(e: unknown): Error {
    const mensaje = e instanceof Error ? e.message : String(e);
    if (mensaje.includes('NOT_FOUND'))
      return new NotFoundException(mensaje);
    if (mensaje.includes('PRECONDITION_FAILED') || mensaje.includes('inequivalent'))
      return new ConflictException(mensaje);
    return new ServiceUnavailableException(mensaje);
  }

  private async pedirALaApi(ruta: string): Promise<unknown[]> {
    let respuesta: Response;
    try {
      respuesta = await fetch(`${this.urlAdmin}${ruta}`, {
        headers: { authorization: this.autorizacion },
        signal:  AbortSignal.timeout(5000),
      });
    } catch (e) {
      const causa = e instanceof Error ? e.message : String(e);
      throw new ServiceUnavailableException(`la API de management no responde: ${causa}`);
    }
    if (!respuesta.ok)
      throw new ServiceUnavailableException(`la API de management respondio ${respuesta.status}`);
    return (await respuesta.json()) as unknown[];
  }
}
