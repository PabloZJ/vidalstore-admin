import {
  INestApplication,
  UnauthorizedException,
  ValidationPipe,
} from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import { JwtVerifyService } from '../src/auth/services/jwt-verify.service.js';
import { KafkaAdminService } from '../src/kafka/kafka-admin.service.js';
import { KafkaModule } from '../src/kafka/kafka.module.js';

const claimsAdmin = {
  sub: 'admin-1',
  client_id: 'cliente-admin',
  token_use: 'access',
  'cognito:groups': ['administradores'],
};

const claimsCliente = {
  sub: 'cliente-1',
  client_id: 'cliente-admin',
  token_use: 'access',
  'cognito:groups': ['clientes'],
};

const kafkaMock = {
  listarTopics: vi.fn(),
  crearTopic: vi.fn(),
  cambiarConfig: vi.fn(),
  aumentarParticiones: vi.fn(),
  borrarTopic: vi.fn(),
  listarGrupos: vi.fn(),
  rebobinar: vi.fn(),
};

// No se valida un JWT real: se reemplaza el verificador por tokens de prueba que representan los grupos de Cognito.
const jwtMock = {
  verifyToken: vi.fn(async (authHeader?: string) => {
    if (authHeader === 'Bearer admin') return claimsAdmin;
    if (authHeader === 'Bearer cliente') return claimsCliente;
    if (authHeader === 'Bearer sin-grupos')
      return { ...claimsAdmin, 'cognito:groups': [] };
    throw new UnauthorizedException('Token invalido o firma no verificada');
  }),
};

const topic = {
  nombre: 'juego.eventos',
  particiones: 3,
  replicas: 3,
  configs: {},
  detalle: [],
};

describe('Kafka admin (e2e)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleFixture = await Test.createTestingModule({
      imports: [KafkaModule],
    })
      .overrideProvider(KafkaAdminService)
      .useValue(kafkaMock)
      .overrideProvider(JwtVerifyService)
      .useValue(jwtMock)
      .compile();

    app = moduleFixture.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
        transformOptions: { enableImplicitConversion: false },
      }),
    );
    await app.init();
  });

  beforeEach(() => {
    vi.clearAllMocks();
    kafkaMock.listarTopics.mockResolvedValue([]);
    kafkaMock.listarGrupos.mockResolvedValue([]);
    kafkaMock.crearTopic.mockResolvedValue(topic);
    kafkaMock.cambiarConfig.mockResolvedValue(topic);
    kafkaMock.aumentarParticiones.mockResolvedValue({
      ...topic,
      particiones: 6,
      aviso: 'ok',
    });
    kafkaMock.borrarTopic.mockResolvedValue({
      borrado: 'temporal',
      particiones: 3,
    });
    kafkaMock.rebobinar.mockResolvedValue({
      grupo: 'vidalstore-panel',
      hacia: 'principio',
      topics: [],
    });
  });

  afterAll(async () => {
    await app.close();
  });

  describe('autenticacion y autorizacion', () => {
    it('401 sin token', async () => {
      const respuesta = await request(app.getHttpServer())
        .get('/v1/admin/kafka/topics')
        .expect(401);

      expect(respuesta.body.message).toContain('Sin token');
    });

    it('401 con token invalido', async () => {
      await request(app.getHttpServer())
        .get('/v1/admin/kafka/topics')
        .set('Authorization', 'Bearer roto')
        .expect(401);
    });

    it('403 con token valido de cliente', async () => {
      await request(app.getHttpServer())
        .get('/v1/admin/kafka/topics')
        .set('Authorization', 'Bearer cliente')
        .expect(403);
    });

    it('403 si el token no trae grupos', async () => {
      await request(app.getHttpServer())
        .get('/v1/admin/kafka/topics')
        .set('Authorization', 'Bearer sin-grupos')
        .expect(403);
    });

    it('401 en una ruta de escritura sin token', async () => {
      await request(app.getHttpServer())
        .delete('/v1/admin/kafka/topics/temporal')
        .expect(401);
    });
  });

  describe('topics', () => {
    it('200 lista topics con token de administrador', async () => {
      const respuesta = await request(app.getHttpServer())
        .get('/v1/admin/kafka/topics')
        .set('Authorization', 'Bearer admin')
        .expect(200);

      expect(respuesta.body).toEqual([]);
      expect(kafkaMock.listarTopics).toHaveBeenCalledTimes(1);
    });

    it('400 si las particiones estan fuera del rango', async () => {
      await request(app.getHttpServer())
        .post('/v1/admin/kafka/topics')
        .set('Authorization', 'Bearer admin')
        .send({ nombre: 'nuevo.topic', particiones: 0, replicas: 3 })
        .expect(400);

      expect(kafkaMock.crearTopic).not.toHaveBeenCalled();
    });

    it('400 con un campo que no esta en el DTO', async () => {
      await request(app.getHttpServer())
        .post('/v1/admin/kafka/topics')
        .set('Authorization', 'Bearer admin')
        .send({
          nombre: 'nuevo.topic',
          particiones: 3,
          replicas: 3,
          sorpresa: true,
        })
        .expect(400);

      expect(kafkaMock.crearTopic).not.toHaveBeenCalled();
    });

    it('201 crea un topic valido', async () => {
      await request(app.getHttpServer())
        .post('/v1/admin/kafka/topics')
        .set('Authorization', 'Bearer admin')
        .send({
          nombre: 'nuevo.topic',
          particiones: 3,
          replicas: 3,
          configs: { 'retention.ms': '604800000' },
        })
        .expect(201);

      expect(kafkaMock.crearTopic).toHaveBeenCalledWith(
        expect.objectContaining({
          nombre: 'nuevo.topic',
          particiones: 3,
          replicas: 3,
        }),
      );
    });

    it('200 cambia las configs de un topic', async () => {
      await request(app.getHttpServer())
        .patch('/v1/admin/kafka/topics/juego.eventos/config')
        .set('Authorization', 'Bearer admin')
        .send({ 'retention.ms': '86400000' })
        .expect(200);

      expect(kafkaMock.cambiarConfig).toHaveBeenCalledWith(
        'juego.eventos',
        expect.objectContaining({ 'retention.ms': '86400000' }),
      );
    });

    it('200 aumenta las particiones de un topic', async () => {
      await request(app.getHttpServer())
        .post('/v1/admin/kafka/topics/juego.eventos/particiones')
        .set('Authorization', 'Bearer admin')
        .send({ particiones: 6 })
        .expect(200);

      expect(kafkaMock.aumentarParticiones).toHaveBeenCalledWith(
        'juego.eventos',
        6,
      );
    });

    it('200 borra un topic propio', async () => {
      await request(app.getHttpServer())
        .delete('/v1/admin/kafka/topics/temporal')
        .set('Authorization', 'Bearer admin')
        .expect(200);

      expect(kafkaMock.borrarTopic).toHaveBeenCalledWith('temporal', undefined);
    });
  });

  describe('consumer groups', () => {
    it('200 lista los grupos con token de administrador', async () => {
      const respuesta = await request(app.getHttpServer())
        .get('/v1/admin/kafka/grupos')
        .set('Authorization', 'Bearer admin')
        .expect(200);

      expect(respuesta.body).toEqual([]);
      expect(kafkaMock.listarGrupos).toHaveBeenCalledTimes(1);
    });

    it('200 rebobina los offsets de un grupo', async () => {
      await request(app.getHttpServer())
        .post('/v1/admin/kafka/grupos/vidalstore-panel/offsets')
        .set('Authorization', 'Bearer admin')
        .send({ hacia: 'principio' })
        .expect(200);

      expect(kafkaMock.rebobinar).toHaveBeenCalledWith(
        'vidalstore-panel',
        expect.objectContaining({ hacia: 'principio' }),
      );
    });

    it('400 si hacia es fecha y falta la fecha', async () => {
      await request(app.getHttpServer())
        .post('/v1/admin/kafka/grupos/vidalstore-panel/offsets')
        .set('Authorization', 'Bearer admin')
        .send({ hacia: 'fecha' })
        .expect(400);

      expect(kafkaMock.rebobinar).not.toHaveBeenCalled();
    });
  });
});
