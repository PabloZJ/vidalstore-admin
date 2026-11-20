import {
  BadRequestException,
  ConflictException,
  NotFoundException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { beforeEach, describe, expect, it, vi } from 'vitest';

// El servicio construye su propio Admin en el constructor. Se reemplaza el modulo completo para no abrir
// conexiones reales y poder controlar cada respuesta del cluster.
const { adminMock, CONFIG_DINAMICA } = vi.hoisted(() => {
  const adminMock = {
    listTopics: vi.fn(),
    createTopics: vi.fn(),
    incrementalAlterConfigs: vi.fn(),
    createPartitions: vi.fn(),
    deleteTopics: vi.fn(),
    listGroups: vi.fn(),
    describeGroups: vi.fn(),
    listConsumerGroupOffsets: vi.fn(),
    alterConsumerGroupOffsets: vi.fn(),
    metadata: vi.fn(),
    describeConfigs: vi.fn(),
    listOffsets: vi.fn(),
    close: vi.fn(),
  };
  return { adminMock, CONFIG_DINAMICA: 1 };
});

vi.mock('@platformatic/kafka', () => ({
  Admin: vi.fn(function () {
    return adminMock;
  }),
  ConfigResourceTypes: { TOPIC: 2 },
  ConfigSources: { DYNAMIC_TOPIC_CONFIG: CONFIG_DINAMICA },
  IncrementalAlterConfigOperationTypes: { SET: 0 },
}));

import { KafkaAdminService } from './kafka-admin.service.js';

const NOMBRE_TOPIC = 'juego.eventos';

const particionesDe = (cantidad: number) =>
  Array.from({ length: cantidad }, (_, indice) => ({
    leader: indice % 3,
    replicas: [0, 1, 2],
    isr: [0, 1, 2],
  }));

const metadataDe = (cantidad: number) => ({
  partitionsCount: cantidad,
  partitions: particionesDe(cantidad),
});

const errorDeKafka = (apiId: string, serverErrorMessage?: string) =>
  Object.assign(new Error(apiId), { apiId, serverErrorMessage });

describe('KafkaAdminService', () => {
  let servicio: KafkaAdminService;

  beforeEach(() => {
    vi.clearAllMocks();
    adminMock.listTopics.mockResolvedValue([]);
    adminMock.createTopics.mockResolvedValue(undefined);
    adminMock.incrementalAlterConfigs.mockResolvedValue(undefined);
    adminMock.createPartitions.mockResolvedValue(undefined);
    adminMock.deleteTopics.mockResolvedValue(undefined);
    adminMock.listGroups.mockResolvedValue(new Map());
    adminMock.describeGroups.mockResolvedValue(new Map());
    adminMock.listConsumerGroupOffsets.mockResolvedValue([]);
    adminMock.alterConsumerGroupOffsets.mockResolvedValue(undefined);
    adminMock.metadata.mockResolvedValue({ topics: new Map() });
    adminMock.describeConfigs.mockResolvedValue([]);
    adminMock.listOffsets.mockResolvedValue([]);
    adminMock.close.mockResolvedValue(undefined);

    servicio = new KafkaAdminService();
  });

  /** El topic existe y la metadata se arma para el nombre que pida el servicio. */
  const prepararTopic = (nombre = NOMBRE_TOPIC, cantidad = 3) => {
    adminMock.listTopics.mockResolvedValue([nombre]);
    adminMock.metadata.mockImplementation(
      ({ topics }: { topics: string[] }) => ({
        topics: new Map(topics.map((topic) => [topic, metadataDe(cantidad)])),
      }),
    );
  };

  describe('topics', () => {
    it('lista los topics ordenados con particiones, replicas y configs propias', async () => {
      adminMock.listTopics.mockResolvedValue(['zeta.topic', 'alfa.topic']);
      adminMock.metadata.mockImplementation(
        ({ topics }: { topics: string[] }) => ({
          topics: new Map(topics.map((topic) => [topic, metadataDe(2)])),
        }),
      );
      adminMock.describeConfigs.mockResolvedValue([
        {
          resourceName: 'alfa.topic',
          configs: [
            {
              name: 'retention.ms',
              value: '604800000',
              configSource: CONFIG_DINAMICA,
            },
            {
              name: 'cleanup.policy',
              value: 'delete',
              configSource: CONFIG_DINAMICA,
            },
          ],
        },
        {
          resourceName: 'zeta.topic',
          configs: [
            {
              name: 'cleanup.policy',
              value: 'compact',
              configSource: CONFIG_DINAMICA,
            },
          ],
        },
      ]);

      const topics = await servicio.listarTopics();

      expect(topics.map((t) => t.nombre)).toEqual(['alfa.topic', 'zeta.topic']);
      expect(topics[0]).toMatchObject({
        nombre: 'alfa.topic',
        particiones: 2,
        replicas: 3,
        configs: { 'retention.ms': '604800000', 'cleanup.policy': 'delete' },
      });
      expect(topics[0].detalle).toHaveLength(2);
      expect(topics[0].detalle[1]).toEqual({
        particion: 1,
        lider: 1,
        replicas: [0, 1, 2],
        isr: [0, 1, 2],
      });
      expect(adminMock.describeConfigs).toHaveBeenCalledWith({
        resources: [
          { resourceType: 2, resourceName: 'alfa.topic' },
          { resourceType: 2, resourceName: 'zeta.topic' },
        ],
      });
    });

    it('crea un topic con sus configs y devuelve la metadata resultante', async () => {
      prepararTopic('nuevo.topic');

      const topic = await servicio.crearTopic({
        nombre: 'nuevo.topic',
        particiones: 3,
        replicas: 3,
        configs: { 'retention.ms': '604800000' },
      });

      expect(adminMock.createTopics).toHaveBeenCalledWith({
        topics: ['nuevo.topic'],
        partitions: 3,
        replicas: 3,
        configs: [{ name: 'retention.ms', value: '604800000' }],
      });
      expect(topic.nombre).toBe('nuevo.topic');
      expect(topic.particiones).toBe(3);
    });

    it('rechaza un nombre de topic invalido antes de llamar a Kafka', async () => {
      await expect(
        servicio.crearTopic({
          nombre: 'topic con espacio',
          particiones: 1,
          replicas: 1,
        }),
      ).rejects.toBeInstanceOf(BadRequestException);

      expect(adminMock.createTopics).not.toHaveBeenCalled();
    });

    it('traduce TOPIC_ALREADY_EXISTS a 409', async () => {
      prepararTopic('nuevo.topic');
      adminMock.createTopics.mockRejectedValue(
        errorDeKafka('TOPIC_ALREADY_EXISTS', 'ya existe'),
      );

      await expect(
        servicio.crearTopic({
          nombre: 'nuevo.topic',
          particiones: 1,
          replicas: 1,
        }),
      ).rejects.toBeInstanceOf(ConflictException);
    });

    it('busca el codigo de Kafka aunque venga anidado en varios errores', async () => {
      prepararTopic('nuevo.topic');
      adminMock.createTopics.mockRejectedValue({
        errors: [
          { errors: [errorDeKafka('TOPIC_ALREADY_EXISTS', 'ya existe')] },
        ],
      });

      await expect(
        servicio.crearTopic({
          nombre: 'nuevo.topic',
          particiones: 1,
          replicas: 1,
        }),
      ).rejects.toBeInstanceOf(ConflictException);
    });

    it('exige al menos una config al cambiarlas', async () => {
      prepararTopic();

      await expect(
        servicio.cambiarConfig(NOMBRE_TOPIC, {}),
      ).rejects.toBeInstanceOf(BadRequestException);
      expect(adminMock.incrementalAlterConfigs).not.toHaveBeenCalled();
    });

    it('cambia configs con SET y devuelve el topic actualizado', async () => {
      prepararTopic();

      await servicio.cambiarConfig(NOMBRE_TOPIC, {
        'retention.ms': '86400000',
        'cleanup.policy': 'compact',
      });

      expect(adminMock.incrementalAlterConfigs).toHaveBeenCalledWith({
        resources: [
          {
            resourceType: 2,
            resourceName: NOMBRE_TOPIC,
            configs: [
              { name: 'retention.ms', configOperation: 0, value: '86400000' },
              { name: 'cleanup.policy', configOperation: 0, value: 'compact' },
            ],
          },
        ],
      });
    });

    it('responde 404 si el topic no existe al cambiar configs', async () => {
      adminMock.listTopics.mockResolvedValue([]);

      await expect(
        servicio.cambiarConfig('no.existe', { 'retention.ms': '1000' }),
      ).rejects.toBeInstanceOf(NotFoundException);
      expect(adminMock.incrementalAlterConfigs).not.toHaveBeenCalled();
    });

    it('no permite disminuir particiones ni dejar el mismo numero', async () => {
      prepararTopic(NOMBRE_TOPIC, 6);

      await expect(
        servicio.aumentarParticiones(NOMBRE_TOPIC, 3),
      ).rejects.toBeInstanceOf(BadRequestException);
      await expect(
        servicio.aumentarParticiones(NOMBRE_TOPIC, 6),
      ).rejects.toBeInstanceOf(BadRequestException);
      expect(adminMock.createPartitions).not.toHaveBeenCalled();
    });

    it('aumenta particiones y advierte que el orden por key cambia', async () => {
      adminMock.listTopics.mockResolvedValue([NOMBRE_TOPIC]);
      adminMock.metadata
        .mockResolvedValueOnce({
          topics: new Map([[NOMBRE_TOPIC, metadataDe(3)]]),
        })
        .mockResolvedValueOnce({
          topics: new Map([[NOMBRE_TOPIC, metadataDe(6)]]),
        });

      const topic = await servicio.aumentarParticiones(NOMBRE_TOPIC, 6);

      expect(adminMock.createPartitions).toHaveBeenCalledWith({
        topics: [{ name: NOMBRE_TOPIC, count: 6 }],
      });
      expect(topic.particiones).toBe(6);
      expect(topic.aviso).toContain('3');
      expect(topic.aviso).toContain('6');
    });

    it('protege los topics vidalstore.* y exige repetir el nombre para borrarlos', async () => {
      prepararTopic('vidalstore.pedidos');

      await expect(
        servicio.borrarTopic('vidalstore.pedidos'),
      ).rejects.toBeInstanceOf(BadRequestException);
      expect(adminMock.deleteTopics).not.toHaveBeenCalled();

      const resultado = await servicio.borrarTopic(
        'vidalstore.pedidos',
        'vidalstore.pedidos',
      );

      expect(adminMock.deleteTopics).toHaveBeenCalledWith({
        topics: ['vidalstore.pedidos'],
      });
      expect(resultado).toEqual({
        borrado: 'vidalstore.pedidos',
        particiones: 3,
      });
    });

    it('borra un topic propio sin pedir confirmacion', async () => {
      prepararTopic('temporal');

      const resultado = await servicio.borrarTopic('temporal');

      expect(adminMock.deleteTopics).toHaveBeenCalledWith({
        topics: ['temporal'],
      });
      expect(resultado).toEqual({ borrado: 'temporal', particiones: 3 });
    });

    it('no toca los topics internos de Kafka', async () => {
      await expect(
        servicio.borrarTopic('__consumer_offsets'),
      ).rejects.toBeInstanceOf(BadRequestException);
      expect(adminMock.deleteTopics).not.toHaveBeenCalled();
    });
  });

  describe('consumer groups', () => {
    it('lista grupos con estado, miembros y lag por particion', async () => {
      adminMock.listTopics.mockResolvedValue([NOMBRE_TOPIC]);
      adminMock.metadata.mockResolvedValue({
        topics: new Map([[NOMBRE_TOPIC, metadataDe(2)]]),
      });
      adminMock.listGroups.mockResolvedValue(
        new Map([['vidalstore-panel', {}]]),
      );
      adminMock.describeGroups.mockResolvedValue(
        new Map([
          [
            'vidalstore-panel',
            { state: 'Stable', members: new Map([['m1', {}]]) },
          ],
        ]),
      );
      adminMock.listConsumerGroupOffsets.mockResolvedValue([
        {
          groupId: 'vidalstore-panel',
          topics: [
            {
              name: NOMBRE_TOPIC,
              partitions: [
                { partitionIndex: 0, committedOffset: 10n },
                { partitionIndex: 1, committedOffset: -1n },
              ],
            },
          ],
        },
      ]);
      adminMock.listOffsets.mockResolvedValue([
        {
          name: NOMBRE_TOPIC,
          partitions: [
            { partitionIndex: 0, offset: 15n },
            { partitionIndex: 1, offset: 8n },
          ],
        },
      ]);

      const grupos = await servicio.listarGrupos();

      expect(grupos).toHaveLength(1);
      expect(grupos[0]).toMatchObject({
        grupo: 'vidalstore-panel',
        estado: 'Stable',
        miembros: 1,
        lagTotal: 5,
      });
      expect(grupos[0].topics[0].particiones).toEqual([
        { particion: 0, confirmado: 10, final: 15, lag: 5 },
        { particion: 1, confirmado: null, final: 8, lag: null },
      ]);
    });

    it('responde 404 al rebobinar un grupo inexistente', async () => {
      adminMock.listGroups.mockResolvedValue(new Map());

      await expect(
        servicio.rebobinar('no-existe', { hacia: 'principio' }),
      ).rejects.toBeInstanceOf(NotFoundException);
      expect(adminMock.alterConsumerGroupOffsets).not.toHaveBeenCalled();
    });

    it('responde 409 si el grupo tiene consumidores conectados', async () => {
      adminMock.listGroups.mockResolvedValue(
        new Map([['vidalstore-panel', {}]]),
      );
      adminMock.describeGroups.mockResolvedValue(
        new Map([
          [
            'vidalstore-panel',
            { state: 'Stable', members: new Map([['m1', {}]]) },
          ],
        ]),
      );

      await expect(
        servicio.rebobinar('vidalstore-panel', { hacia: 'principio' }),
      ).rejects.toBeInstanceOf(ConflictException);
      expect(adminMock.alterConsumerGroupOffsets).not.toHaveBeenCalled();
    });

    it('rebobina al principio y devuelve los offsets antes y despues', async () => {
      prepararTopic(NOMBRE_TOPIC, 1);
      adminMock.listGroups.mockResolvedValue(
        new Map([['vidalstore-panel', {}]]),
      );
      adminMock.describeGroups.mockResolvedValue(
        new Map([['vidalstore-panel', { state: 'Empty', members: new Map() }]]),
      );
      adminMock.listConsumerGroupOffsets.mockResolvedValue([
        {
          groupId: 'vidalstore-panel',
          topics: [
            {
              name: NOMBRE_TOPIC,
              partitions: [{ partitionIndex: 0, committedOffset: 10n }],
            },
          ],
        },
      ]);
      adminMock.listOffsets.mockResolvedValue([
        { name: NOMBRE_TOPIC, partitions: [{ partitionIndex: 0, offset: 0n }] },
      ]);

      const resultado = await servicio.rebobinar('vidalstore-panel', {
        hacia: 'principio',
      });

      expect(adminMock.listOffsets).toHaveBeenCalledWith({
        topics: [
          {
            name: NOMBRE_TOPIC,
            partitions: [{ partitionIndex: 0, timestamp: -2n }],
          },
        ],
      });
      expect(adminMock.alterConsumerGroupOffsets).toHaveBeenCalledWith({
        groupId: 'vidalstore-panel',
        topics: [
          {
            name: NOMBRE_TOPIC,
            partitionOffsets: [{ partition: 0, offset: 0n }],
          },
        ],
      });
      expect(resultado.topics[0].particiones[0]).toEqual({
        particion: 0,
        antes: 10,
        ahora: 0,
      });
    });

    it('rebobina a una fecha usando el timestamp de Kafka', async () => {
      prepararTopic(NOMBRE_TOPIC, 1);
      adminMock.listGroups.mockResolvedValue(
        new Map([['vidalstore-panel', {}]]),
      );
      adminMock.describeGroups.mockResolvedValue(
        new Map([['vidalstore-panel', { state: 'Empty', members: new Map() }]]),
      );
      adminMock.listConsumerGroupOffsets.mockResolvedValue([
        {
          groupId: 'vidalstore-panel',
          topics: [
            {
              name: NOMBRE_TOPIC,
              partitions: [{ partitionIndex: 0, committedOffset: 10n }],
            },
          ],
        },
      ]);
      adminMock.listOffsets.mockResolvedValue([
        { name: NOMBRE_TOPIC, partitions: [{ partitionIndex: 0, offset: 5n }] },
      ]);

      await servicio.rebobinar('vidalstore-panel', {
        hacia: 'fecha',
        fecha: '2026-11-07T00:30:00Z',
        topic: NOMBRE_TOPIC,
      });

      expect(adminMock.listOffsets).toHaveBeenNthCalledWith(1, {
        topics: [
          {
            name: NOMBRE_TOPIC,
            partitions: [
              {
                partitionIndex: 0,
                timestamp: BigInt(Date.parse('2026-11-07T00:30:00Z')),
              },
            ],
          },
        ],
      });
      // Despues consulta el final de cada particion para resolver el -1 de Kafka.
      expect(adminMock.listOffsets).toHaveBeenNthCalledWith(2, {
        topics: [
          {
            name: NOMBRE_TOPIC,
            partitions: [{ partitionIndex: 0, timestamp: -1n }],
          },
        ],
      });
      expect(adminMock.alterConsumerGroupOffsets).toHaveBeenCalledWith({
        groupId: 'vidalstore-panel',
        topics: [
          {
            name: NOMBRE_TOPIC,
            partitionOffsets: [{ partition: 0, offset: 5n }],
          },
        ],
      });
    });

    it('rechaza ids de grupo con caracteres invalidos', async () => {
      await expect(
        servicio.rebobinar('grupo con espacio', { hacia: 'principio' }),
      ).rejects.toBeInstanceOf(BadRequestException);
    });
  });

  describe('errores y cierre', () => {
    it('traduce un error de protocolo desconocido a 503', async () => {
      adminMock.listTopics.mockRejectedValue(
        errorDeKafka('SOMETHING_ELSE', 'raro'),
      );

      await expect(servicio.listarTopics()).rejects.toBeInstanceOf(
        ServiceUnavailableException,
      );
    });

    it('traduce INVALID_CONFIG a 400', async () => {
      adminMock.listTopics.mockRejectedValue(
        errorDeKafka('INVALID_CONFIG', 'retention.ms invalido'),
      );

      await expect(servicio.listarTopics()).rejects.toBeInstanceOf(
        BadRequestException,
      );
    });

    it('traduce un cluster caido a 503', async () => {
      adminMock.listTopics.mockRejectedValue(
        new Error('ECONNREFUSED 127.0.0.1:9092'),
      );

      await expect(servicio.listarTopics()).rejects.toBeInstanceOf(
        ServiceUnavailableException,
      );
    });

    it('cierra el Admin al destruir el modulo', async () => {
      await servicio.onModuleDestroy();

      expect(adminMock.close).toHaveBeenCalledTimes(1);
    });
  });
});
