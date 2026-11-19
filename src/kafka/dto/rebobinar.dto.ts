import { IsIn, IsISO8601, IsOptional, IsString, Matches, ValidateIf } from 'class-validator';
import { MENSAJE_NOMBRE_TOPIC, NOMBRE_TOPIC } from './crear-topic.dto.js';

export const DESTINOS = ['principio', 'final', 'fecha'] as const;
export type Destino = (typeof DESTINOS)[number];

/**
 * A donde se lleva el grupo: al principio (reprocesar todo), al final (saltarse lo pendiente) o al primer
 * mensaje publicado desde una fecha. La fecha es el timestamp de Kafka (cuando el mensaje llego al cluster), no el
 * emitidoEn del evento. Sin topic, se mueven todos los topics en los que el grupo tiene offsets.
 */
export class RebobinarDto {
  @IsIn(DESTINOS, { message: `hacia tiene que ser uno de: ${DESTINOS.join(', ')}` })
  hacia!: Destino;

  @ValidateIf((dto: RebobinarDto) => dto.hacia === 'fecha')
  @IsISO8601({ strict: true }, { message: 'con hacia "fecha", fecha es obligatoria y va en ISO 8601, por ejemplo "2026-11-07T00:30:00Z"' })
  fecha?: string;

  @IsOptional()
  @IsString({ message: 'topic tiene que ser texto' })
  @Matches(NOMBRE_TOPIC, { message: MENSAJE_NOMBRE_TOPIC })
  topic?: string;
}