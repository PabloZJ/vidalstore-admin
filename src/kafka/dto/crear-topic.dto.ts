import { Type } from 'class-transformer';
import { IsInt, IsObject, IsOptional, IsString, Matches, Max, Min, ValidateNested } from 'class-validator';
import { ConfigsTopicDto } from './configs-topic.dto.js';

// Lo que Kafka acepta en un nombre de topic: letras, numeros, punto, guion y guion bajo, hasta 249.
export const NOMBRE_TOPIC = /^[a-zA-Z0-9._-]{1,249}$/;
export const MENSAJE_NOMBRE_TOPIC = 'el nombre de un topic solo admite letras, numeros, punto, guion y guion bajo (hasta 249)';
// Tope del administrador, no de Kafka: mas particiones que instancias posibles no leen mas rapido.
export const MAXIMO_PARTICIONES = 50;

export class CrearTopicDto {
  @IsString({ message: 'nombre tiene que ser texto' })
  @Matches(NOMBRE_TOPIC, { message: MENSAJE_NOMBRE_TOPIC })
  nombre!: string;

  @IsInt({ message: 'particiones tiene que ser un numero entero' })
  @Min(1, { message: 'particiones tiene que ser 1 o mas' })
  @Max(MAXIMO_PARTICIONES, { message: `particiones no puede pasar de ${MAXIMO_PARTICIONES}` })
  particiones!: number;

  @IsInt({ message: 'replicas tiene que ser un numero entero' })
  @Min(1, { message: 'replicas tiene que ser 1 o mas' })
  @Max(3, { message: 'replicas no puede pasar de 3: el cluster tiene 3 brokers y cada copia vive en uno distinto' })
  replicas!: number;

  @IsOptional()
  @IsObject({ message: 'configs es un objeto, por ejemplo {"retention.ms": "604800000"}' })
  @ValidateNested()
  @Type(() => ConfigsTopicDto)
  configs?: ConfigsTopicDto;
}