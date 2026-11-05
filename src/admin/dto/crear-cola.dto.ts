import {
  IsBoolean,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Matches,
  MaxLength,
  Min,
} from 'class-validator';

const NOMBRE_VALIDO = /^[a-zA-Z0-9._-]+$/;

export const TIPOS_DE_COLA = ['clasica', 'quorum'] as const;
export type TipoDeCola = (typeof TIPOS_DE_COLA)[number];

export class CrearColaDto {
  @IsString({ message: 'nombre tiene que ser texto' })
  @MaxLength(255, { message: 'nombre no puede pasar de 255 caracteres' })
  @Matches(NOMBRE_VALIDO, {
    message: 'nombre solo admite letras, numeros, punto, guion y guion bajo',
  })
  nombre!: string;

  @IsOptional()
  @IsIn(TIPOS_DE_COLA, { message: `tipo tiene que ser uno de: ${TIPOS_DE_COLA.join(', ')}` })
  tipo?: TipoDeCola;

  @IsOptional()
  @IsBoolean({ message: 'durable es true o false' })
  durable?: boolean;

  @IsOptional()
  @IsString()
  @MaxLength(255)
  @Matches(NOMBRE_VALIDO, { message: 'dlx no es un nombre valido de exchange' })
  dlx?: string;

  @IsOptional()
  @IsString()
  @MaxLength(255)
  dlxRoutingKey?: string;

  @IsOptional()
  @IsInt({ message: 'maxLargo tiene que ser un numero entero' })
  @Min(1, { message: 'maxLargo tiene que ser 1 o mas' })
  maxLargo?: number;
}
