import { IsInt, Max, Min } from 'class-validator';
import { MAXIMO_PARTICIONES } from './crear-topic.dto.js';

/** El total de particiones que debe quedar, no cuantas se agregan (igual que kafka-topics.sh --alter). */
export class AumentarParticionesDto {
  @IsInt({ message: 'particiones tiene que ser un numero entero: el total que debe quedar' })
  @Min(1, { message: 'particiones tiene que ser 1 o mas' })
  @Max(MAXIMO_PARTICIONES, { message: `particiones no puede pasar de ${MAXIMO_PARTICIONES}` })
  particiones!: number;
}