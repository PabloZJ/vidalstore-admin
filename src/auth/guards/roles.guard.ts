import { CanActivate, ExecutionContext, ForbiddenException, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { Request } from 'express';
import { ROLES_KEY } from '../decorators/roles.decorator.js';
import type { Claims } from '../types/claims.type.js';

@Injectable()
export class RolesGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const requiredRoles = this.reflector.getAllAndOverride<string[]>(ROLES_KEY, [
      context.getHandler(),
      context.getClass(),
    ]) ?? [];
    if (requiredRoles.length === 0) return true;

    const request = context.switchToHttp().getRequest<Request>();
    const user = (request as unknown as Record<string, unknown>).user as Claims | undefined;
    if (!user) throw new ForbiddenException('No hay claims de usuario en la solicitud');
    const groups = user['cognito:groups'] ?? [];
    if (!requiredRoles.some((role) => groups.includes(role)))
      throw new ForbiddenException(`Se requiere uno de los siguientes roles: [${requiredRoles.join(', ')}]`);
    return true;
  }
}
