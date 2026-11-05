import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import type { Request } from 'express';
import { JwtVerifyService } from '../services/jwt-verify.service.js';

@Injectable()
export class JwtAuthGuard implements CanActivate {
  constructor(private readonly jwtVerifyService: JwtVerifyService) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<Request>();
    const authHeader = request.headers.authorization;
    if (!authHeader) throw new UnauthorizedException('Sin token en Authorization header');
    const claims = await this.jwtVerifyService.verifyToken(authHeader);
    (request as unknown as Record<string, unknown>).user = claims;
    return true;
  }
}
