import { Injectable, UnauthorizedException } from '@nestjs/common';
import { createRemoteJWKSet, jwtVerify } from 'jose';
import type { Claims } from '../types/claims.type.js';

@Injectable()
export class JwtVerifyService {
  private readonly issuer = process.env.COGNITO_ISSUER ?? '';
  private readonly clientId = process.env.COGNITO_CLIENT_ID ?? '';
  private readonly jwks = this.issuer
    ? createRemoteJWKSet(new URL(`${this.issuer}/.well-known/jwks.json`))
    : null;

  async verifyToken(authHeader?: string): Promise<Claims> {
    if (!authHeader?.startsWith('Bearer '))
      throw new UnauthorizedException('Sin token en Authorization header');
    if (!this.jwks || !this.clientId)
      throw new UnauthorizedException('Autenticacion del administrador no configurada');

    try {
      const { payload } = await jwtVerify(authHeader.slice(7), this.jwks, {
        issuer: this.issuer,
      });
      if (payload.token_use !== 'access' || payload.client_id !== this.clientId)
        throw new UnauthorizedException('Token no valido para esta aplicacion');
      return payload as unknown as Claims;
    } catch (error) {
      if (error instanceof UnauthorizedException) throw error;
      throw new UnauthorizedException('Token invalido o firma no verificada');
    }
  }
}
