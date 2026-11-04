export interface Claims {
  sub: string;
  client_id: string;
  token_use: string;
  'cognito:groups'?: string[];
}
