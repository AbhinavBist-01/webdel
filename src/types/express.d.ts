export interface AuthenticatedCustomer {
  id: string;
  name: string;
}

declare global {
  namespace Express {
    interface Request {
      customer?: AuthenticatedCustomer;
    }
  }
}
