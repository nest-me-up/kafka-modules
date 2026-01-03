export class EntityEventMessage {
  entityName: string
  kind: MessageKind
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  payload: any
}

export enum MessageKind {
  created = 'created',
  updated = 'updated',
  deleted = 'deleted',
  sync = 'sync',
}
