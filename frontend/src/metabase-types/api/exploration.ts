import type { Collection, CollectionId } from "./collection";
import type { PaginationRequest, PaginationResponse } from "./pagination";
import type { UserId } from "./user";

export type ExplorationId = number;
export type ExplorationThreadId = number;

export interface CreateExplorationRequest {
  name: string;
  description?: string | null;
  prompt?: string | null;
  collection_id?: CollectionId | null;
}

export interface UpdateExplorationRequest {
  id: ExplorationId;
  name?: string | null;
  description?: string | null;
  archived?: boolean;
  collection_id?: CollectionId | null;
  collection_position?: number | null;
}

export interface ExplorationThread {
  id: ExplorationThreadId;
  exploration_id: ExplorationId;
  name: string | null;
  prompt: string | null;
  position: number;
  started_at: string | null;
  entity_id: string;
  created_at: string;
  updated_at: string;
}

export interface ExplorationCreator {
  id: UserId;
  email: string;
  first_name: string | null;
  last_name: string | null;
}

export interface ExplorationSummary {
  id: ExplorationId;
  name: string;
  description?: string | null;
  creator_id: UserId;
  creator?: ExplorationCreator;
  collection_id?: CollectionId | null;
  collection?: Pick<Collection, "id" | "name"> | null;
  archived?: boolean;
  created_at: string;
  updated_at: string;
  current_user_last_touched_at: string;
}

export type GetMyExplorationsRequest = PaginationRequest;

export type GetMyExplorationsResponse = {
  data: ExplorationSummary[];
} & PaginationResponse;

export interface Exploration {
  id: ExplorationId;
  name: string;
  description: string | null;
  creator_id: UserId;
  archived: boolean;
  collection_id: CollectionId | null;
  collection_position: number | null;
  collection?: Collection | null;
  entity_id: string;
  created_at: string;
  updated_at: string;
  creator?: ExplorationCreator;
  threads?: ExplorationThread[];
  can_write: boolean;
}
