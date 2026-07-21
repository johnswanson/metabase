import type {
  CreateExplorationRequest,
  Exploration,
  ExplorationId,
  GetExplorationDataRequest,
  GetExplorationDataResponse,
  GetMyExplorationsRequest,
  GetMyExplorationsResponse,
  UpdateExplorationRequest,
} from "metabase-types/api";

import { Api } from "./api";
import { idTag, invalidateTags, listTag, provideMetricListTags } from "./tags";

export const explorationApi = Api.injectEndpoints({
  endpoints: (builder) => ({
    getExplorationData: builder.query<
      GetExplorationDataResponse,
      GetExplorationDataRequest
    >({
      query: (params) => ({
        method: "GET",
        url: "/api/exploration/dimensions",
        params,
      }),
      providesTags: (response) =>
        provideMetricListTags(response?.metrics ?? []),
    }),
    getExploration: builder.query<Exploration, ExplorationId>({
      query: (id) => ({
        method: "GET",
        url: `/api/exploration/${id}`,
      }),
      providesTags: (exploration) =>
        exploration ? [idTag("exploration", exploration.id)] : [],
    }),
    getMyExplorations: builder.query<
      GetMyExplorationsResponse,
      GetMyExplorationsRequest | void
    >({
      query: (params) => ({
        method: "GET",
        url: "/api/exploration/mine",
        params,
      }),
      providesTags: () => [listTag("exploration")],
    }),
    createExploration: builder.mutation<Exploration, CreateExplorationRequest>({
      query: (body) => ({
        method: "POST",
        url: "/api/exploration",
        body,
      }),
      invalidatesTags: (_, error) =>
        invalidateTags(error, [listTag("exploration")]),
    }),
    updateExploration: builder.mutation<Exploration, UpdateExplorationRequest>({
      query: ({ id, ...body }) => ({
        method: "PUT",
        url: `/api/exploration/${id}`,
        body,
      }),
      invalidatesTags: (_, error, { id }) =>
        invalidateTags(error, [
          idTag("exploration", id),
          listTag("exploration"),
        ]),
    }),
    deleteExploration: builder.mutation<void, ExplorationId>({
      query: (id) => ({
        method: "DELETE",
        url: `/api/exploration/${id}`,
      }),
      invalidatesTags: (_, error, id) =>
        invalidateTags(error, [
          idTag("exploration", id),
          listTag("exploration"),
        ]),
    }),
  }),
});

export const {
  useGetExplorationDataQuery,
  useGetExplorationQuery,
  useGetMyExplorationsQuery,
  useCreateExplorationMutation,
  useUpdateExplorationMutation,
  useDeleteExplorationMutation,
} = explorationApi;
