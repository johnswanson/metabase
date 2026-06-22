import { Link } from "react-router";
import { push } from "react-router-redux";
import { t } from "ttag";

import {
  useCreateExplorationMutation,
  useGetMyExplorationsQuery,
} from "metabase/api";
import { LoadingAndErrorWrapper } from "metabase/common/components/LoadingAndErrorWrapper";
import { useDispatch } from "metabase/redux";
import { Box, Button, Stack, Text, Title } from "metabase/ui";
import * as Urls from "metabase/urls";

export function NewExplorationPage() {
  const dispatch = useDispatch();
  const { data, isLoading, error } = useGetMyExplorationsQuery();
  const [createExploration, { isLoading: isCreating }] =
    useCreateExplorationMutation();

  const handleCreate = async () => {
    const exploration = await createExploration({
      name: t`New exploration`,
    }).unwrap();
    dispatch(push(Urls.exploration(exploration.id)));
  };

  const explorations = data?.data ?? [];

  return (
    <LoadingAndErrorWrapper loading={isLoading} error={error}>
      <Box p="xl">
        <Title order={1} mb="md">{t`Explorations`}</Title>
        <Button
          variant="filled"
          loading={isCreating}
          onClick={handleCreate}
          mb="lg"
        >{t`New exploration`}</Button>
        <Stack gap="sm">
          {explorations.map((exploration) => (
            <Link key={exploration.id} to={Urls.exploration(exploration.id)}>
              {exploration.name}
            </Link>
          ))}
          {explorations.length === 0 && (
            <Text c="text-secondary">{t`No explorations yet.`}</Text>
          )}
        </Stack>
      </Box>
    </LoadingAndErrorWrapper>
  );
}
