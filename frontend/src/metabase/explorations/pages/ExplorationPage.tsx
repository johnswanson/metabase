import { push } from "react-router-redux";
import { t } from "ttag";

import {
  useDeleteExplorationMutation,
  useGetExplorationQuery,
} from "metabase/api";
import { LoadingAndErrorWrapper } from "metabase/common/components/LoadingAndErrorWrapper";
import { useDispatch } from "metabase/redux";
import { Box, Button, Group, Stack, Text, Title } from "metabase/ui";
import * as Urls from "metabase/urls";

export function ExplorationPage(props: { params: { id: string } }) {
  const dispatch = useDispatch();
  const id = Number(props.params.id);
  const { data: exploration, isLoading, error } = useGetExplorationQuery(id);
  const [deleteExploration] = useDeleteExplorationMutation();

  const handleDelete = async () => {
    await deleteExploration(id).unwrap();
    dispatch(push(Urls.newExploration()));
  };

  return (
    <LoadingAndErrorWrapper loading={isLoading} error={error}>
      {exploration && (
        <Box p="xl">
          <Group justify="space-between" mb="md">
            <Title order={1}>{exploration.name}</Title>
            {exploration.can_write && (
              <Button variant="subtle" color="error" onClick={handleDelete}>
                {t`Delete`}
              </Button>
            )}
          </Group>
          {exploration.description && (
            <Text mb="md">{exploration.description}</Text>
          )}
          <Stack gap="xs">
            <Text c="text-secondary">{t`This exploration is empty.`}</Text>
          </Stack>
        </Box>
      )}
    </LoadingAndErrorWrapper>
  );
}
