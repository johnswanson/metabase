(ns ^:synchronous metabase.explorations.queues-test
  "End-to-end tests for the exploration queues.

  `mq.tu/with-test-mq` starts the real subsystem on an in-memory backend and realizes every
  `def-queue!`/`def-listener!` in the codebase — including the ones in
  `metabase.explorations.queues` — so these exercise the production wiring, not a stand-in.

  Marked `^:synchronous` because `with-test-mq` installs its backend with `alter-var-root` — it is
  process-global, so two of these running in parallel would clobber each other's backend."
  (:require
   [clojure.test :refer :all]
   [metabase.explorations.queues]
   [metabase.mq.queue.registry :as q.registry]
   [metabase.mq.test-util :as mq.tu]
   [metabase.test.fixtures :as fixtures]))

(set! *warn-on-reflection* true)

(use-fixtures :once (fixtures/initialize :db))

(deftest queues-declare-their-batching-and-concurrency-test
  ;; with-test-mq is what realizes the `def-queue!` declarations into the registry.
  (mq.tu/with-test-mq [_ctx]
    (testing "planning is uncapped: it is the pipeline's intake — one short LLM call, published one
              per click — so throttling it would only make one user's exploration wait behind
              another's before anything is on screen"
      (is (nil? (q.registry/max-concurrent-batches :queue/exploration-plan)))
      (is (= 1 (q.registry/max-batch-messages :queue/exploration-plan))))))
