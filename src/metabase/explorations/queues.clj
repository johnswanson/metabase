(ns metabase.explorations.queues
  "Wires the units of exploration work onto the persistent queue.

    :queue/exploration-plan            one message per started thread

  Starting a thread enqueues a plan. `metabase.explorations.runner` holds the actual work; this
  namespace is only the plumbing.

  Every publish happens inside the transaction that produced the rows the message names, so the
  queues are declared `:transactional :require`: a message exists iff the write that justified it
  committed. That is what lets the handlers be pure idempotency gates — a message can never point at
  a row that isn't there, and a row can never be left with no message coming for it.

  Delivery is at-least-once with a bounded retry budget and no dead-letter queue, so each queue also
  declares an `:on-error` handler. This matters more here than for most work: an exploration's
  completion gate is \"no queries still pending\", and the client polls until the thread completes.
  A batch that exhausted its retries and vanished would strand the row in `pending` and the user on
  a spinner forever. The `:on-error` handler writes the terminal state the UI already knows how to
  render (the planning-failed doc on a thread that never planned), so giving up is something the
  user sees rather than something that hangs."
  (:require
   [metabase.explorations.runner :as runner]
   [metabase.mq.core :as mq]
   [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------- Queues -------------------------------------------

(mq/def-queue! :queue/exploration-plan
  {:transactional :require
   ;; One planner call per thread — a batch of 1 keeps each thread's LLM call on its own
   ;; retry budget rather than making a slow thread's failure re-run its neighbours.
   :max-batch-messages 1
   :on-error (fn [{:keys [messages error]}]
               (log/error error "Exploration planning gave up after exhausting retries"
                          {:thread-ids (mapv :thread-id messages)})
               (doseq [{:keys [thread-id]} messages]
                 ;; fail-plan! terminally stamps a thread that never planned, which is what stops
                 ;; the client polling it.
                 (runner/fail-plan! thread-id (ex-message error))))})

;;; ------------------------------------------ Publishing ------------------------------------------

(defn start-thread!
  "Start `thread-id`'s background processing by enqueuing its planning stage. Call inside the
  transaction that starts the thread, so the thread is planned iff it was really started."
  [thread-id]
  (mq/with-queue :queue/exploration-plan [q]
    (mq/put q {:thread-id thread-id})))

;;; ------------------------------------------- Listeners -------------------------------------------

(mq/def-listener! :queue/exploration-plan [messages]
  (doseq [{:keys [thread-id]} messages]
    (runner/plan-thread! thread-id)))
