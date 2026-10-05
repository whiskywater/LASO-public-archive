# ADR 0007: Worker interactions use a narrow durable request channel

## Context

External workers sometimes need an approval, permission decision, or human
answer while a durable worker job is running. Arbitrary callbacks into LASO
would bypass policy and make restart behavior ambiguous.

## Decision

The versioned process protocol permits only correlated `approval`, `permission`,
and `question` requests. LASO persists each request by request ID, evaluates
policy for permissions/approvals, exposes pending decisions through the API, and
replays terminal decisions for duplicate request IDs. Job cancellation cancels
pending requests; restart never auto-approves them. The channel is not general
RPC or recursive pipeline execution.

## Consequences

Native worker ABI behavior remains unchanged. Process adapters can map supported
vendor interactions without placing vendor code in Core. Pending human work can
survive restart, while an unresponsive worker or lost external interaction is
still reported conservatively. More request types require a versioned protocol
decision and bounded policy semantics.

## Supersedes

None; this records the current additive interaction boundary.
