--------------------------- MODULE DurableSubmission ---------------------------
(***************************************************************************)
(* Bounded model of the durable Submission protocol of                    *)
(* `@effect-agent/session` (docs/spec/durability.md): one Conversation    *)
(* lane, FIFO Submissions, worker Attempts with ownership + producer-     *)
(* epoch fencing, canonical-history-first recovery classification, tool   *)
(* uncertainty (DUR-009/DUR-017), approval suspension, joined input       *)
(* (DUR-016), abort (DUR-012), and terminalization (DUR-011).             *)
(*                                                                         *)
(* Every action corresponds to a coordinator function or ledger operation *)
(* named in formal/CORRESPONDENCE.md; the abstraction assumptions are     *)
(* stated there.  Crash is modeled as a nondeterministic choice at every  *)
(* PlusCal label -- i.e. between any two durable mutations -- consuming a *)
(* bounded fault budget.  `FencingEnabled = FALSE` is a committed         *)
(* negative control: it disables ownership/epoch checks so TLC            *)
(* demonstrates the invariants are load-bearing, not vacuous.             *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Subs,            \* Submission queue as an initial segment of 1..n; index = queueSequence (DUR-004)
  Workers,         \* worker identities (model values)
  MaxFaults,       \* combined crash + premature-lease-expiry budget
  FencingEnabled   \* TRUE for the real protocol; FALSE = negative control

NoSub == 0
Outcomes == {"completed", "aborted"}

(* --algorithm DurableSubmission

variables
  \* -- SubmissionLedger rows (ledger.ts SubmissionState; "running" folded into ownership) --
  lstate = [s \in Subs |-> "unsubmitted"],
  inputMarked = [s \in Subs |-> FALSE],      \* markInputApplied marker
  abortIntent = [s \in Subs |-> FALSE],      \* requestAbort intent row
  resv = [s \in Subs |-> "none"],            \* reserveSettlement reserved outcome
  resvFinal = [s \in Subs |-> FALSE],        \* finalizeSettlement applied
  hostOf = [s \in Subs |-> NoSub],           \* claimJoining host linkage
  apprDecided = [s \in Subs |-> FALSE],      \* recordApprovalDecision intent
  unknownRes = [s \in Subs |-> "none"],      \* recordUnknownResolution intent: none|completed|never
  \* -- ConversationStore canonical records (per Submission, digest chain abstracted) --
  convMat = FALSE,                           \* materialize + ConversationCreated
  inputRec = [s \in Subs |-> FALSE],         \* UserInputRecorded input:{sid}
  respKind = [s \in Subs |-> "none"],        \* committed complete model response: none|finish|tool|approval
  prepared = [s \in Subs |-> FALSE],         \* ToolCallPrepared without outcome
  callSettled = [s \in Subs |-> FALSE],      \* ToolCallSettled / ToolCallResolved
  callAudited = [s \in Subs |-> FALSE],      \* ToolCallUnknown audit record (abort path)
  apprReq = [s \in Subs |-> FALSE],          \* ToolApprovalRequested canonical record
  settRec = [s \in Subs |-> "none"],         \* SubmissionSettled canonical outcome
  \* -- external world --
  extEffect = [s \in Subs |-> "none"],       \* ordinary tool side effect: none|maybe|done
  \* -- lane ownership + fencing (claim/renew/release; producer epoch DUR-006) --
  laneEpoch = 0,
  laneOwner = "none",
  leaseLive = FALSE,
  \* -- bounded fault budget --
  faults = 0,
  \* -- ghost evidence counters (capped to keep the state space finite) --
  inputAppends = [s \in Subs |-> 0],
  settAppends = [s \in Subs |-> 0],
  staleWrites = 0,                           \* canonical/ledger mutation by a superseded Attempt
  unknownViol = 0;                           \* canonical model/tool record on an unknown lane

define
  Unsettled == {s \in Subs : lstate[s] \notin {"unsubmitted", "settled"}}
  Head == IF Unsettled = {} THEN NoSub
          ELSE CHOOSE s \in Unsettled : \A t \in Unsettled : s <= t
  OpenCall(s) == prepared[s] /\ ~callSettled[s] /\ ~callAudited[s]
  \* claim: FIFO-head rule; never grants admitted/joining/joined/suspended/unknown heads
  Claimable ==
    /\ Head # NoSub
    /\ lstate[Head] \in {"ready", "input-applied", "terminalizing"}
    /\ (laneOwner = "none" \/ ~leaseLive)
  LaneQuiet == laneOwner = "none" \/ ~leaseLive
  CapInc(n) == IF n < 2 THEN n + 1 ELSE n

  \* Pure recovery classifier: packages/session/src/recovery.ts classifyRecovery,
  \* restricted to the non-Subagent rows (rows 1-8 and 10-12 of its precedence doc).
  Classify(s) ==
    IF lstate[s] = "settled" THEN "NoAction"
    ELSE IF settRec[s] # "none" THEN "FinalizeLedgerFromHistory"
    ELSE IF resv[s] # "none" THEN
      IF resvFinal[s] THEN "FinalizeLedgerFromHistory" ELSE "AppendReservedSettlement"
    ELSE IF lstate[s] = "joining" THEN
      IF inputRec[s] THEN "RepairJoinMarker" ELSE "RevertJoining"
    ELSE IF lstate[s] = "joined" THEN
      IF hostOf[s] # NoSub /\ settRec[hostOf[s]] # "none"
        THEN "SettleJoinedWithHost" ELSE "AwaitHostSettlement"
    ELSE IF abortIntent[s] THEN "SettleAborted"
    ELSE IF lstate[s] # "unknown" /\ OpenCall(s) THEN "MarkUnknown"
    ELSE IF lstate[s] = "unknown" THEN
      IF OpenCall(s) /\ unknownRes[s] = "none"
        THEN "AwaitUnknownResolution" ELSE "ApplyUnknownResolutions"
    ELSE IF lstate[s] = "suspended" THEN
      IF apprDecided[s] THEN "ResumeSuspended" ELSE "AwaitApprovalDecision"
    ELSE IF apprReq[s] /\ ~apprDecided[s] THEN "AwaitApprovalDecision"
    ELSE IF respKind[s] = "tool" /\ ~prepared[s] /\ ~callSettled[s]
      THEN "ResumePendingToolBatch"
    ELSE IF respKind[s] = "approval" /\ ~apprReq[s] THEN "ResumePendingToolBatch"
    ELSE IF lstate[s] = "admitted" THEN
      IF convMat THEN "RepairReadiness" ELSE "CompleteMaterialization"
    ELSE IF ~inputRec[s] THEN "ApplyInput"
    ELSE IF ~inputMarked[s] THEN "RepairInputMarker"
    ELSE "ResumeFromTurnBoundary"

  \* Decisions the recovery process executes directly (the rest resume via a worker claim).
  RecoveryStep(s) ==
    LET d == Classify(s) IN
    \/ d \in {"FinalizeLedgerFromHistory", "RevertJoining", "RepairJoinMarker",
              "SettleJoinedWithHost", "ApplyUnknownResolutions", "ResumeSuspended",
              "CompleteMaterialization", "RepairReadiness"}
    \/ (d \in {"AppendReservedSettlement", "SettleAborted", "MarkUnknown"} /\ s = Head)
  RecoveryActionable(s) ==
    /\ lstate[s] \notin {"unsubmitted", "settled"}
    /\ LaneQuiet
    /\ RecoveryStep(s)

  MaxSub == CHOOSE s \in Subs : \A t \in Subs : t <= s
end define;

\* =========================== worker processes ===========================
\* One durable worker (DurableAgentRuntime.runWorker / drainConversation).
\* Every label is one durable step; the `either` crash branch at each label
\* models process loss between two durable mutations.
fair process worker \in Workers
variables sub = NoSub, ep = 0, jt = NoSub;
begin
WIdle:
  await Claimable;
  laneEpoch := laneEpoch + 1;   \* ledger.claim: atomic epoch bump (DUR-006)
  laneOwner := self;
  leaseLive := TRUE;
  ep := laneEpoch;
  sub := Head;
WResume:
  \* drainConversation resume dispatch over canonical evidence + ledger row
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if sub = NoSub \/ lstate[sub] = "settled" then
      \* ledger.releaseOwnership: graceful drain when nothing is owed here
      if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
      sub := NoSub; goto WIdle;
    elsif settRec[sub] # "none" then goto WFin;
    elsif resv[sub] # "none" then goto WSettApp;
    elsif abortIntent[sub] then goto WAbortAudit;
    elsif ~inputRec[sub] then goto WInput;
    elsif ~inputMarked[sub] then goto WMarkInput;
    elsif respKind[sub] = "none" then goto WTurn;
    elsif OpenCall(sub) then goto WMarkUnknown;
    elsif respKind[sub] = "tool" then
      if callSettled[sub] then goto WReserve; else goto WPrepare; end if;
    elsif respKind[sub] = "approval" then
      if ~apprReq[sub] then goto WApprReq;
      elsif ~apprDecided[sub] then goto WSuspend;
      elsif callSettled[sub] then goto WReserve;
      else goto WPrepare;
      end if;
    else goto WReserve;   \* respKind = "finish"
    end if;
  end either;
WInput:
  \* applyCanonicalInput: fenced append of the deterministic input:{sid} record (DUR-007)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ ep # laneEpoch then
      sub := NoSub; goto WIdle;    \* FenceRejected: superseded epoch (DUR-006)
    else
      if ep # laneEpoch then staleWrites := 1; end if;
      if lstate[sub] = "unknown" then unknownViol := 1; end if;
      if ~inputRec[sub] then
        inputRec[sub] := TRUE;
        inputAppends[sub] := CapInc(inputAppends[sub]);
      end if;
    end if;
  end either;
WMarkInput:
  \* ledger.markInputApplied (ownership-token-guarded marker)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ laneOwner # self then
      sub := NoSub; goto WIdle;    \* OwnershipLost
    else
      if laneOwner # self then staleWrites := 1; end if;
      lstate[sub] := "input-applied";
      inputMarked[sub] := TRUE;
    end if;
  end either;
WTurn:
  \* Turn boundary: abort honored first (durability SS13); otherwise claim joining
  \* input (plan SS2.5) or invoke the model and append one complete response.
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if abortIntent[sub] then goto WAbortAudit; end if;
JoinOrRespond:
    either
      \* ledger.claimJoining: contiguous strictly-later ready prefix (here:
      \* sub+1).  An abort-intended row is not re-claimed: the coordinator
      \* claims joining input at most once per Turn boundary and reverts an
      \* aborted claim (durable-runtime.ts 3199-3206); modeling the re-claim
      \* would need a Turn-progress counter for the same behavior.  Abort
      \* arriving BETWEEN claim and delivery still exercises the revert path.
      await sub < MaxSub /\ lstate[sub + 1] = "ready" /\ ~abortIntent[sub + 1];
      if FencingEnabled /\ laneOwner # self then
        sub := NoSub; goto WIdle;
      else
        if laneOwner # self then staleWrites := 1; end if;
        jt := sub + 1;
        lstate[sub + 1] := "joining";
        hostOf[sub + 1] := sub;
        goto WJoinDeliver;
      end if;
    or
      \* model invocation: one complete response commits atomically (durability SS9)
      with kind \in {"finish", "tool", "approval"} do
        if FencingEnabled /\ ep # laneEpoch then
          sub := NoSub; goto WIdle;
        else
          if ep # laneEpoch then staleWrites := 1; end if;
          if lstate[sub] = "unknown" then unknownViol := 1; end if;
          respKind[sub] := kind;
          if kind = "finish" then goto WReserve;
          elsif kind = "tool" then goto WPrepare;
          else goto WApprReq;
          end if;
        end if;
      end with;
    end either;
  end either;
WJoinDeliver:
  \* abort recorded before the host consumed the input: revert-then-abort (plan SS2.5);
  \* otherwise fenced append of the joined input:{sid} record (DUR-016)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub; jt := NoSub;
    goto WIdle;
  or
    if abortIntent[jt] then
      lstate[jt] := "ready";      \* ledger.revertJoining
      hostOf[jt] := NoSub;
      jt := NoSub;
      goto WTurn;
    elsif FencingEnabled /\ ep # laneEpoch then
      sub := NoSub; jt := NoSub; goto WIdle;
    else
      if ep # laneEpoch then staleWrites := 1; end if;
      if ~inputRec[jt] then
        inputRec[jt] := TRUE;
        inputAppends[jt] := CapInc(inputAppends[jt]);
      end if;
    end if;
  end either;
WJoinMark:
  \* ledger.markJoined: strictly joining -> joined under the recorded host
  \* linkage; a row that recovery reverted in between has no linkage and the
  \* operation FAILS (sqlite-ledger.ts markJoined) -- the host continues its
  \* turn without the join and the reverted input reattaches later (DUR-016)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub; jt := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ laneOwner # self then
      sub := NoSub; jt := NoSub; goto WIdle;
    else
      if laneOwner # self then staleWrites := 1; end if;
      if lstate[jt] = "joining" /\ hostOf[jt] # NoSub then
        lstate[jt] := "joined";
      end if;
      jt := NoSub;
      goto WTurn;
    end if;
  end either;
WPrepare:
  \* fenced append of ToolCallPrepared (durability SS10)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ ep # laneEpoch then
      sub := NoSub; goto WIdle;
    else
      if ep # laneEpoch then staleWrites := 1; end if;
      if lstate[sub] = "unknown" then unknownViol := 1; end if;
      prepared[sub] := TRUE;
    end if;
  end either;
WExec:
  \* ordinary tool invocation: an EXTERNAL effect -- deliberately unfenced; a
  \* superseded Attempt can still fire it (that is why Unknown exists, DUR-009)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    extEffect[sub] := "maybe";
  end either;
WSettleCall:
  \* fenced append of ToolCallSettled after output validation
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ ep # laneEpoch then
      sub := NoSub; goto WIdle;
    else
      if ep # laneEpoch then staleWrites := 1; end if;
      if lstate[sub] = "unknown" then unknownViol := 1; end if;
      callSettled[sub] := TRUE;
      extEffect[sub] := "done";
      goto WReserve;
    end if;
  end either;
WApprReq:
  \* fenced append of ToolApprovalRequested (ADR-0012: the request record is the
  \* suspension's entire canonical footprint)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ ep # laneEpoch then
      sub := NoSub; goto WIdle;
    else
      if ep # laneEpoch then staleWrites := 1; end if;
      if lstate[sub] = "unknown" then unknownViol := 1; end if;
      apprReq[sub] := TRUE;
    end if;
  end either;
WSuspend:
  \* ledger.suspend(ApprovalPending): ends the ownership period without settling;
  \* returns resume-immediately when the reason is already covered
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ laneOwner # self then
      sub := NoSub; goto WIdle;
    else
      if laneOwner # self then staleWrites := 1; end if;
      if apprDecided[sub] then
        goto WPrepare;             \* resume-immediately: decision raced ahead
      else
        lstate[sub] := "suspended";
        laneOwner := "none";
        leaseLive := FALSE;
        sub := NoSub;
        goto WIdle;
      end if;
    end if;
  end either;
WMarkUnknown:
  \* reconcile-then-mark (no reconciler proof available): ledger.markUnknown;
  \* the lane blocks and stops consuming worker permits (DUR-009/DUR-017)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ laneOwner # self then
      sub := NoSub; goto WIdle;
    else
      if laneOwner # self then staleWrites := 1; end if;
      lstate[sub] := "unknown";
      laneOwner := "none";
      leaseLive := FALSE;
      sub := NoSub;
      goto WIdle;
    end if;
  end either;
WAbortAudit:
  \* settleAborted: open ordinary calls become ToolCallUnknown audit records --
  \* abort settles the obligation but never asserts external rollback (SS13)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if OpenCall(sub) then
      if FencingEnabled /\ ep # laneEpoch then
        sub := NoSub; goto WIdle;
      else
        if ep # laneEpoch then staleWrites := 1; end if;
        callAudited[sub] := TRUE;
      end if;
    end if;
  end either;
WAbortReserve:
  \* ledger.reserveSettlement(aborted) (DUR-011/DUR-012)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ laneOwner # self then
      sub := NoSub; goto WIdle;
    else
      if laneOwner # self then staleWrites := 1; end if;
      if resv[sub] = "none" then
        resv[sub] := "aborted";
        lstate[sub] := "terminalizing";
      end if;
      goto WSettApp;
    end if;
  end either;
WReserve:
  \* ledger.reserveSettlement(completed): one exact reserved outcome (DUR-011)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ laneOwner # self then
      sub := NoSub; goto WIdle;
    else
      if laneOwner # self then staleWrites := 1; end if;
      if resv[sub] = "none" then
        resv[sub] := "completed";
        lstate[sub] := "terminalizing";
      end if;
    end if;
  end either;
WSettApp:
  \* fenced append of the EXACT reserved SubmissionSettled record (DUR-011 step 2)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    if FencingEnabled /\ ep # laneEpoch then
      sub := NoSub; goto WIdle;
    else
      if ep # laneEpoch then staleWrites := 1; end if;
      if settRec[sub] = "none" then
        settRec[sub] := resv[sub];
        settAppends[sub] := CapInc(settAppends[sub]);
      end if;
    end if;
  end either;
WFin:
  \* ledger.finalizeSettlement: canonical history authorizes it, no token (DUR-015)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
    sub := NoSub;
    goto WIdle;
  or
    resvFinal[sub] := TRUE;
    lstate[sub] := "settled";
    if laneOwner = self then laneOwner := "none" || leaseLive := FALSE; end if;
  end either;
WJoinedSettle:
  \* settleJoinedSubmissions: each joined Submission is owed its own settlement
  \* with the host outcome (DUR-002, plan SS2.5)
  either
    await faults < MaxFaults;
    faults := faults + 1;
    sub := NoSub;
    goto WIdle;
  or
    if \E t \in Subs : hostOf[t] = sub /\ lstate[t] = "joined" then
      with t \in {t \in Subs : hostOf[t] = sub /\ lstate[t] = "joined"} do
        resv[t] := settRec[sub];   \* host-linkage-authorized reserve (no token)
        lstate[t] := "terminalizing";
      end with;
      goto WJoinedApp;
    else
      sub := NoSub;
      goto WIdle;
    end if;
  end either;
WJoinedApp:
  either
    await faults < MaxFaults;
    faults := faults + 1;
    sub := NoSub;
    goto WIdle;
  or
    if \E t \in Subs : hostOf[t] = sub /\ lstate[t] = "terminalizing" then
      with t \in {t \in Subs : hostOf[t] = sub /\ lstate[t] = "terminalizing"} do
        if FencingEnabled /\ ep # laneEpoch then
          sub := NoSub; goto WIdle;
        else
          if ep # laneEpoch then staleWrites := 1; end if;
          if settRec[t] = "none" then
            settRec[t] := resv[t];
            settAppends[t] := CapInc(settAppends[t]);
          end if;
          resvFinal[t] := TRUE;
          lstate[t] := "settled";
          goto WJoinedSettle;
        end if;
      end with;
    else
      goto WJoinedSettle;
    end if;
  end either;
end process;

\* =========================== recovery process ===========================
\* DurableAgentRuntime.runRecovery: scan nonterminal work, classify with the
\* pure classifier, execute exactly one idempotent durable repair step, repeat.
\* Runs only against a lane with no live lease (the deployed trigger is worker
\* loss / lease expiry); each iteration is one durable mutation, so recovery
\* crashes need no extra modeling -- re-running the loop IS recovery recovery.
fair process recovery = "recovery"
begin
RScan:
  while TRUE do
    await \E s \in Subs : RecoveryActionable(s);
    with s \in {t \in Subs : RecoveryActionable(t)} do
      if Classify(s) = "FinalizeLedgerFromHistory" then
        \* finalizeFromHistory: repair the ledger from canonical history (DUR-015)
        resv[s] := settRec[s];
        resvFinal[s] := TRUE;
        lstate[s] := "settled";
      elsif Classify(s) = "AppendReservedSettlement" then
        \* completeReservation: claimFor (rotating ownership, bumping the epoch)
        \* + append of the exact reserved record + release, folded atomic
        laneEpoch := laneEpoch + 1;
        laneOwner := "none";
        leaseLive := FALSE;
        settRec[s] := resv[s];
        settAppends[s] := CapInc(settAppends[s]);
      elsif Classify(s) = "RevertJoining" then
        \* ledger.revertJoining: the input was never canonical (DUR-016)
        lstate[s] := "ready";
        hostOf[s] := NoSub;
      elsif Classify(s) = "RepairJoinMarker" then
        \* ledger.markJoined replay: the exact record is canonical, marker lost
        lstate[s] := "joined";
      elsif Classify(s) = "SettleJoinedWithHost" then
        \* settleOneJoined: reserve with the host's canonical outcome
        resv[s] := settRec[hostOf[s]];
        lstate[s] := "terminalizing";
      elsif Classify(s) = "SettleAborted" then
        if lstate[s] = "suspended" then
          \* settleAbortedForRecovery: deny undecided approvals durably, wake lane
          apprDecided[s] := TRUE;
          lstate[s] := "input-applied";
        elsif OpenCall(s) then
          \* claimFor + ToolCallUnknown audit append (SS13: no rollback claim)
          laneEpoch := laneEpoch + 1;
          laneOwner := "none";
          leaseLive := FALSE;
          callAudited[s] := TRUE;
        else
          \* claimFor + reserveSettlement(aborted)
          laneEpoch := laneEpoch + 1;
          laneOwner := "none";
          leaseLive := FALSE;
          resv[s] := "aborted";
          lstate[s] := "terminalizing";
        end if;
      elsif Classify(s) = "MarkUnknown" then
        \* markUnknownForRecovery: claimFor, reconcile finds no proof, mark,
        \* releaseOwnership -- folded atomic
        laneEpoch := laneEpoch + 1;
        laneOwner := "none";
        leaseLive := FALSE;
        lstate[s] := "unknown";
      elsif Classify(s) = "ApplyUnknownResolutions" then
        \* applyUnknownResolutions: append ToolCallResolved(+Settled) at the tail
        \* (an unknown head is never claimable, so no live owner exists) and wake
        if OpenCall(s) /\ unknownRes[s] = "completed" then
          callSettled[s] := TRUE;
        elsif OpenCall(s) /\ unknownRes[s] = "never" then
          prepared[s] := FALSE;      \* NeverHappened: the batch may re-execute
          unknownRes[s] := "none";
        end if;
        lstate[s] := "input-applied";
      elsif Classify(s) = "ResumeSuspended" then
        \* recordApprovalDecision wake replay: suspended -> input-applied
        lstate[s] := "input-applied";
      elsif Classify(s) = "CompleteMaterialization" then
        \* store.materialize + ConversationCreated (idempotent)
        convMat := TRUE;
      else
        \* RepairReadiness: ledger.markReady replay
        lstate[s] := "ready";
      end if;
    end with;
  end while;
end process;

\* =========================== resolution dependency ===========================
\* The documented DUR-017 dependency: an authorized approver/reconciler that
\* eventually decides pending approvals and resolves Unknown Outcomes.  Its weak
\* fairness IS the liveness assumption durability SS1 conditions settlement on.
fair process resolver = "resolver"
begin
ResLoop:
  while TRUE do
    await \E s \in Subs :
      \/ (apprReq[s] /\ ~apprDecided[s] /\ lstate[s] # "settled")
      \/ (lstate[s] = "unknown" /\ unknownRes[s] = "none");
    with s \in {t \in Subs :
      \/ (apprReq[t] /\ ~apprDecided[t] /\ lstate[t] # "settled")
      \/ (lstate[t] = "unknown" /\ unknownRes[t] = "none")} do
      if lstate[s] = "unknown" /\ unknownRes[s] = "none" then
        \* recordUnknownResolution: the resolver consults the EXTERNAL truth --
        \* an effect that provably never started resolves NeverHappened; an
        \* effect that may have run resolves CompletedWithResult (DUR-017)
        if extEffect[s] = "none" then
          unknownRes[s] := "never";
        else
          unknownRes[s] := "completed";
        end if;
      else
        \* recordApprovalDecision: durable intent; wakes a suspended lane
        apprDecided[s] := TRUE;
        if lstate[s] = "suspended" then
          lstate[s] := "input-applied";
        end if;
      end if;
    end with;
  end while;
end process;

\* =========================== client process ===========================
\* DurableAgentRuntime.submit: admit -> materialize -> markReady (durability SS4).
\* A crash after admission abandons the rest; recovery completes it (DUR-001).
fair process client = "client"
variables cs = 1;
begin
CLoop:
  while cs <= MaxSub do
CAdmit:
    lstate[cs] := "admitted";     \* ledger.admit commits atomically
CMat:
    either
      await faults < MaxFaults;
      faults := faults + 1;
      goto CNext;                 \* submit crashed after admit (DUR-001 window)
    or
      convMat := TRUE;            \* materialize + ConversationCreated
    end either;
CReady:
    either
      await faults < MaxFaults;
      faults := faults + 1;
      goto CNext;                 \* crashed between materialize and markReady
    or
      \* ledger.markReady: idempotent admitted -> ready; marking an
      \* already-ready (or later-state) Submission is a no-op
      if lstate[cs] = "admitted" then
        lstate[cs] := "ready";
      end if;
    end either;
CNext:
    cs := cs + 1;
  end while;
end process;

\* =========================== abort requests ===========================
\* requestAbort (durability SS13): durable, idempotent, never after settlement,
\* never against a joined Submission (JoinedToHost).  Not fair: aborts MAY happen.
process aborter = "aborter"
begin
AbLoop:
  while TRUE do
    await \E s \in Subs :
      lstate[s] \notin {"unsubmitted", "joined", "settled"} /\ ~abortIntent[s];
    with s \in {t \in Subs :
        lstate[t] \notin {"unsubmitted", "joined", "settled"} /\ ~abortIntent[t]} do
      abortIntent[s] := TRUE;
    end with;
  end while;
end process;

\* =========================== lease expiry ===========================
\* D5: lease expiry is a liveness hint that makes an abandoned or SLOW claim
\* reclaimable; expiring a live owner's lease creates the stale-Attempt scenario
\* fencing exists for.  Not fair: expiry MAY happen, bounded by the fault budget.
process expiry = "expiry"
begin
ExpLoop:
  while TRUE do
    await leaseLive /\ faults < MaxFaults;
    leaseLive := FALSE;
    faults := faults + 1;
  end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "33db73b" /\ chksum(tla) = "cea0b264")
VARIABLES lstate, inputMarked, abortIntent, resv, resvFinal, hostOf, 
          apprDecided, unknownRes, convMat, inputRec, respKind, prepared, 
          callSettled, callAudited, apprReq, settRec, extEffect, laneEpoch, 
          laneOwner, leaseLive, faults, inputAppends, settAppends, 
          staleWrites, unknownViol, pc

(* define statement *)
Unsettled == {s \in Subs : lstate[s] \notin {"unsubmitted", "settled"}}
Head == IF Unsettled = {} THEN NoSub
        ELSE CHOOSE s \in Unsettled : \A t \in Unsettled : s <= t
OpenCall(s) == prepared[s] /\ ~callSettled[s] /\ ~callAudited[s]

Claimable ==
  /\ Head # NoSub
  /\ lstate[Head] \in {"ready", "input-applied", "terminalizing"}
  /\ (laneOwner = "none" \/ ~leaseLive)
LaneQuiet == laneOwner = "none" \/ ~leaseLive
CapInc(n) == IF n < 2 THEN n + 1 ELSE n



Classify(s) ==
  IF lstate[s] = "settled" THEN "NoAction"
  ELSE IF settRec[s] # "none" THEN "FinalizeLedgerFromHistory"
  ELSE IF resv[s] # "none" THEN
    IF resvFinal[s] THEN "FinalizeLedgerFromHistory" ELSE "AppendReservedSettlement"
  ELSE IF lstate[s] = "joining" THEN
    IF inputRec[s] THEN "RepairJoinMarker" ELSE "RevertJoining"
  ELSE IF lstate[s] = "joined" THEN
    IF hostOf[s] # NoSub /\ settRec[hostOf[s]] # "none"
      THEN "SettleJoinedWithHost" ELSE "AwaitHostSettlement"
  ELSE IF abortIntent[s] THEN "SettleAborted"
  ELSE IF lstate[s] # "unknown" /\ OpenCall(s) THEN "MarkUnknown"
  ELSE IF lstate[s] = "unknown" THEN
    IF OpenCall(s) /\ unknownRes[s] = "none"
      THEN "AwaitUnknownResolution" ELSE "ApplyUnknownResolutions"
  ELSE IF lstate[s] = "suspended" THEN
    IF apprDecided[s] THEN "ResumeSuspended" ELSE "AwaitApprovalDecision"
  ELSE IF apprReq[s] /\ ~apprDecided[s] THEN "AwaitApprovalDecision"
  ELSE IF respKind[s] = "tool" /\ ~prepared[s] /\ ~callSettled[s]
    THEN "ResumePendingToolBatch"
  ELSE IF respKind[s] = "approval" /\ ~apprReq[s] THEN "ResumePendingToolBatch"
  ELSE IF lstate[s] = "admitted" THEN
    IF convMat THEN "RepairReadiness" ELSE "CompleteMaterialization"
  ELSE IF ~inputRec[s] THEN "ApplyInput"
  ELSE IF ~inputMarked[s] THEN "RepairInputMarker"
  ELSE "ResumeFromTurnBoundary"


RecoveryStep(s) ==
  LET d == Classify(s) IN
  \/ d \in {"FinalizeLedgerFromHistory", "RevertJoining", "RepairJoinMarker",
            "SettleJoinedWithHost", "ApplyUnknownResolutions", "ResumeSuspended",
            "CompleteMaterialization", "RepairReadiness"}
  \/ (d \in {"AppendReservedSettlement", "SettleAborted", "MarkUnknown"} /\ s = Head)
RecoveryActionable(s) ==
  /\ lstate[s] \notin {"unsubmitted", "settled"}
  /\ LaneQuiet
  /\ RecoveryStep(s)

MaxSub == CHOOSE s \in Subs : \A t \in Subs : t <= s

VARIABLES sub, ep, jt, cs

vars == << lstate, inputMarked, abortIntent, resv, resvFinal, hostOf, 
           apprDecided, unknownRes, convMat, inputRec, respKind, prepared, 
           callSettled, callAudited, apprReq, settRec, extEffect, laneEpoch, 
           laneOwner, leaseLive, faults, inputAppends, settAppends, 
           staleWrites, unknownViol, pc, sub, ep, jt, cs >>

ProcSet == (Workers) \cup {"recovery"} \cup {"resolver"} \cup {"client"} \cup {"aborter"} \cup {"expiry"}

Init == (* Global variables *)
        /\ lstate = [s \in Subs |-> "unsubmitted"]
        /\ inputMarked = [s \in Subs |-> FALSE]
        /\ abortIntent = [s \in Subs |-> FALSE]
        /\ resv = [s \in Subs |-> "none"]
        /\ resvFinal = [s \in Subs |-> FALSE]
        /\ hostOf = [s \in Subs |-> NoSub]
        /\ apprDecided = [s \in Subs |-> FALSE]
        /\ unknownRes = [s \in Subs |-> "none"]
        /\ convMat = FALSE
        /\ inputRec = [s \in Subs |-> FALSE]
        /\ respKind = [s \in Subs |-> "none"]
        /\ prepared = [s \in Subs |-> FALSE]
        /\ callSettled = [s \in Subs |-> FALSE]
        /\ callAudited = [s \in Subs |-> FALSE]
        /\ apprReq = [s \in Subs |-> FALSE]
        /\ settRec = [s \in Subs |-> "none"]
        /\ extEffect = [s \in Subs |-> "none"]
        /\ laneEpoch = 0
        /\ laneOwner = "none"
        /\ leaseLive = FALSE
        /\ faults = 0
        /\ inputAppends = [s \in Subs |-> 0]
        /\ settAppends = [s \in Subs |-> 0]
        /\ staleWrites = 0
        /\ unknownViol = 0
        (* Process worker *)
        /\ sub = [self \in Workers |-> NoSub]
        /\ ep = [self \in Workers |-> 0]
        /\ jt = [self \in Workers |-> NoSub]
        (* Process client *)
        /\ cs = 1
        /\ pc = [self \in ProcSet |-> CASE self \in Workers -> "WIdle"
                                        [] self = "recovery" -> "RScan"
                                        [] self = "resolver" -> "ResLoop"
                                        [] self = "client" -> "CLoop"
                                        [] self = "aborter" -> "AbLoop"
                                        [] self = "expiry" -> "ExpLoop"]

WIdle(self) == /\ pc[self] = "WIdle"
               /\ Claimable
               /\ laneEpoch' = laneEpoch + 1
               /\ laneOwner' = self
               /\ leaseLive' = TRUE
               /\ ep' = [ep EXCEPT ![self] = laneEpoch']
               /\ sub' = [sub EXCEPT ![self] = Head]
               /\ pc' = [pc EXCEPT ![self] = "WResume"]
               /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                               resvFinal, hostOf, apprDecided, unknownRes, 
                               convMat, inputRec, respKind, prepared, 
                               callSettled, callAudited, apprReq, settRec, 
                               extEffect, faults, inputAppends, settAppends, 
                               staleWrites, unknownViol, jt, cs >>

WResume(self) == /\ pc[self] = "WResume"
                 /\ \/ /\ faults < MaxFaults
                       /\ faults' = faults + 1
                       /\ IF laneOwner = self
                             THEN /\ /\ laneOwner' = "none"
                                     /\ leaseLive' = FALSE
                             ELSE /\ TRUE
                                  /\ UNCHANGED << laneOwner, leaseLive >>
                       /\ sub' = [sub EXCEPT ![self] = NoSub]
                       /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                    \/ /\ IF sub[self] = NoSub \/ lstate[sub[self]] = "settled"
                             THEN /\ IF laneOwner = self
                                        THEN /\ /\ laneOwner' = "none"
                                                /\ leaseLive' = FALSE
                                        ELSE /\ TRUE
                                             /\ UNCHANGED << laneOwner, 
                                                             leaseLive >>
                                  /\ sub' = [sub EXCEPT ![self] = NoSub]
                                  /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                             ELSE /\ IF settRec[sub[self]] # "none"
                                        THEN /\ pc' = [pc EXCEPT ![self] = "WFin"]
                                        ELSE /\ IF resv[sub[self]] # "none"
                                                   THEN /\ pc' = [pc EXCEPT ![self] = "WSettApp"]
                                                   ELSE /\ IF abortIntent[sub[self]]
                                                              THEN /\ pc' = [pc EXCEPT ![self] = "WAbortAudit"]
                                                              ELSE /\ IF ~inputRec[sub[self]]
                                                                         THEN /\ pc' = [pc EXCEPT ![self] = "WInput"]
                                                                         ELSE /\ IF ~inputMarked[sub[self]]
                                                                                    THEN /\ pc' = [pc EXCEPT ![self] = "WMarkInput"]
                                                                                    ELSE /\ IF respKind[sub[self]] = "none"
                                                                                               THEN /\ pc' = [pc EXCEPT ![self] = "WTurn"]
                                                                                               ELSE /\ IF OpenCall(sub[self])
                                                                                                          THEN /\ pc' = [pc EXCEPT ![self] = "WMarkUnknown"]
                                                                                                          ELSE /\ IF respKind[sub[self]] = "tool"
                                                                                                                     THEN /\ IF callSettled[sub[self]]
                                                                                                                                THEN /\ pc' = [pc EXCEPT ![self] = "WReserve"]
                                                                                                                                ELSE /\ pc' = [pc EXCEPT ![self] = "WPrepare"]
                                                                                                                     ELSE /\ IF respKind[sub[self]] = "approval"
                                                                                                                                THEN /\ IF ~apprReq[sub[self]]
                                                                                                                                           THEN /\ pc' = [pc EXCEPT ![self] = "WApprReq"]
                                                                                                                                           ELSE /\ IF ~apprDecided[sub[self]]
                                                                                                                                                      THEN /\ pc' = [pc EXCEPT ![self] = "WSuspend"]
                                                                                                                                                      ELSE /\ IF callSettled[sub[self]]
                                                                                                                                                                 THEN /\ pc' = [pc EXCEPT ![self] = "WReserve"]
                                                                                                                                                                 ELSE /\ pc' = [pc EXCEPT ![self] = "WPrepare"]
                                                                                                                                ELSE /\ pc' = [pc EXCEPT ![self] = "WReserve"]
                                  /\ UNCHANGED << laneOwner, leaseLive, sub >>
                       /\ UNCHANGED faults
                 /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                                 resvFinal, hostOf, apprDecided, unknownRes, 
                                 convMat, inputRec, respKind, prepared, 
                                 callSettled, callAudited, apprReq, settRec, 
                                 extEffect, laneEpoch, inputAppends, 
                                 settAppends, staleWrites, unknownViol, ep, jt, 
                                 cs >>

WInput(self) == /\ pc[self] = "WInput"
                /\ \/ /\ faults < MaxFaults
                      /\ faults' = faults + 1
                      /\ IF laneOwner = self
                            THEN /\ /\ laneOwner' = "none"
                                    /\ leaseLive' = FALSE
                            ELSE /\ TRUE
                                 /\ UNCHANGED << laneOwner, leaseLive >>
                      /\ sub' = [sub EXCEPT ![self] = NoSub]
                      /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                      /\ UNCHANGED <<inputRec, inputAppends, staleWrites, unknownViol>>
                   \/ /\ IF FencingEnabled /\ ep[self] # laneEpoch
                            THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                 /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                 /\ UNCHANGED << inputRec, inputAppends, 
                                                 staleWrites, unknownViol >>
                            ELSE /\ IF ep[self] # laneEpoch
                                       THEN /\ staleWrites' = 1
                                       ELSE /\ TRUE
                                            /\ UNCHANGED staleWrites
                                 /\ IF lstate[sub[self]] = "unknown"
                                       THEN /\ unknownViol' = 1
                                       ELSE /\ TRUE
                                            /\ UNCHANGED unknownViol
                                 /\ IF ~inputRec[sub[self]]
                                       THEN /\ inputRec' = [inputRec EXCEPT ![sub[self]] = TRUE]
                                            /\ inputAppends' = [inputAppends EXCEPT ![sub[self]] = CapInc(inputAppends[sub[self]])]
                                       ELSE /\ TRUE
                                            /\ UNCHANGED << inputRec, 
                                                            inputAppends >>
                                 /\ pc' = [pc EXCEPT ![self] = "WMarkInput"]
                                 /\ sub' = sub
                      /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                                resvFinal, hostOf, apprDecided, unknownRes, 
                                convMat, respKind, prepared, callSettled, 
                                callAudited, apprReq, settRec, extEffect, 
                                laneEpoch, settAppends, ep, jt, cs >>

WMarkInput(self) == /\ pc[self] = "WMarkInput"
                    /\ \/ /\ faults < MaxFaults
                          /\ faults' = faults + 1
                          /\ IF laneOwner = self
                                THEN /\ /\ laneOwner' = "none"
                                        /\ leaseLive' = FALSE
                                ELSE /\ TRUE
                                     /\ UNCHANGED << laneOwner, leaseLive >>
                          /\ sub' = [sub EXCEPT ![self] = NoSub]
                          /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                          /\ UNCHANGED <<lstate, inputMarked, staleWrites>>
                       \/ /\ IF FencingEnabled /\ laneOwner # self
                                THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                     /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                     /\ UNCHANGED << lstate, inputMarked, 
                                                     staleWrites >>
                                ELSE /\ IF laneOwner # self
                                           THEN /\ staleWrites' = 1
                                           ELSE /\ TRUE
                                                /\ UNCHANGED staleWrites
                                     /\ lstate' = [lstate EXCEPT ![sub[self]] = "input-applied"]
                                     /\ inputMarked' = [inputMarked EXCEPT ![sub[self]] = TRUE]
                                     /\ pc' = [pc EXCEPT ![self] = "WTurn"]
                                     /\ sub' = sub
                          /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                    /\ UNCHANGED << abortIntent, resv, resvFinal, hostOf, 
                                    apprDecided, unknownRes, convMat, inputRec, 
                                    respKind, prepared, callSettled, 
                                    callAudited, apprReq, settRec, extEffect, 
                                    laneEpoch, inputAppends, settAppends, 
                                    unknownViol, ep, jt, cs >>

WTurn(self) == /\ pc[self] = "WTurn"
               /\ \/ /\ faults < MaxFaults
                     /\ faults' = faults + 1
                     /\ IF laneOwner = self
                           THEN /\ /\ laneOwner' = "none"
                                   /\ leaseLive' = FALSE
                           ELSE /\ TRUE
                                /\ UNCHANGED << laneOwner, leaseLive >>
                     /\ sub' = [sub EXCEPT ![self] = NoSub]
                     /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                  \/ /\ IF abortIntent[sub[self]]
                           THEN /\ pc' = [pc EXCEPT ![self] = "WAbortAudit"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "JoinOrRespond"]
                     /\ UNCHANGED <<laneOwner, leaseLive, faults, sub>>
               /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                               resvFinal, hostOf, apprDecided, unknownRes, 
                               convMat, inputRec, respKind, prepared, 
                               callSettled, callAudited, apprReq, settRec, 
                               extEffect, laneEpoch, inputAppends, settAppends, 
                               staleWrites, unknownViol, ep, jt, cs >>

JoinOrRespond(self) == /\ pc[self] = "JoinOrRespond"
                       /\ \/ /\ sub[self] < MaxSub /\ lstate[sub[self] + 1] = "ready" /\ ~abortIntent[sub[self] + 1]
                             /\ IF FencingEnabled /\ laneOwner # self
                                   THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                        /\ UNCHANGED << lstate, hostOf, 
                                                        staleWrites, jt >>
                                   ELSE /\ IF laneOwner # self
                                              THEN /\ staleWrites' = 1
                                              ELSE /\ TRUE
                                                   /\ UNCHANGED staleWrites
                                        /\ jt' = [jt EXCEPT ![self] = sub[self] + 1]
                                        /\ lstate' = [lstate EXCEPT ![sub[self] + 1] = "joining"]
                                        /\ hostOf' = [hostOf EXCEPT ![sub[self] + 1] = sub[self]]
                                        /\ pc' = [pc EXCEPT ![self] = "WJoinDeliver"]
                                        /\ sub' = sub
                             /\ UNCHANGED <<respKind, unknownViol>>
                          \/ /\ \E kind \in {"finish", "tool", "approval"}:
                                  IF FencingEnabled /\ ep[self] # laneEpoch
                                     THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                          /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                          /\ UNCHANGED << respKind, 
                                                          staleWrites, 
                                                          unknownViol >>
                                     ELSE /\ IF ep[self] # laneEpoch
                                                THEN /\ staleWrites' = 1
                                                ELSE /\ TRUE
                                                     /\ UNCHANGED staleWrites
                                          /\ IF lstate[sub[self]] = "unknown"
                                                THEN /\ unknownViol' = 1
                                                ELSE /\ TRUE
                                                     /\ UNCHANGED unknownViol
                                          /\ respKind' = [respKind EXCEPT ![sub[self]] = kind]
                                          /\ IF kind = "finish"
                                                THEN /\ pc' = [pc EXCEPT ![self] = "WReserve"]
                                                ELSE /\ IF kind = "tool"
                                                           THEN /\ pc' = [pc EXCEPT ![self] = "WPrepare"]
                                                           ELSE /\ pc' = [pc EXCEPT ![self] = "WApprReq"]
                                          /\ sub' = sub
                             /\ UNCHANGED <<lstate, hostOf, jt>>
                       /\ UNCHANGED << inputMarked, abortIntent, resv, 
                                       resvFinal, apprDecided, unknownRes, 
                                       convMat, inputRec, prepared, 
                                       callSettled, callAudited, apprReq, 
                                       settRec, extEffect, laneEpoch, 
                                       laneOwner, leaseLive, faults, 
                                       inputAppends, settAppends, ep, cs >>

WJoinDeliver(self) == /\ pc[self] = "WJoinDeliver"
                      /\ \/ /\ faults < MaxFaults
                            /\ faults' = faults + 1
                            /\ IF laneOwner = self
                                  THEN /\ /\ laneOwner' = "none"
                                          /\ leaseLive' = FALSE
                                  ELSE /\ TRUE
                                       /\ UNCHANGED << laneOwner, leaseLive >>
                            /\ sub' = [sub EXCEPT ![self] = NoSub]
                            /\ jt' = [jt EXCEPT ![self] = NoSub]
                            /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                            /\ UNCHANGED <<lstate, hostOf, inputRec, inputAppends, staleWrites>>
                         \/ /\ IF abortIntent[jt[self]]
                                  THEN /\ lstate' = [lstate EXCEPT ![jt[self]] = "ready"]
                                       /\ hostOf' = [hostOf EXCEPT ![jt[self]] = NoSub]
                                       /\ jt' = [jt EXCEPT ![self] = NoSub]
                                       /\ pc' = [pc EXCEPT ![self] = "WTurn"]
                                       /\ UNCHANGED << inputRec, inputAppends, 
                                                       staleWrites, sub >>
                                  ELSE /\ IF FencingEnabled /\ ep[self] # laneEpoch
                                             THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                                  /\ jt' = [jt EXCEPT ![self] = NoSub]
                                                  /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                                  /\ UNCHANGED << inputRec, 
                                                                  inputAppends, 
                                                                  staleWrites >>
                                             ELSE /\ IF ep[self] # laneEpoch
                                                        THEN /\ staleWrites' = 1
                                                        ELSE /\ TRUE
                                                             /\ UNCHANGED staleWrites
                                                  /\ IF ~inputRec[jt[self]]
                                                        THEN /\ inputRec' = [inputRec EXCEPT ![jt[self]] = TRUE]
                                                             /\ inputAppends' = [inputAppends EXCEPT ![jt[self]] = CapInc(inputAppends[jt[self]])]
                                                        ELSE /\ TRUE
                                                             /\ UNCHANGED << inputRec, 
                                                                             inputAppends >>
                                                  /\ pc' = [pc EXCEPT ![self] = "WJoinMark"]
                                                  /\ UNCHANGED << sub, jt >>
                                       /\ UNCHANGED << lstate, hostOf >>
                            /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                      /\ UNCHANGED << inputMarked, abortIntent, resv, 
                                      resvFinal, apprDecided, unknownRes, 
                                      convMat, respKind, prepared, callSettled, 
                                      callAudited, apprReq, settRec, extEffect, 
                                      laneEpoch, settAppends, unknownViol, ep, 
                                      cs >>

WJoinMark(self) == /\ pc[self] = "WJoinMark"
                   /\ \/ /\ faults < MaxFaults
                         /\ faults' = faults + 1
                         /\ IF laneOwner = self
                               THEN /\ /\ laneOwner' = "none"
                                       /\ leaseLive' = FALSE
                               ELSE /\ TRUE
                                    /\ UNCHANGED << laneOwner, leaseLive >>
                         /\ sub' = [sub EXCEPT ![self] = NoSub]
                         /\ jt' = [jt EXCEPT ![self] = NoSub]
                         /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                         /\ UNCHANGED <<lstate, staleWrites>>
                      \/ /\ IF FencingEnabled /\ laneOwner # self
                               THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                    /\ jt' = [jt EXCEPT ![self] = NoSub]
                                    /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                    /\ UNCHANGED << lstate, staleWrites >>
                               ELSE /\ IF laneOwner # self
                                          THEN /\ staleWrites' = 1
                                          ELSE /\ TRUE
                                               /\ UNCHANGED staleWrites
                                    /\ IF lstate[jt[self]] = "joining" /\ hostOf[jt[self]] # NoSub
                                          THEN /\ lstate' = [lstate EXCEPT ![jt[self]] = "joined"]
                                          ELSE /\ TRUE
                                               /\ UNCHANGED lstate
                                    /\ jt' = [jt EXCEPT ![self] = NoSub]
                                    /\ pc' = [pc EXCEPT ![self] = "WTurn"]
                                    /\ sub' = sub
                         /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                   /\ UNCHANGED << inputMarked, abortIntent, resv, resvFinal, 
                                   hostOf, apprDecided, unknownRes, convMat, 
                                   inputRec, respKind, prepared, callSettled, 
                                   callAudited, apprReq, settRec, extEffect, 
                                   laneEpoch, inputAppends, settAppends, 
                                   unknownViol, ep, cs >>

WPrepare(self) == /\ pc[self] = "WPrepare"
                  /\ \/ /\ faults < MaxFaults
                        /\ faults' = faults + 1
                        /\ IF laneOwner = self
                              THEN /\ /\ laneOwner' = "none"
                                      /\ leaseLive' = FALSE
                              ELSE /\ TRUE
                                   /\ UNCHANGED << laneOwner, leaseLive >>
                        /\ sub' = [sub EXCEPT ![self] = NoSub]
                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                        /\ UNCHANGED <<prepared, staleWrites, unknownViol>>
                     \/ /\ IF FencingEnabled /\ ep[self] # laneEpoch
                              THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                   /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                   /\ UNCHANGED << prepared, staleWrites, 
                                                   unknownViol >>
                              ELSE /\ IF ep[self] # laneEpoch
                                         THEN /\ staleWrites' = 1
                                         ELSE /\ TRUE
                                              /\ UNCHANGED staleWrites
                                   /\ IF lstate[sub[self]] = "unknown"
                                         THEN /\ unknownViol' = 1
                                         ELSE /\ TRUE
                                              /\ UNCHANGED unknownViol
                                   /\ prepared' = [prepared EXCEPT ![sub[self]] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = "WExec"]
                                   /\ sub' = sub
                        /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                  /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                                  resvFinal, hostOf, apprDecided, unknownRes, 
                                  convMat, inputRec, respKind, callSettled, 
                                  callAudited, apprReq, settRec, extEffect, 
                                  laneEpoch, inputAppends, settAppends, ep, jt, 
                                  cs >>

WExec(self) == /\ pc[self] = "WExec"
               /\ \/ /\ faults < MaxFaults
                     /\ faults' = faults + 1
                     /\ IF laneOwner = self
                           THEN /\ /\ laneOwner' = "none"
                                   /\ leaseLive' = FALSE
                           ELSE /\ TRUE
                                /\ UNCHANGED << laneOwner, leaseLive >>
                     /\ sub' = [sub EXCEPT ![self] = NoSub]
                     /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                     /\ UNCHANGED extEffect
                  \/ /\ extEffect' = [extEffect EXCEPT ![sub[self]] = "maybe"]
                     /\ pc' = [pc EXCEPT ![self] = "WSettleCall"]
                     /\ UNCHANGED <<laneOwner, leaseLive, faults, sub>>
               /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                               resvFinal, hostOf, apprDecided, unknownRes, 
                               convMat, inputRec, respKind, prepared, 
                               callSettled, callAudited, apprReq, settRec, 
                               laneEpoch, inputAppends, settAppends, 
                               staleWrites, unknownViol, ep, jt, cs >>

WSettleCall(self) == /\ pc[self] = "WSettleCall"
                     /\ \/ /\ faults < MaxFaults
                           /\ faults' = faults + 1
                           /\ IF laneOwner = self
                                 THEN /\ /\ laneOwner' = "none"
                                         /\ leaseLive' = FALSE
                                 ELSE /\ TRUE
                                      /\ UNCHANGED << laneOwner, leaseLive >>
                           /\ sub' = [sub EXCEPT ![self] = NoSub]
                           /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                           /\ UNCHANGED <<callSettled, extEffect, staleWrites, unknownViol>>
                        \/ /\ IF FencingEnabled /\ ep[self] # laneEpoch
                                 THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                      /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                      /\ UNCHANGED << callSettled, extEffect, 
                                                      staleWrites, unknownViol >>
                                 ELSE /\ IF ep[self] # laneEpoch
                                            THEN /\ staleWrites' = 1
                                            ELSE /\ TRUE
                                                 /\ UNCHANGED staleWrites
                                      /\ IF lstate[sub[self]] = "unknown"
                                            THEN /\ unknownViol' = 1
                                            ELSE /\ TRUE
                                                 /\ UNCHANGED unknownViol
                                      /\ callSettled' = [callSettled EXCEPT ![sub[self]] = TRUE]
                                      /\ extEffect' = [extEffect EXCEPT ![sub[self]] = "done"]
                                      /\ pc' = [pc EXCEPT ![self] = "WReserve"]
                                      /\ sub' = sub
                           /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                     /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                                     resvFinal, hostOf, apprDecided, 
                                     unknownRes, convMat, inputRec, respKind, 
                                     prepared, callAudited, apprReq, settRec, 
                                     laneEpoch, inputAppends, settAppends, ep, 
                                     jt, cs >>

WApprReq(self) == /\ pc[self] = "WApprReq"
                  /\ \/ /\ faults < MaxFaults
                        /\ faults' = faults + 1
                        /\ IF laneOwner = self
                              THEN /\ /\ laneOwner' = "none"
                                      /\ leaseLive' = FALSE
                              ELSE /\ TRUE
                                   /\ UNCHANGED << laneOwner, leaseLive >>
                        /\ sub' = [sub EXCEPT ![self] = NoSub]
                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                        /\ UNCHANGED <<apprReq, staleWrites, unknownViol>>
                     \/ /\ IF FencingEnabled /\ ep[self] # laneEpoch
                              THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                   /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                   /\ UNCHANGED << apprReq, staleWrites, 
                                                   unknownViol >>
                              ELSE /\ IF ep[self] # laneEpoch
                                         THEN /\ staleWrites' = 1
                                         ELSE /\ TRUE
                                              /\ UNCHANGED staleWrites
                                   /\ IF lstate[sub[self]] = "unknown"
                                         THEN /\ unknownViol' = 1
                                         ELSE /\ TRUE
                                              /\ UNCHANGED unknownViol
                                   /\ apprReq' = [apprReq EXCEPT ![sub[self]] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = "WSuspend"]
                                   /\ sub' = sub
                        /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                  /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                                  resvFinal, hostOf, apprDecided, unknownRes, 
                                  convMat, inputRec, respKind, prepared, 
                                  callSettled, callAudited, settRec, extEffect, 
                                  laneEpoch, inputAppends, settAppends, ep, jt, 
                                  cs >>

WSuspend(self) == /\ pc[self] = "WSuspend"
                  /\ \/ /\ faults < MaxFaults
                        /\ faults' = faults + 1
                        /\ IF laneOwner = self
                              THEN /\ /\ laneOwner' = "none"
                                      /\ leaseLive' = FALSE
                              ELSE /\ TRUE
                                   /\ UNCHANGED << laneOwner, leaseLive >>
                        /\ sub' = [sub EXCEPT ![self] = NoSub]
                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                        /\ UNCHANGED <<lstate, staleWrites>>
                     \/ /\ IF FencingEnabled /\ laneOwner # self
                              THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                   /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                   /\ UNCHANGED << lstate, laneOwner, 
                                                   leaseLive, staleWrites >>
                              ELSE /\ IF laneOwner # self
                                         THEN /\ staleWrites' = 1
                                         ELSE /\ TRUE
                                              /\ UNCHANGED staleWrites
                                   /\ IF apprDecided[sub[self]]
                                         THEN /\ pc' = [pc EXCEPT ![self] = "WPrepare"]
                                              /\ UNCHANGED << lstate, 
                                                              laneOwner, 
                                                              leaseLive, sub >>
                                         ELSE /\ lstate' = [lstate EXCEPT ![sub[self]] = "suspended"]
                                              /\ laneOwner' = "none"
                                              /\ leaseLive' = FALSE
                                              /\ sub' = [sub EXCEPT ![self] = NoSub]
                                              /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                        /\ UNCHANGED faults
                  /\ UNCHANGED << inputMarked, abortIntent, resv, resvFinal, 
                                  hostOf, apprDecided, unknownRes, convMat, 
                                  inputRec, respKind, prepared, callSettled, 
                                  callAudited, apprReq, settRec, extEffect, 
                                  laneEpoch, inputAppends, settAppends, 
                                  unknownViol, ep, jt, cs >>

WMarkUnknown(self) == /\ pc[self] = "WMarkUnknown"
                      /\ \/ /\ faults < MaxFaults
                            /\ faults' = faults + 1
                            /\ IF laneOwner = self
                                  THEN /\ /\ laneOwner' = "none"
                                          /\ leaseLive' = FALSE
                                  ELSE /\ TRUE
                                       /\ UNCHANGED << laneOwner, leaseLive >>
                            /\ sub' = [sub EXCEPT ![self] = NoSub]
                            /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                            /\ UNCHANGED <<lstate, staleWrites>>
                         \/ /\ IF FencingEnabled /\ laneOwner # self
                                  THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                       /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                       /\ UNCHANGED << lstate, laneOwner, 
                                                       leaseLive, staleWrites >>
                                  ELSE /\ IF laneOwner # self
                                             THEN /\ staleWrites' = 1
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED staleWrites
                                       /\ lstate' = [lstate EXCEPT ![sub[self]] = "unknown"]
                                       /\ laneOwner' = "none"
                                       /\ leaseLive' = FALSE
                                       /\ sub' = [sub EXCEPT ![self] = NoSub]
                                       /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                            /\ UNCHANGED faults
                      /\ UNCHANGED << inputMarked, abortIntent, resv, 
                                      resvFinal, hostOf, apprDecided, 
                                      unknownRes, convMat, inputRec, respKind, 
                                      prepared, callSettled, callAudited, 
                                      apprReq, settRec, extEffect, laneEpoch, 
                                      inputAppends, settAppends, unknownViol, 
                                      ep, jt, cs >>

WAbortAudit(self) == /\ pc[self] = "WAbortAudit"
                     /\ \/ /\ faults < MaxFaults
                           /\ faults' = faults + 1
                           /\ IF laneOwner = self
                                 THEN /\ /\ laneOwner' = "none"
                                         /\ leaseLive' = FALSE
                                 ELSE /\ TRUE
                                      /\ UNCHANGED << laneOwner, leaseLive >>
                           /\ sub' = [sub EXCEPT ![self] = NoSub]
                           /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                           /\ UNCHANGED <<callAudited, staleWrites>>
                        \/ /\ IF OpenCall(sub[self])
                                 THEN /\ IF FencingEnabled /\ ep[self] # laneEpoch
                                            THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                                 /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                                 /\ UNCHANGED << callAudited, 
                                                                 staleWrites >>
                                            ELSE /\ IF ep[self] # laneEpoch
                                                       THEN /\ staleWrites' = 1
                                                       ELSE /\ TRUE
                                                            /\ UNCHANGED staleWrites
                                                 /\ callAudited' = [callAudited EXCEPT ![sub[self]] = TRUE]
                                                 /\ pc' = [pc EXCEPT ![self] = "WAbortReserve"]
                                                 /\ sub' = sub
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "WAbortReserve"]
                                      /\ UNCHANGED << callAudited, staleWrites, 
                                                      sub >>
                           /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                     /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                                     resvFinal, hostOf, apprDecided, 
                                     unknownRes, convMat, inputRec, respKind, 
                                     prepared, callSettled, apprReq, settRec, 
                                     extEffect, laneEpoch, inputAppends, 
                                     settAppends, unknownViol, ep, jt, cs >>

WAbortReserve(self) == /\ pc[self] = "WAbortReserve"
                       /\ \/ /\ faults < MaxFaults
                             /\ faults' = faults + 1
                             /\ IF laneOwner = self
                                   THEN /\ /\ laneOwner' = "none"
                                           /\ leaseLive' = FALSE
                                   ELSE /\ TRUE
                                        /\ UNCHANGED << laneOwner, leaseLive >>
                             /\ sub' = [sub EXCEPT ![self] = NoSub]
                             /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                             /\ UNCHANGED <<lstate, resv, staleWrites>>
                          \/ /\ IF FencingEnabled /\ laneOwner # self
                                   THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                        /\ UNCHANGED << lstate, resv, 
                                                        staleWrites >>
                                   ELSE /\ IF laneOwner # self
                                              THEN /\ staleWrites' = 1
                                              ELSE /\ TRUE
                                                   /\ UNCHANGED staleWrites
                                        /\ IF resv[sub[self]] = "none"
                                              THEN /\ resv' = [resv EXCEPT ![sub[self]] = "aborted"]
                                                   /\ lstate' = [lstate EXCEPT ![sub[self]] = "terminalizing"]
                                              ELSE /\ TRUE
                                                   /\ UNCHANGED << lstate, 
                                                                   resv >>
                                        /\ pc' = [pc EXCEPT ![self] = "WSettApp"]
                                        /\ sub' = sub
                             /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                       /\ UNCHANGED << inputMarked, abortIntent, resvFinal, 
                                       hostOf, apprDecided, unknownRes, 
                                       convMat, inputRec, respKind, prepared, 
                                       callSettled, callAudited, apprReq, 
                                       settRec, extEffect, laneEpoch, 
                                       inputAppends, settAppends, unknownViol, 
                                       ep, jt, cs >>

WReserve(self) == /\ pc[self] = "WReserve"
                  /\ \/ /\ faults < MaxFaults
                        /\ faults' = faults + 1
                        /\ IF laneOwner = self
                              THEN /\ /\ laneOwner' = "none"
                                      /\ leaseLive' = FALSE
                              ELSE /\ TRUE
                                   /\ UNCHANGED << laneOwner, leaseLive >>
                        /\ sub' = [sub EXCEPT ![self] = NoSub]
                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                        /\ UNCHANGED <<lstate, resv, staleWrites>>
                     \/ /\ IF FencingEnabled /\ laneOwner # self
                              THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                   /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                   /\ UNCHANGED << lstate, resv, staleWrites >>
                              ELSE /\ IF laneOwner # self
                                         THEN /\ staleWrites' = 1
                                         ELSE /\ TRUE
                                              /\ UNCHANGED staleWrites
                                   /\ IF resv[sub[self]] = "none"
                                         THEN /\ resv' = [resv EXCEPT ![sub[self]] = "completed"]
                                              /\ lstate' = [lstate EXCEPT ![sub[self]] = "terminalizing"]
                                         ELSE /\ TRUE
                                              /\ UNCHANGED << lstate, resv >>
                                   /\ pc' = [pc EXCEPT ![self] = "WSettApp"]
                                   /\ sub' = sub
                        /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                  /\ UNCHANGED << inputMarked, abortIntent, resvFinal, hostOf, 
                                  apprDecided, unknownRes, convMat, inputRec, 
                                  respKind, prepared, callSettled, callAudited, 
                                  apprReq, settRec, extEffect, laneEpoch, 
                                  inputAppends, settAppends, unknownViol, ep, 
                                  jt, cs >>

WSettApp(self) == /\ pc[self] = "WSettApp"
                  /\ \/ /\ faults < MaxFaults
                        /\ faults' = faults + 1
                        /\ IF laneOwner = self
                              THEN /\ /\ laneOwner' = "none"
                                      /\ leaseLive' = FALSE
                              ELSE /\ TRUE
                                   /\ UNCHANGED << laneOwner, leaseLive >>
                        /\ sub' = [sub EXCEPT ![self] = NoSub]
                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                        /\ UNCHANGED <<settRec, settAppends, staleWrites>>
                     \/ /\ IF FencingEnabled /\ ep[self] # laneEpoch
                              THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                   /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                   /\ UNCHANGED << settRec, settAppends, 
                                                   staleWrites >>
                              ELSE /\ IF ep[self] # laneEpoch
                                         THEN /\ staleWrites' = 1
                                         ELSE /\ TRUE
                                              /\ UNCHANGED staleWrites
                                   /\ IF settRec[sub[self]] = "none"
                                         THEN /\ settRec' = [settRec EXCEPT ![sub[self]] = resv[sub[self]]]
                                              /\ settAppends' = [settAppends EXCEPT ![sub[self]] = CapInc(settAppends[sub[self]])]
                                         ELSE /\ TRUE
                                              /\ UNCHANGED << settRec, 
                                                              settAppends >>
                                   /\ pc' = [pc EXCEPT ![self] = "WFin"]
                                   /\ sub' = sub
                        /\ UNCHANGED <<laneOwner, leaseLive, faults>>
                  /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, 
                                  resvFinal, hostOf, apprDecided, unknownRes, 
                                  convMat, inputRec, respKind, prepared, 
                                  callSettled, callAudited, apprReq, extEffect, 
                                  laneEpoch, inputAppends, unknownViol, ep, jt, 
                                  cs >>

WFin(self) == /\ pc[self] = "WFin"
              /\ \/ /\ faults < MaxFaults
                    /\ faults' = faults + 1
                    /\ IF laneOwner = self
                          THEN /\ /\ laneOwner' = "none"
                                  /\ leaseLive' = FALSE
                          ELSE /\ TRUE
                               /\ UNCHANGED << laneOwner, leaseLive >>
                    /\ sub' = [sub EXCEPT ![self] = NoSub]
                    /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                    /\ UNCHANGED <<lstate, resvFinal>>
                 \/ /\ resvFinal' = [resvFinal EXCEPT ![sub[self]] = TRUE]
                    /\ lstate' = [lstate EXCEPT ![sub[self]] = "settled"]
                    /\ IF laneOwner = self
                          THEN /\ /\ laneOwner' = "none"
                                  /\ leaseLive' = FALSE
                          ELSE /\ TRUE
                               /\ UNCHANGED << laneOwner, leaseLive >>
                    /\ pc' = [pc EXCEPT ![self] = "WJoinedSettle"]
                    /\ UNCHANGED <<faults, sub>>
              /\ UNCHANGED << inputMarked, abortIntent, resv, hostOf, 
                              apprDecided, unknownRes, convMat, inputRec, 
                              respKind, prepared, callSettled, callAudited, 
                              apprReq, settRec, extEffect, laneEpoch, 
                              inputAppends, settAppends, staleWrites, 
                              unknownViol, ep, jt, cs >>

WJoinedSettle(self) == /\ pc[self] = "WJoinedSettle"
                       /\ \/ /\ faults < MaxFaults
                             /\ faults' = faults + 1
                             /\ sub' = [sub EXCEPT ![self] = NoSub]
                             /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                             /\ UNCHANGED <<lstate, resv>>
                          \/ /\ IF \E t \in Subs : hostOf[t] = sub[self] /\ lstate[t] = "joined"
                                   THEN /\ \E t \in {t \in Subs : hostOf[t] = sub[self] /\ lstate[t] = "joined"}:
                                             /\ resv' = [resv EXCEPT ![t] = settRec[sub[self]]]
                                             /\ lstate' = [lstate EXCEPT ![t] = "terminalizing"]
                                        /\ pc' = [pc EXCEPT ![self] = "WJoinedApp"]
                                        /\ sub' = sub
                                   ELSE /\ sub' = [sub EXCEPT ![self] = NoSub]
                                        /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                        /\ UNCHANGED << lstate, resv >>
                             /\ UNCHANGED faults
                       /\ UNCHANGED << inputMarked, abortIntent, resvFinal, 
                                       hostOf, apprDecided, unknownRes, 
                                       convMat, inputRec, respKind, prepared, 
                                       callSettled, callAudited, apprReq, 
                                       settRec, extEffect, laneEpoch, 
                                       laneOwner, leaseLive, inputAppends, 
                                       settAppends, staleWrites, unknownViol, 
                                       ep, jt, cs >>

WJoinedApp(self) == /\ pc[self] = "WJoinedApp"
                    /\ \/ /\ faults < MaxFaults
                          /\ faults' = faults + 1
                          /\ sub' = [sub EXCEPT ![self] = NoSub]
                          /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                          /\ UNCHANGED <<lstate, resvFinal, settRec, settAppends, staleWrites>>
                       \/ /\ IF \E t \in Subs : hostOf[t] = sub[self] /\ lstate[t] = "terminalizing"
                                THEN /\ \E t \in {t \in Subs : hostOf[t] = sub[self] /\ lstate[t] = "terminalizing"}:
                                          IF FencingEnabled /\ ep[self] # laneEpoch
                                             THEN /\ sub' = [sub EXCEPT ![self] = NoSub]
                                                  /\ pc' = [pc EXCEPT ![self] = "WIdle"]
                                                  /\ UNCHANGED << lstate, 
                                                                  resvFinal, 
                                                                  settRec, 
                                                                  settAppends, 
                                                                  staleWrites >>
                                             ELSE /\ IF ep[self] # laneEpoch
                                                        THEN /\ staleWrites' = 1
                                                        ELSE /\ TRUE
                                                             /\ UNCHANGED staleWrites
                                                  /\ IF settRec[t] = "none"
                                                        THEN /\ settRec' = [settRec EXCEPT ![t] = resv[t]]
                                                             /\ settAppends' = [settAppends EXCEPT ![t] = CapInc(settAppends[t])]
                                                        ELSE /\ TRUE
                                                             /\ UNCHANGED << settRec, 
                                                                             settAppends >>
                                                  /\ resvFinal' = [resvFinal EXCEPT ![t] = TRUE]
                                                  /\ lstate' = [lstate EXCEPT ![t] = "settled"]
                                                  /\ pc' = [pc EXCEPT ![self] = "WJoinedSettle"]
                                                  /\ sub' = sub
                                ELSE /\ pc' = [pc EXCEPT ![self] = "WJoinedSettle"]
                                     /\ UNCHANGED << lstate, resvFinal, 
                                                     settRec, settAppends, 
                                                     staleWrites, sub >>
                          /\ UNCHANGED faults
                    /\ UNCHANGED << inputMarked, abortIntent, resv, hostOf, 
                                    apprDecided, unknownRes, convMat, inputRec, 
                                    respKind, prepared, callSettled, 
                                    callAudited, apprReq, extEffect, laneEpoch, 
                                    laneOwner, leaseLive, inputAppends, 
                                    unknownViol, ep, jt, cs >>

worker(self) == WIdle(self) \/ WResume(self) \/ WInput(self)
                   \/ WMarkInput(self) \/ WTurn(self)
                   \/ JoinOrRespond(self) \/ WJoinDeliver(self)
                   \/ WJoinMark(self) \/ WPrepare(self) \/ WExec(self)
                   \/ WSettleCall(self) \/ WApprReq(self) \/ WSuspend(self)
                   \/ WMarkUnknown(self) \/ WAbortAudit(self)
                   \/ WAbortReserve(self) \/ WReserve(self)
                   \/ WSettApp(self) \/ WFin(self) \/ WJoinedSettle(self)
                   \/ WJoinedApp(self)

RScan == /\ pc["recovery"] = "RScan"
         /\ \E s \in Subs : RecoveryActionable(s)
         /\ \E s \in {t \in Subs : RecoveryActionable(t)}:
              IF Classify(s) = "FinalizeLedgerFromHistory"
                 THEN /\ resv' = [resv EXCEPT ![s] = settRec[s]]
                      /\ resvFinal' = [resvFinal EXCEPT ![s] = TRUE]
                      /\ lstate' = [lstate EXCEPT ![s] = "settled"]
                      /\ UNCHANGED << hostOf, apprDecided, unknownRes, convMat, 
                                      prepared, callSettled, callAudited, 
                                      settRec, laneEpoch, laneOwner, leaseLive, 
                                      settAppends >>
                 ELSE /\ IF Classify(s) = "AppendReservedSettlement"
                            THEN /\ laneEpoch' = laneEpoch + 1
                                 /\ laneOwner' = "none"
                                 /\ leaseLive' = FALSE
                                 /\ settRec' = [settRec EXCEPT ![s] = resv[s]]
                                 /\ settAppends' = [settAppends EXCEPT ![s] = CapInc(settAppends[s])]
                                 /\ UNCHANGED << lstate, resv, hostOf, 
                                                 apprDecided, unknownRes, 
                                                 convMat, prepared, 
                                                 callSettled, callAudited >>
                            ELSE /\ IF Classify(s) = "RevertJoining"
                                       THEN /\ lstate' = [lstate EXCEPT ![s] = "ready"]
                                            /\ hostOf' = [hostOf EXCEPT ![s] = NoSub]
                                            /\ UNCHANGED << resv, apprDecided, 
                                                            unknownRes, 
                                                            convMat, prepared, 
                                                            callSettled, 
                                                            callAudited, 
                                                            laneEpoch, 
                                                            laneOwner, 
                                                            leaseLive >>
                                       ELSE /\ IF Classify(s) = "RepairJoinMarker"
                                                  THEN /\ lstate' = [lstate EXCEPT ![s] = "joined"]
                                                       /\ UNCHANGED << resv, 
                                                                       apprDecided, 
                                                                       unknownRes, 
                                                                       convMat, 
                                                                       prepared, 
                                                                       callSettled, 
                                                                       callAudited, 
                                                                       laneEpoch, 
                                                                       laneOwner, 
                                                                       leaseLive >>
                                                  ELSE /\ IF Classify(s) = "SettleJoinedWithHost"
                                                             THEN /\ resv' = [resv EXCEPT ![s] = settRec[hostOf[s]]]
                                                                  /\ lstate' = [lstate EXCEPT ![s] = "terminalizing"]
                                                                  /\ UNCHANGED << apprDecided, 
                                                                                  unknownRes, 
                                                                                  convMat, 
                                                                                  prepared, 
                                                                                  callSettled, 
                                                                                  callAudited, 
                                                                                  laneEpoch, 
                                                                                  laneOwner, 
                                                                                  leaseLive >>
                                                             ELSE /\ IF Classify(s) = "SettleAborted"
                                                                        THEN /\ IF lstate[s] = "suspended"
                                                                                   THEN /\ apprDecided' = [apprDecided EXCEPT ![s] = TRUE]
                                                                                        /\ lstate' = [lstate EXCEPT ![s] = "input-applied"]
                                                                                        /\ UNCHANGED << resv, 
                                                                                                        callAudited, 
                                                                                                        laneEpoch, 
                                                                                                        laneOwner, 
                                                                                                        leaseLive >>
                                                                                   ELSE /\ IF OpenCall(s)
                                                                                              THEN /\ laneEpoch' = laneEpoch + 1
                                                                                                   /\ laneOwner' = "none"
                                                                                                   /\ leaseLive' = FALSE
                                                                                                   /\ callAudited' = [callAudited EXCEPT ![s] = TRUE]
                                                                                                   /\ UNCHANGED << lstate, 
                                                                                                                   resv >>
                                                                                              ELSE /\ laneEpoch' = laneEpoch + 1
                                                                                                   /\ laneOwner' = "none"
                                                                                                   /\ leaseLive' = FALSE
                                                                                                   /\ resv' = [resv EXCEPT ![s] = "aborted"]
                                                                                                   /\ lstate' = [lstate EXCEPT ![s] = "terminalizing"]
                                                                                                   /\ UNCHANGED callAudited
                                                                                        /\ UNCHANGED apprDecided
                                                                             /\ UNCHANGED << unknownRes, 
                                                                                             convMat, 
                                                                                             prepared, 
                                                                                             callSettled >>
                                                                        ELSE /\ IF Classify(s) = "MarkUnknown"
                                                                                   THEN /\ laneEpoch' = laneEpoch + 1
                                                                                        /\ laneOwner' = "none"
                                                                                        /\ leaseLive' = FALSE
                                                                                        /\ lstate' = [lstate EXCEPT ![s] = "unknown"]
                                                                                        /\ UNCHANGED << unknownRes, 
                                                                                                        convMat, 
                                                                                                        prepared, 
                                                                                                        callSettled >>
                                                                                   ELSE /\ IF Classify(s) = "ApplyUnknownResolutions"
                                                                                              THEN /\ IF OpenCall(s) /\ unknownRes[s] = "completed"
                                                                                                         THEN /\ callSettled' = [callSettled EXCEPT ![s] = TRUE]
                                                                                                              /\ UNCHANGED << unknownRes, 
                                                                                                                              prepared >>
                                                                                                         ELSE /\ IF OpenCall(s) /\ unknownRes[s] = "never"
                                                                                                                    THEN /\ prepared' = [prepared EXCEPT ![s] = FALSE]
                                                                                                                         /\ unknownRes' = [unknownRes EXCEPT ![s] = "none"]
                                                                                                                    ELSE /\ TRUE
                                                                                                                         /\ UNCHANGED << unknownRes, 
                                                                                                                                         prepared >>
                                                                                                              /\ UNCHANGED callSettled
                                                                                                   /\ lstate' = [lstate EXCEPT ![s] = "input-applied"]
                                                                                                   /\ UNCHANGED convMat
                                                                                              ELSE /\ IF Classify(s) = "ResumeSuspended"
                                                                                                         THEN /\ lstate' = [lstate EXCEPT ![s] = "input-applied"]
                                                                                                              /\ UNCHANGED convMat
                                                                                                         ELSE /\ IF Classify(s) = "CompleteMaterialization"
                                                                                                                    THEN /\ convMat' = TRUE
                                                                                                                         /\ UNCHANGED lstate
                                                                                                                    ELSE /\ lstate' = [lstate EXCEPT ![s] = "ready"]
                                                                                                                         /\ UNCHANGED convMat
                                                                                                   /\ UNCHANGED << unknownRes, 
                                                                                                                   prepared, 
                                                                                                                   callSettled >>
                                                                                        /\ UNCHANGED << laneEpoch, 
                                                                                                        laneOwner, 
                                                                                                        leaseLive >>
                                                                             /\ UNCHANGED << resv, 
                                                                                             apprDecided, 
                                                                                             callAudited >>
                                            /\ UNCHANGED hostOf
                                 /\ UNCHANGED << settRec, settAppends >>
                      /\ UNCHANGED resvFinal
         /\ pc' = [pc EXCEPT !["recovery"] = "RScan"]
         /\ UNCHANGED << inputMarked, abortIntent, inputRec, respKind, apprReq, 
                         extEffect, faults, inputAppends, staleWrites, 
                         unknownViol, sub, ep, jt, cs >>

recovery == RScan

ResLoop == /\ pc["resolver"] = "ResLoop"
           /\     \E s \in Subs :
              \/ (apprReq[s] /\ ~apprDecided[s] /\ lstate[s] # "settled")
              \/ (lstate[s] = "unknown" /\ unknownRes[s] = "none")
           /\ \E s \in          {t \in Subs :
                       \/ (apprReq[t] /\ ~apprDecided[t] /\ lstate[t] # "settled")
                       \/ (lstate[t] = "unknown" /\ unknownRes[t] = "none")}:
                IF lstate[s] = "unknown" /\ unknownRes[s] = "none"
                   THEN /\ IF extEffect[s] = "none"
                              THEN /\ unknownRes' = [unknownRes EXCEPT ![s] = "never"]
                              ELSE /\ unknownRes' = [unknownRes EXCEPT ![s] = "completed"]
                        /\ UNCHANGED << lstate, apprDecided >>
                   ELSE /\ apprDecided' = [apprDecided EXCEPT ![s] = TRUE]
                        /\ IF lstate[s] = "suspended"
                              THEN /\ lstate' = [lstate EXCEPT ![s] = "input-applied"]
                              ELSE /\ TRUE
                                   /\ UNCHANGED lstate
                        /\ UNCHANGED unknownRes
           /\ pc' = [pc EXCEPT !["resolver"] = "ResLoop"]
           /\ UNCHANGED << inputMarked, abortIntent, resv, resvFinal, hostOf, 
                           convMat, inputRec, respKind, prepared, callSettled, 
                           callAudited, apprReq, settRec, extEffect, laneEpoch, 
                           laneOwner, leaseLive, faults, inputAppends, 
                           settAppends, staleWrites, unknownViol, sub, ep, jt, 
                           cs >>

resolver == ResLoop

CLoop == /\ pc["client"] = "CLoop"
         /\ IF cs <= MaxSub
               THEN /\ pc' = [pc EXCEPT !["client"] = "CAdmit"]
               ELSE /\ pc' = [pc EXCEPT !["client"] = "Done"]
         /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, resvFinal, 
                         hostOf, apprDecided, unknownRes, convMat, inputRec, 
                         respKind, prepared, callSettled, callAudited, apprReq, 
                         settRec, extEffect, laneEpoch, laneOwner, leaseLive, 
                         faults, inputAppends, settAppends, staleWrites, 
                         unknownViol, sub, ep, jt, cs >>

CAdmit == /\ pc["client"] = "CAdmit"
          /\ lstate' = [lstate EXCEPT ![cs] = "admitted"]
          /\ pc' = [pc EXCEPT !["client"] = "CMat"]
          /\ UNCHANGED << inputMarked, abortIntent, resv, resvFinal, hostOf, 
                          apprDecided, unknownRes, convMat, inputRec, respKind, 
                          prepared, callSettled, callAudited, apprReq, settRec, 
                          extEffect, laneEpoch, laneOwner, leaseLive, faults, 
                          inputAppends, settAppends, staleWrites, unknownViol, 
                          sub, ep, jt, cs >>

CMat == /\ pc["client"] = "CMat"
        /\ \/ /\ faults < MaxFaults
              /\ faults' = faults + 1
              /\ pc' = [pc EXCEPT !["client"] = "CNext"]
              /\ UNCHANGED convMat
           \/ /\ convMat' = TRUE
              /\ pc' = [pc EXCEPT !["client"] = "CReady"]
              /\ UNCHANGED faults
        /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, resvFinal, 
                        hostOf, apprDecided, unknownRes, inputRec, respKind, 
                        prepared, callSettled, callAudited, apprReq, settRec, 
                        extEffect, laneEpoch, laneOwner, leaseLive, 
                        inputAppends, settAppends, staleWrites, unknownViol, 
                        sub, ep, jt, cs >>

CReady == /\ pc["client"] = "CReady"
          /\ \/ /\ faults < MaxFaults
                /\ faults' = faults + 1
                /\ pc' = [pc EXCEPT !["client"] = "CNext"]
                /\ UNCHANGED lstate
             \/ /\ IF lstate[cs] = "admitted"
                      THEN /\ lstate' = [lstate EXCEPT ![cs] = "ready"]
                      ELSE /\ TRUE
                           /\ UNCHANGED lstate
                /\ pc' = [pc EXCEPT !["client"] = "CNext"]
                /\ UNCHANGED faults
          /\ UNCHANGED << inputMarked, abortIntent, resv, resvFinal, hostOf, 
                          apprDecided, unknownRes, convMat, inputRec, respKind, 
                          prepared, callSettled, callAudited, apprReq, settRec, 
                          extEffect, laneEpoch, laneOwner, leaseLive, 
                          inputAppends, settAppends, staleWrites, unknownViol, 
                          sub, ep, jt, cs >>

CNext == /\ pc["client"] = "CNext"
         /\ cs' = cs + 1
         /\ pc' = [pc EXCEPT !["client"] = "CLoop"]
         /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, resvFinal, 
                         hostOf, apprDecided, unknownRes, convMat, inputRec, 
                         respKind, prepared, callSettled, callAudited, apprReq, 
                         settRec, extEffect, laneEpoch, laneOwner, leaseLive, 
                         faults, inputAppends, settAppends, staleWrites, 
                         unknownViol, sub, ep, jt >>

client == CLoop \/ CAdmit \/ CMat \/ CReady \/ CNext

AbLoop == /\ pc["aborter"] = "AbLoop"
          /\     \E s \in Subs :
             lstate[s] \notin {"unsubmitted", "joined", "settled"} /\ ~abortIntent[s]
          /\ \E s \in        {t \in Subs :
                      lstate[t] \notin {"unsubmitted", "joined", "settled"} /\ ~abortIntent[t]}:
               abortIntent' = [abortIntent EXCEPT ![s] = TRUE]
          /\ pc' = [pc EXCEPT !["aborter"] = "AbLoop"]
          /\ UNCHANGED << lstate, inputMarked, resv, resvFinal, hostOf, 
                          apprDecided, unknownRes, convMat, inputRec, respKind, 
                          prepared, callSettled, callAudited, apprReq, settRec, 
                          extEffect, laneEpoch, laneOwner, leaseLive, faults, 
                          inputAppends, settAppends, staleWrites, unknownViol, 
                          sub, ep, jt, cs >>

aborter == AbLoop

ExpLoop == /\ pc["expiry"] = "ExpLoop"
           /\ leaseLive /\ faults < MaxFaults
           /\ leaseLive' = FALSE
           /\ faults' = faults + 1
           /\ pc' = [pc EXCEPT !["expiry"] = "ExpLoop"]
           /\ UNCHANGED << lstate, inputMarked, abortIntent, resv, resvFinal, 
                           hostOf, apprDecided, unknownRes, convMat, inputRec, 
                           respKind, prepared, callSettled, callAudited, 
                           apprReq, settRec, extEffect, laneEpoch, laneOwner, 
                           inputAppends, settAppends, staleWrites, unknownViol, 
                           sub, ep, jt, cs >>

expiry == ExpLoop

Next == recovery \/ resolver \/ client \/ aborter \/ expiry
           \/ (\E self \in Workers: worker(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Workers : WF_vars(worker(self))
        /\ WF_vars(recovery)
        /\ WF_vars(resolver)
        /\ WF_vars(client)

\* END TRANSLATION

-----------------------------------------------------------------------------
(***************************************************************************)
(* Invariants (safety).  Each maps to executable evidence named in         *)
(* formal/CORRESPONDENCE.md.                                               *)
(***************************************************************************)

States == {"unsubmitted", "admitted", "ready", "joining", "joined",
           "input-applied", "suspended", "unknown", "terminalizing", "settled"}

TypeOK ==
  /\ lstate \in [Subs -> States]
  /\ respKind \in [Subs -> {"none", "finish", "tool", "approval"}]
  /\ resv \in [Subs -> {"none"} \cup Outcomes]
  /\ settRec \in [Subs -> {"none"} \cup Outcomes]
  /\ unknownRes \in [Subs -> {"none", "completed", "never"}]
  /\ extEffect \in [Subs -> {"none", "maybe", "done"}]
  /\ hostOf \in [Subs -> Subs \cup {NoSub}]
  /\ faults \in 0..MaxFaults
  /\ laneEpoch \in Nat

\* DUR-002 / DUR-011: at most one canonical terminal record per accepted
\* Submission; the canonical record always matches the reserved outcome; a
\* ledger-settled row always has its canonical settlement.
ExactlyOneSettlement ==
  \A s \in Subs :
    /\ settAppends[s] <= 1
    /\ settRec[s] # "none" => resv[s] = settRec[s]
    /\ resvFinal[s] => settRec[s] # "none"
    /\ lstate[s] = "settled" => (settRec[s] # "none" /\ resvFinal[s])

\* DUR-004: canonical input order and canonical settlement order both follow
\* the admitted queue sequence.  An earlier Submission without a canonical
\* input must have settled aborted-without-execution (durability SS13:
\* accepted-but-inactive work may settle aborted with no execution attempt).
\* (WP7 item (c) proposes relaxing the settlement half for aborted never-run
\* work; this models CURRENT behavior, where it is strict.)
FIFOPerLane ==
  \A j, k \in Subs : j < k =>
    /\ inputRec[k] => (inputRec[j] \/ settRec[j] = "aborted")
    /\ settRec[k] # "none" => settRec[j] # "none"

\* DUR-014: an accepted Submission is never nonterminally invisible -- the
\* pure classifier yields a decision (never NoAction) for every nonterminal
\* row, and a settled row always carries exactly one canonical outcome.
NoLostAcceptedWork ==
  \A s \in Subs :
    /\ lstate[s] \notin {"unsubmitted", "settled"} => Classify(s) # "NoAction"
    /\ lstate[s] = "settled" => settRec[s] # "none"

\* DUR-009 / DUR-017: no canonical model/tool record commits on an unknown
\* lane without a covering resolution (ghost counter), and only a lane that
\* durably declared a tool batch can ever be unknown.  NOTE: the lane may be
\* unknown WITHOUT a currently-open call -- TLC found the benign interleaving
\* where a superseded Attempt re-marks a just-resolved lane through the
\* ownership-free ledger.markUnknown (its canonical audit append dedupes as
\* an identity conflict); classifier row 7 then wakes the lane immediately.
UnknownBlocksContinuation ==
  /\ unknownViol = 0
  /\ \A s \in Subs : lstate[s] = "unknown" => respKind[s] # "none"

\* DUR-006: no canonical or ledger mutation ever commits under a superseded
\* Attempt (ghost counter; the NoFencing negative control violates this).
FencingSafety == staleWrites = 0

\* DUR-016: a joined input is appended exactly once, never lost after the
\* joined marker, and settles with its host's outcome.
JoinConservation ==
  \A s \in Subs :
    /\ inputAppends[s] <= 1
    /\ lstate[s] = "joined" => (inputRec[s] /\ hostOf[s] # NoSub)
    /\ (hostOf[s] # NoSub /\ settRec[s] # "none") =>
         settRec[s] = settRec[hostOf[s]]

\* DUR-009 corollary: a canonical tool result is never fabricated -- it exists
\* only when the external invocation at least started.
NoFabricatedToolResult ==
  \A s \in Subs : callSettled[s] => extEffect[s] # "none"

-----------------------------------------------------------------------------
(***************************************************************************)
(* Liveness, under the documented assumptions (durability SS1): weak       *)
(* fairness of worker, recovery, and resolution-dependency actions, and a  *)
(* bounded fault budget (crashes/expiries eventually cease).  The resolver *)
(* process IS the required outcome-resolution dependency (DUR-017).        *)
(***************************************************************************)

EventuallySettled ==
  \A s \in Subs : (lstate[s] = "admitted") ~> (lstate[s] = "settled")

=============================================================================
