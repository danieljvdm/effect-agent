------------------------ MODULE SubagentEstablishment ------------------------
(***************************************************************************)
(* Bounded model of the durable attached-Subagent protocol:               *)
(* one parent Submission with one                                        *)
(* delegation Tool Call, one child Conversation lane, the recoverable      *)
(* establishment ladder (reserve -> request -> admit -> materialize ->     *)
(* lineage -> ready -> start), waitingForChild suspension, at-least-once   *)
(* recordChildSettled wakes, the canonical-settlement join, the budget     *)
(* reservation lifecycle, and request-abort-and-join (SS13.1).             *)
(*                                                                         *)
(* Routed cross-lane calls can fail or answer Indeterminate (SUB-031):     *)
(* bounded by MaxRouteFaults, after which the authoritative owner answers  *)
(* definitively -- the documented resolution-dependency assumption.        *)
(*                                                                         *)
(* AwaitParentEstablishment models the plan SS7(a) fix: when TRUE, the     *)
(* child lane's own recovery DEFERS materialization/readiness repair of a  *)
(* parent-linked admitted Submission until the immutable lineage record is *)
(* canonical, so a child can never run a Turn before its lineage exists.   *)
(* When FALSE (the current implementation: recovery.ts rows 11 applied to  *)
(* a child), TLC finds the child-runs-before-lineage interleaving -- see   *)
(* SubagentEstablishmentRace.cfg (expected violation) versus               *)
(* SubagentEstablishmentFix.cfg (expected pass).                           *)
(*                                                                         *)
(* Scope split: Attempt ownership rotation and producer-epoch fencing are  *)
(* modeled and checked in DurableSubmission.tla; here each lane has one    *)
(* claim at a time, recovery acts only on quiet lanes, and crash clears    *)
(* the claim (lease expiry abstracted).                                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  MaxFaults,               \* worker crash budget
  MaxRouteFaults,          \* Indeterminate admission answers + lost child-settled wakes
  AwaitParentEstablishment \* FALSE = current discipline; TRUE = plan SS7(a) fix

(* --algorithm SubagentEstablishment

variables
  \* -- parent lane (ledger + canonical records) --
  pState = "input-applied",   \* parent Submission ledger state
  pOwner = "none",            \* the parent lane's single Attempt claim
  pResp = FALSE,              \* committed model response declaring the delegation call
  reqRec = FALSE,             \* canonical SubagentRequested (parent log)
  startRec = FALSE,           \* canonical SubagentStarted (parent log)
  joinRec = FALSE,            \* canonical SubagentJoined + parent ToolCallSettled (atomic batch)
  pAbort = FALSE,             \* durable parent abort intent (requestAbort)
  pResv = "none",             \* reserved parent settlement outcome
  pSettRec = "none",          \* canonical parent SubmissionSettled outcome
  pFinal = FALSE,
  \* -- child budget reservation (reserveChildBudget lifecycle) --
  resvSt = "none",            \* none | reserved | releasePending | released
  freezeKind = "none",        \* none | join | orphan (the frozen accounting decision)
  \* -- child lane --
  cState = "none",            \* none | admitted | ready | input-applied | terminalizing | settled
  cOwner = "none",
  cMat = FALSE,               \* child Conversation materialized + ConversationCreated
  lineageRec = FALSE,         \* canonical SubagentLineageRecorded (child log)
  cInputRec = FALSE,          \* canonical child input:{sid}
  cTurn = FALSE,              \* the child ran a model Turn (ModelResponseRecorded)
  cAbort = FALSE,             \* durable child abort intent (propagated)
  cResv = "none",
  cSettRec = "none",          \* canonical child SubmissionSettled outcome (the join authority)
  cFinal = FALSE,
  \* -- budgets --
  faults = 0,
  routeFaults = 0,
  \* -- ghost evidence --
  cAdmits = 0,                \* child admissions performed (OneChildPerToolCall)
  joinCount = 0,
  releaseCount = 0,
  pSettAppends = 0,
  cSettAppends = 0,
  ranBeforeLineage = FALSE;   \* the SS7(a) race witness

define
  ChildSettledCanonically == cSettRec # "none"
  ChildEstablished == cState # "none"
  CapInc(n) == IF n < 2 THEN n + 1 ELSE n
  \* The child lane's own recovery may repair materialization/readiness of a
  \* parent-linked admitted Submission only when the fix discipline allows it.
  ChildSelfRepairAllowed == lineageRec \/ ~AwaitParentEstablishment
end define;

\* ========================= parent worker =========================
\* The parent Attempt driving the delegation Tool batch: establishment ladder
\* (establishChildFromRequest), waitingForChild suspension, join, release,
\* terminalization.  Crash choice at every durable boundary.
fair process pworker = "pworker"
begin
PIdle:
  await pState \in {"input-applied", "terminalizing"} /\ pOwner = "none";
  pOwner := "pworker";
PResume:
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if pState = "settled" then
      pOwner := "none"; goto PIdle;
    elsif pSettRec # "none" then goto PFin;
    elsif pResv # "none" then goto PSettApp;
    elsif pAbort then goto PAbort;
    elsif ~pResp then goto PRespond;
    else goto PLadder;
    end if;
  end either;
PRespond:
  \* one complete model response declaring the delegation Tool Call
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    pResp := TRUE;
  end either;
PLadder:
  \* the idempotent establishment/join ladder, most-settled work first
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if pAbort then goto PAbort;
    elsif joinRec then
      if resvSt = "reserved" then goto PRelBegin;
      elsif resvSt = "releasePending" then goto PRelease;
      else goto PReserveOut;
      end if;
    elsif ChildSettledCanonically /\ startRec then goto PJoin;
    elsif resvSt = "none" then goto PReserveBudget;
    elsif ~reqRec then goto PReqApp;
    elsif ~startRec then goto PResolveAdm;
    else goto PSuspend;
    end if;
  end either;
PReserveBudget:
  \* ledger.reserveChildBudget: idempotent get-or-create under the parent claim
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if resvSt = "none" then resvSt := "reserved"; end if;
    goto PLadder;
  end either;
PReqApp:
  \* canonical SubagentRequested append (deterministic identity, parent fence)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    reqRec := TRUE;
    goto PLadder;
  end either;
PResolveAdm:
  \* ledger.resolveAdmission (SUB-031): the routed authoritative lookup may
  \* answer Indeterminate (bounded); only a proven NotAdmitted admits
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    either
      await routeFaults < MaxRouteFaults;
      routeFaults := routeFaults + 1;   \* Indeterminate: wait and retry
      goto PLadder;
    or
      if cState = "none" then goto PAdmit;
      else goto PMat;                   \* Admitted: reattach the ONE child
      end if;
    end either;
  end either;
PAdmit:
  \* ledger.admit on the child lane with immutable ParentLinkage (SUB-016)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if cState = "none" then
      cState := "admitted";
      cAdmits := CapInc(cAdmits);
    end if;
    goto PMat;
  end either;
PMat:
  \* child store.materialize + ConversationCreated (idempotent)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    cMat := TRUE;
  end either;
PLineage:
  \* ensureChildLineage: deterministic SubagentLineageRecorded append to the
  \* child log (stale-tail refresh + record-identity dedupe absorbed)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    lineageRec := TRUE;
  end either;
PReady:
  \* ledger.markReady(child): idempotent admitted -> ready, then wake
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if cState = "admitted" then cState := "ready"; end if;
  end either;
PStart:
  \* canonical SubagentStarted append (the exact deterministic link, SUB-017)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    startRec := TRUE;
    goto PLadder;
  end either;
PSuspend:
  \* ledger.suspend(WaitingForChild): the lane holds no worker permit while
  \* the child runs; a child settlement that raced ahead resumes immediately
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if ChildSettledCanonically then
      goto PLadder;                     \* resume-immediately
    else
      pState := "suspended";
      pOwner := "none";
      goto PIdle;
    end if;
  end either;
PJoin:
  \* verifySettledChild + the atomic SubagentJoined/ToolCallSettled batch:
  \* the child's CANONICAL Settlement is the only cross-lane authority
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if ~joinRec then
      joinRec := TRUE;
      joinCount := CapInc(joinCount);
    end if;
    goto PLadder;
  end either;
PRelBegin:
  \* ledger.beginChildBudgetRelease: freeze the accounting decision FROM the
  \* canonical join exactly once (reserved -> releasePending)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if resvSt = "reserved" then
      resvSt := "releasePending";
      freezeKind := "join";
    end if;
    goto PRelease;
  end either;
PRelease:
  \* ledger.releaseChildBudget: releasePending -> released, exactly once
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if resvSt = "releasePending" then
      resvSt := "released";
      releaseCount := CapInc(releaseCount);
    end if;
    goto PLadder;
  end either;
PReserveOut:
  \* ledger.reserveSettlement for the parent (DUR-011); an abort intent that
  \* arrived after the join settles the parent aborted (SS13.1)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if pResv = "none" then
      if pAbort then pResv := "aborted"; else pResv := "completed"; end if;
      pState := "terminalizing";
    end if;
    goto PSettApp;
  end either;
PSettApp:
  \* canonical parent SubmissionSettled append
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if pSettRec = "none" then
      pSettRec := pResv;
      pSettAppends := CapInc(pSettAppends);
    end if;
  end either;
PFin:
  \* ledger.finalizeSettlement (token-free, canonical history authorizes it)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    pFinal := TRUE;
    pState := "settled";
    pOwner := "none";
    goto PIdle;
  end either;
PAbort:
  \* classifyDelegationAbort (SS13.1 request-abort-and-join): joins first,
  \* repairs the start link before aborting a linked child, never admits a
  \* child merely to abort it, releases provably-childless reservations once
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if joinRec then
      if resvSt = "reserved" then goto PRelBegin;
      elsif resvSt = "releasePending" then goto PRelease;
      else goto PReserveOut;
      end if;
    elsif ChildSettledCanonically /\ startRec then goto PJoin;
    elsif ChildEstablished /\ ~startRec then goto PMat;  \* repair the link first
    elsif startRec then
      if ~cAbort then goto PAbortChild; else goto PAbortWait; end if;
    elsif reqRec then
      \* requested, no proven child: resolve authoritatively; only a proven
      \* NotAdmitted releases the reservation (never a second admission)
      either
        await routeFaults < MaxRouteFaults;
        routeFaults := routeFaults + 1;
        goto PAbort;
      or
        if resvSt = "reserved" then goto PAbortOrphan;
        elsif resvSt = "releasePending" then goto PRelease;
        else goto PReserveOut;
        end if;
      end either;
    elsif resvSt = "reserved" then goto PAbortOrphan;
    elsif resvSt = "releasePending" then goto PRelease;
    else goto PReserveOut;
    end if;
  end either;
PAbortOrphan:
  \* beginChildBudgetRelease with the deterministic zero-consumed decision for
  \* a provably childless reservation under abort (spec SS13/SS14: "releases
  \* the reservation exactly once"; never admits a child merely to abort it)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if resvSt = "reserved" then
      resvSt := "releasePending";
      freezeKind := "orphan";
    end if;
    goto PRelease;
  end either;
PAbortChild:
  \* the ONE idempotent durable child abort command (SUB-022, DUR-012)
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    cAbort := TRUE;
    goto PAbortWait;
  end either;
PAbortWait:
  \* the parent stays waiting for the child join; never settles past it
  either
    await faults < MaxFaults; faults := faults + 1;
    pOwner := "none"; goto PIdle;
  or
    if ChildSettledCanonically then
      goto PAbort;
    else
      pState := "suspended";
      pOwner := "none";
      goto PIdle;
    end if;
  end either;
end process;

\* ========================= child worker =========================
\* The child's own Attempt on its own Conversation lane (SUB-020): claims a
\* ready child, applies input, runs one Turn, terminalizes, finalizes, then
\* routes the at-least-once recordChildSettled wake to the parent lane.
fair process cworker = "cworker"
begin
CIdle:
  await cState \in {"ready", "input-applied"} /\ cOwner = "none";
  cOwner := "cworker";
CResume:
  either
    await faults < MaxFaults; faults := faults + 1;
    cOwner := "none"; goto CIdle;
  or
    if cState = "settled" then
      cOwner := "none"; goto CIdle;
    elsif cSettRec # "none" then goto CFin;
    elsif cResv # "none" then goto CSettApp;
    elsif cAbort then goto CReserveAborted;
    elsif ~cInputRec then goto CInput;
    elsif ~cTurn then goto CTurn;
    else goto CReserve;
    end if;
  end either;
CInput:
  \* canonical child input append + applied marker (windows proven in
  \* DurableSubmission.tla; folded here)
  either
    await faults < MaxFaults; faults := faults + 1;
    cOwner := "none"; goto CIdle;
  or
    cInputRec := TRUE;
    cState := "input-applied";
    goto CResume;
  end either;
CTurn:
  \* the child's model Turn -- the SS7(a) race witness: under the current
  \* discipline it can run before the parent's lineage append is canonical
  either
    await faults < MaxFaults; faults := faults + 1;
    cOwner := "none"; goto CIdle;
  or
    cTurn := TRUE;
    if ~lineageRec then ranBeforeLineage := TRUE; end if;
    goto CReserve;
  end either;
CReserve:
  either
    await faults < MaxFaults; faults := faults + 1;
    cOwner := "none"; goto CIdle;
  or
    if cResv = "none" then
      if cAbort then cResv := "aborted"; else cResv := "completed"; end if;
      cState := "terminalizing";
    end if;
    goto CSettApp;
  end either;
CReserveAborted:
  either
    await faults < MaxFaults; faults := faults + 1;
    cOwner := "none"; goto CIdle;
  or
    if cResv = "none" then
      cResv := "aborted";
      cState := "terminalizing";
    end if;
    goto CSettApp;
  end either;
CSettApp:
  \* canonical child SubmissionSettled append -- the join authority (SUB-019)
  either
    await faults < MaxFaults; faults := faults + 1;
    cOwner := "none"; goto CIdle;
  or
    if cSettRec = "none" then
      cSettRec := cResv;
      cSettAppends := CapInc(cSettAppends);
    end if;
    goto CFin;
  end either;
CFin:
  either
    await faults < MaxFaults; faults := faults + 1;
    cOwner := "none"; goto CIdle;
  or
    cFinal := TRUE;
    cState := "settled";
    cOwner := "none";
  end either;
CNotify:
  \* ledger.recordChildSettled routed to the parent lane: at-least-once -- the
  \* wake can be lost (bounded) and recovery's ResumeWaitingParent replays it
  either
    await routeFaults < MaxRouteFaults;
    routeFaults := routeFaults + 1;     \* notification lost in transit
    goto CIdle;
  or
    if pState = "suspended" then
      pState := "input-applied";        \* woken (idempotent: not-waiting is a no-op)
    end if;
    goto CIdle;
  end either;
end process;

\* ========================= recovery =========================
\* runRecovery over both lanes: classify (recovery.ts classifyRecovery with
\* the S2 rows) and execute one idempotent durable repair per iteration.
\* Acts only on quiet lanes; re-running the loop is recovery of recovery.
fair process recovery = "recovery"
begin
RScan:
  while TRUE do
    either
      \* ---- parent lane rows (spec SS13, classifyRecovery order) ----
      await pOwner = "none" /\ pState # "settled";
      if pSettRec # "none" then
        \* FinalizeLedgerFromHistory
        pResv := pSettRec; pFinal := TRUE; pState := "settled";
      elsif pResv # "none" then
        \* AppendReservedSettlement
        pSettRec := pResv; pSettAppends := CapInc(pSettAppends);
      elsif pAbort then
        \* classifyDelegationAbort rows
        if ChildEstablished /\ ~startRec then
          \* repair the establishment so the abort targets a linked child
          if ~cMat then cMat := TRUE;
          elsif ~lineageRec then lineageRec := TRUE;
          elsif cState = "admitted" then cState := "ready";
          else startRec := TRUE;
          end if;
        elsif startRec /\ ~joinRec /\ cSettRec = "none" /\ ~cAbort then
          \* PropagateChildAbort: the one idempotent child abort command
          cAbort := TRUE;
        elsif startRec /\ ~joinRec /\ cSettRec # "none" /\ pState = "suspended" then
          \* ResumeWaitingParent: replay the idempotent wake; a worker joins
          pState := "input-applied";
        elsif reqRec /\ cState = "none" /\ resvSt = "reserved" then
          \* proven childless under abort: freeze the zero-consumed decision
          either
            await routeFaults < MaxRouteFaults;
            routeFaults := routeFaults + 1;  \* Indeterminate: keep waiting
          or
            resvSt := "releasePending"; freezeKind := "orphan";
          end either;
        elsif ~reqRec /\ resvSt = "reserved" then
          \* ReleaseOrphanChildReservation under abort (no resumable intent)
          resvSt := "releasePending"; freezeKind := "orphan";
        elsif resvSt = "releasePending" then
          \* ApplyJoinAccounting second half
          resvSt := "released"; releaseCount := CapInc(releaseCount);
        elsif joinRec /\ resvSt = "reserved" then
          resvSt := "releasePending"; freezeKind := "join";
        elsif (cState = "none" \/ joinRec) /\ resvSt \in {"none", "released"}
              /\ pState # "suspended" then
          \* SettleAborted: no open child obligation remains
          pResv := "aborted"; pState := "terminalizing";
        end if;
      else
        \* classifyDelegationRepairs rows (live parent)
        if joinRec /\ resvSt = "reserved" then
          resvSt := "releasePending"; freezeKind := "join";
        elsif resvSt = "releasePending" then
          resvSt := "released"; releaseCount := CapInc(releaseCount);
        elsif reqRec /\ cState = "none" /\ ~startRec then
          \* CompleteChildAdmission after a proven NotAdmitted (SUB-031)
          either
            await routeFaults < MaxRouteFaults;
            routeFaults := routeFaults + 1;  \* AwaitChildAdmissionResolution
          or
            cState := "admitted"; cAdmits := CapInc(cAdmits);
          end either;
        elsif ChildEstablished /\ ~startRec then
          \* RepairSubagentStartLink via the idempotent establishment ladder
          if ~cMat then cMat := TRUE;
          elsif ~lineageRec then lineageRec := TRUE;
          elsif cState = "admitted" then cState := "ready";
          else startRec := TRUE;
          end if;
        elsif startRec /\ ~joinRec /\ cSettRec = "none"
              /\ pState = "input-applied" then
          \* EnsureWaitingForChild: restore the lost suspension
          pState := "suspended";
        elsif startRec /\ ~joinRec /\ cSettRec # "none"
              /\ pState = "suspended" then
          \* ResumeWaitingParent: replay the idempotent wake (dropped
          \* recordChildSettled); the claiming worker performs the join
          pState := "input-applied";
        end if;
      end if;
    or
      \* ---- child lane rows (the child is a normal Submission, SUB-020) ----
      await cOwner = "none" /\ cState \notin {"none", "settled"};
      if cSettRec # "none" then
        cResv := cSettRec; cFinal := TRUE; cState := "settled";
      elsif cResv # "none" then
        cSettRec := cResv; cSettAppends := CapInc(cSettAppends);
      elsif cAbort then
        \* child SettleAborted (no execution attempt needed, durability SS13)
        cResv := "aborted"; cState := "terminalizing";
      elsif cState = "admitted" /\ ChildSelfRepairAllowed then
        \* recovery.ts row 11 applied by the CHILD lane's own recovery:
        \* CompleteMaterialization / RepairReadiness.  Under the plan SS7(a)
        \* fix (AwaitParentEstablishment = TRUE) a parent-linked child without
        \* canonical lineage DEFERS here and the parent's establishment
        \* completes readiness instead.
        if ~cMat then cMat := TRUE;
        else cState := "ready";
        end if;
      end if;
    end either;
  end while;
end process;

\* ========================= abort requests =========================
\* requestAbort against the parent (SS13.1).  Not fair: abort MAY happen.
\* Direct child aborts are propagation-only in S2 (spec SS13.1).
process aborter = "aborter"
begin
AbLoop:
  await ~pAbort /\ pState # "settled";
  pAbort := TRUE;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "48a8070e" /\ chksum(tla) = "77b60514")
VARIABLES pState, pOwner, pResp, reqRec, startRec, joinRec, pAbort, pResv, 
          pSettRec, pFinal, resvSt, freezeKind, cState, cOwner, cMat, 
          lineageRec, cInputRec, cTurn, cAbort, cResv, cSettRec, cFinal, 
          faults, routeFaults, cAdmits, joinCount, releaseCount, pSettAppends, 
          cSettAppends, ranBeforeLineage, pc

(* define statement *)
ChildSettledCanonically == cSettRec # "none"
ChildEstablished == cState # "none"
CapInc(n) == IF n < 2 THEN n + 1 ELSE n


ChildSelfRepairAllowed == lineageRec \/ ~AwaitParentEstablishment


vars == << pState, pOwner, pResp, reqRec, startRec, joinRec, pAbort, pResv, 
           pSettRec, pFinal, resvSt, freezeKind, cState, cOwner, cMat, 
           lineageRec, cInputRec, cTurn, cAbort, cResv, cSettRec, cFinal, 
           faults, routeFaults, cAdmits, joinCount, releaseCount, 
           pSettAppends, cSettAppends, ranBeforeLineage, pc >>

ProcSet == {"pworker"} \cup {"cworker"} \cup {"recovery"} \cup {"aborter"}

Init == (* Global variables *)
        /\ pState = "input-applied"
        /\ pOwner = "none"
        /\ pResp = FALSE
        /\ reqRec = FALSE
        /\ startRec = FALSE
        /\ joinRec = FALSE
        /\ pAbort = FALSE
        /\ pResv = "none"
        /\ pSettRec = "none"
        /\ pFinal = FALSE
        /\ resvSt = "none"
        /\ freezeKind = "none"
        /\ cState = "none"
        /\ cOwner = "none"
        /\ cMat = FALSE
        /\ lineageRec = FALSE
        /\ cInputRec = FALSE
        /\ cTurn = FALSE
        /\ cAbort = FALSE
        /\ cResv = "none"
        /\ cSettRec = "none"
        /\ cFinal = FALSE
        /\ faults = 0
        /\ routeFaults = 0
        /\ cAdmits = 0
        /\ joinCount = 0
        /\ releaseCount = 0
        /\ pSettAppends = 0
        /\ cSettAppends = 0
        /\ ranBeforeLineage = FALSE
        /\ pc = [self \in ProcSet |-> CASE self = "pworker" -> "PIdle"
                                        [] self = "cworker" -> "CIdle"
                                        [] self = "recovery" -> "RScan"
                                        [] self = "aborter" -> "AbLoop"]

PIdle == /\ pc["pworker"] = "PIdle"
         /\ pState \in {"input-applied", "terminalizing"} /\ pOwner = "none"
         /\ pOwner' = "pworker"
         /\ pc' = [pc EXCEPT !["pworker"] = "PResume"]
         /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                         pResv, pSettRec, pFinal, resvSt, freezeKind, cState, 
                         cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                         cResv, cSettRec, cFinal, faults, routeFaults, cAdmits, 
                         joinCount, releaseCount, pSettAppends, cSettAppends, 
                         ranBeforeLineage >>

PResume == /\ pc["pworker"] = "PResume"
           /\ \/ /\ faults < MaxFaults
                 /\ faults' = faults + 1
                 /\ pOwner' = "none"
                 /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
              \/ /\ IF pState = "settled"
                       THEN /\ pOwner' = "none"
                            /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                       ELSE /\ IF pSettRec # "none"
                                  THEN /\ pc' = [pc EXCEPT !["pworker"] = "PFin"]
                                  ELSE /\ IF pResv # "none"
                                             THEN /\ pc' = [pc EXCEPT !["pworker"] = "PSettApp"]
                                             ELSE /\ IF pAbort
                                                        THEN /\ pc' = [pc EXCEPT !["pworker"] = "PAbort"]
                                                        ELSE /\ IF ~pResp
                                                                   THEN /\ pc' = [pc EXCEPT !["pworker"] = "PRespond"]
                                                                   ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                            /\ UNCHANGED pOwner
                 /\ UNCHANGED faults
           /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                           pResv, pSettRec, pFinal, resvSt, freezeKind, cState, 
                           cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                           cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                           joinCount, releaseCount, pSettAppends, cSettAppends, 
                           ranBeforeLineage >>

PRespond == /\ pc["pworker"] = "PRespond"
            /\ \/ /\ faults < MaxFaults
                  /\ faults' = faults + 1
                  /\ pOwner' = "none"
                  /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                  /\ pResp' = pResp
               \/ /\ pResp' = TRUE
                  /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                  /\ UNCHANGED <<pOwner, faults>>
            /\ UNCHANGED << pState, reqRec, startRec, joinRec, pAbort, pResv, 
                            pSettRec, pFinal, resvSt, freezeKind, cState, 
                            cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                            cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                            joinCount, releaseCount, pSettAppends, 
                            cSettAppends, ranBeforeLineage >>

PLadder == /\ pc["pworker"] = "PLadder"
           /\ \/ /\ faults < MaxFaults
                 /\ faults' = faults + 1
                 /\ pOwner' = "none"
                 /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
              \/ /\ IF pAbort
                       THEN /\ pc' = [pc EXCEPT !["pworker"] = "PAbort"]
                       ELSE /\ IF joinRec
                                  THEN /\ IF resvSt = "reserved"
                                             THEN /\ pc' = [pc EXCEPT !["pworker"] = "PRelBegin"]
                                             ELSE /\ IF resvSt = "releasePending"
                                                        THEN /\ pc' = [pc EXCEPT !["pworker"] = "PRelease"]
                                                        ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PReserveOut"]
                                  ELSE /\ IF ChildSettledCanonically /\ startRec
                                             THEN /\ pc' = [pc EXCEPT !["pworker"] = "PJoin"]
                                             ELSE /\ IF resvSt = "none"
                                                        THEN /\ pc' = [pc EXCEPT !["pworker"] = "PReserveBudget"]
                                                        ELSE /\ IF ~reqRec
                                                                   THEN /\ pc' = [pc EXCEPT !["pworker"] = "PReqApp"]
                                                                   ELSE /\ IF ~startRec
                                                                              THEN /\ pc' = [pc EXCEPT !["pworker"] = "PResolveAdm"]
                                                                              ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PSuspend"]
                 /\ UNCHANGED <<pOwner, faults>>
           /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                           pResv, pSettRec, pFinal, resvSt, freezeKind, cState, 
                           cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                           cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                           joinCount, releaseCount, pSettAppends, cSettAppends, 
                           ranBeforeLineage >>

PReserveBudget == /\ pc["pworker"] = "PReserveBudget"
                  /\ \/ /\ faults < MaxFaults
                        /\ faults' = faults + 1
                        /\ pOwner' = "none"
                        /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                        /\ UNCHANGED resvSt
                     \/ /\ IF resvSt = "none"
                              THEN /\ resvSt' = "reserved"
                              ELSE /\ TRUE
                                   /\ UNCHANGED resvSt
                        /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                        /\ UNCHANGED <<pOwner, faults>>
                  /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, 
                                  pAbort, pResv, pSettRec, pFinal, freezeKind, 
                                  cState, cOwner, cMat, lineageRec, cInputRec, 
                                  cTurn, cAbort, cResv, cSettRec, cFinal, 
                                  routeFaults, cAdmits, joinCount, 
                                  releaseCount, pSettAppends, cSettAppends, 
                                  ranBeforeLineage >>

PReqApp == /\ pc["pworker"] = "PReqApp"
           /\ \/ /\ faults < MaxFaults
                 /\ faults' = faults + 1
                 /\ pOwner' = "none"
                 /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                 /\ UNCHANGED reqRec
              \/ /\ reqRec' = TRUE
                 /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                 /\ UNCHANGED <<pOwner, faults>>
           /\ UNCHANGED << pState, pResp, startRec, joinRec, pAbort, pResv, 
                           pSettRec, pFinal, resvSt, freezeKind, cState, 
                           cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                           cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                           joinCount, releaseCount, pSettAppends, cSettAppends, 
                           ranBeforeLineage >>

PResolveAdm == /\ pc["pworker"] = "PResolveAdm"
               /\ \/ /\ faults < MaxFaults
                     /\ faults' = faults + 1
                     /\ pOwner' = "none"
                     /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                     /\ UNCHANGED routeFaults
                  \/ /\ \/ /\ routeFaults < MaxRouteFaults
                           /\ routeFaults' = routeFaults + 1
                           /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                        \/ /\ IF cState = "none"
                                 THEN /\ pc' = [pc EXCEPT !["pworker"] = "PAdmit"]
                                 ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PMat"]
                           /\ UNCHANGED routeFaults
                     /\ UNCHANGED <<pOwner, faults>>
               /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, 
                               pAbort, pResv, pSettRec, pFinal, resvSt, 
                               freezeKind, cState, cOwner, cMat, lineageRec, 
                               cInputRec, cTurn, cAbort, cResv, cSettRec, 
                               cFinal, cAdmits, joinCount, releaseCount, 
                               pSettAppends, cSettAppends, ranBeforeLineage >>

PAdmit == /\ pc["pworker"] = "PAdmit"
          /\ \/ /\ faults < MaxFaults
                /\ faults' = faults + 1
                /\ pOwner' = "none"
                /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                /\ UNCHANGED <<cState, cAdmits>>
             \/ /\ IF cState = "none"
                      THEN /\ cState' = "admitted"
                           /\ cAdmits' = CapInc(cAdmits)
                      ELSE /\ TRUE
                           /\ UNCHANGED << cState, cAdmits >>
                /\ pc' = [pc EXCEPT !["pworker"] = "PMat"]
                /\ UNCHANGED <<pOwner, faults>>
          /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                          pResv, pSettRec, pFinal, resvSt, freezeKind, cOwner, 
                          cMat, lineageRec, cInputRec, cTurn, cAbort, cResv, 
                          cSettRec, cFinal, routeFaults, joinCount, 
                          releaseCount, pSettAppends, cSettAppends, 
                          ranBeforeLineage >>

PMat == /\ pc["pworker"] = "PMat"
        /\ \/ /\ faults < MaxFaults
              /\ faults' = faults + 1
              /\ pOwner' = "none"
              /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
              /\ cMat' = cMat
           \/ /\ cMat' = TRUE
              /\ pc' = [pc EXCEPT !["pworker"] = "PLineage"]
              /\ UNCHANGED <<pOwner, faults>>
        /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                        pResv, pSettRec, pFinal, resvSt, freezeKind, cState, 
                        cOwner, lineageRec, cInputRec, cTurn, cAbort, cResv, 
                        cSettRec, cFinal, routeFaults, cAdmits, joinCount, 
                        releaseCount, pSettAppends, cSettAppends, 
                        ranBeforeLineage >>

PLineage == /\ pc["pworker"] = "PLineage"
            /\ \/ /\ faults < MaxFaults
                  /\ faults' = faults + 1
                  /\ pOwner' = "none"
                  /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                  /\ UNCHANGED lineageRec
               \/ /\ lineageRec' = TRUE
                  /\ pc' = [pc EXCEPT !["pworker"] = "PReady"]
                  /\ UNCHANGED <<pOwner, faults>>
            /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                            pResv, pSettRec, pFinal, resvSt, freezeKind, 
                            cState, cOwner, cMat, cInputRec, cTurn, cAbort, 
                            cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                            joinCount, releaseCount, pSettAppends, 
                            cSettAppends, ranBeforeLineage >>

PReady == /\ pc["pworker"] = "PReady"
          /\ \/ /\ faults < MaxFaults
                /\ faults' = faults + 1
                /\ pOwner' = "none"
                /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                /\ UNCHANGED cState
             \/ /\ IF cState = "admitted"
                      THEN /\ cState' = "ready"
                      ELSE /\ TRUE
                           /\ UNCHANGED cState
                /\ pc' = [pc EXCEPT !["pworker"] = "PStart"]
                /\ UNCHANGED <<pOwner, faults>>
          /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                          pResv, pSettRec, pFinal, resvSt, freezeKind, cOwner, 
                          cMat, lineageRec, cInputRec, cTurn, cAbort, cResv, 
                          cSettRec, cFinal, routeFaults, cAdmits, joinCount, 
                          releaseCount, pSettAppends, cSettAppends, 
                          ranBeforeLineage >>

PStart == /\ pc["pworker"] = "PStart"
          /\ \/ /\ faults < MaxFaults
                /\ faults' = faults + 1
                /\ pOwner' = "none"
                /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                /\ UNCHANGED startRec
             \/ /\ startRec' = TRUE
                /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                /\ UNCHANGED <<pOwner, faults>>
          /\ UNCHANGED << pState, pResp, reqRec, joinRec, pAbort, pResv, 
                          pSettRec, pFinal, resvSt, freezeKind, cState, cOwner, 
                          cMat, lineageRec, cInputRec, cTurn, cAbort, cResv, 
                          cSettRec, cFinal, routeFaults, cAdmits, joinCount, 
                          releaseCount, pSettAppends, cSettAppends, 
                          ranBeforeLineage >>

PSuspend == /\ pc["pworker"] = "PSuspend"
            /\ \/ /\ faults < MaxFaults
                  /\ faults' = faults + 1
                  /\ pOwner' = "none"
                  /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                  /\ UNCHANGED pState
               \/ /\ IF ChildSettledCanonically
                        THEN /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                             /\ UNCHANGED << pState, pOwner >>
                        ELSE /\ pState' = "suspended"
                             /\ pOwner' = "none"
                             /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                  /\ UNCHANGED faults
            /\ UNCHANGED << pResp, reqRec, startRec, joinRec, pAbort, pResv, 
                            pSettRec, pFinal, resvSt, freezeKind, cState, 
                            cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                            cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                            joinCount, releaseCount, pSettAppends, 
                            cSettAppends, ranBeforeLineage >>

PJoin == /\ pc["pworker"] = "PJoin"
         /\ \/ /\ faults < MaxFaults
               /\ faults' = faults + 1
               /\ pOwner' = "none"
               /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
               /\ UNCHANGED <<joinRec, joinCount>>
            \/ /\ IF ~joinRec
                     THEN /\ joinRec' = TRUE
                          /\ joinCount' = CapInc(joinCount)
                     ELSE /\ TRUE
                          /\ UNCHANGED << joinRec, joinCount >>
               /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
               /\ UNCHANGED <<pOwner, faults>>
         /\ UNCHANGED << pState, pResp, reqRec, startRec, pAbort, pResv, 
                         pSettRec, pFinal, resvSt, freezeKind, cState, cOwner, 
                         cMat, lineageRec, cInputRec, cTurn, cAbort, cResv, 
                         cSettRec, cFinal, routeFaults, cAdmits, releaseCount, 
                         pSettAppends, cSettAppends, ranBeforeLineage >>

PRelBegin == /\ pc["pworker"] = "PRelBegin"
             /\ \/ /\ faults < MaxFaults
                   /\ faults' = faults + 1
                   /\ pOwner' = "none"
                   /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                   /\ UNCHANGED <<resvSt, freezeKind>>
                \/ /\ IF resvSt = "reserved"
                         THEN /\ resvSt' = "releasePending"
                              /\ freezeKind' = "join"
                         ELSE /\ TRUE
                              /\ UNCHANGED << resvSt, freezeKind >>
                   /\ pc' = [pc EXCEPT !["pworker"] = "PRelease"]
                   /\ UNCHANGED <<pOwner, faults>>
             /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                             pResv, pSettRec, pFinal, cState, cOwner, cMat, 
                             lineageRec, cInputRec, cTurn, cAbort, cResv, 
                             cSettRec, cFinal, routeFaults, cAdmits, joinCount, 
                             releaseCount, pSettAppends, cSettAppends, 
                             ranBeforeLineage >>

PRelease == /\ pc["pworker"] = "PRelease"
            /\ \/ /\ faults < MaxFaults
                  /\ faults' = faults + 1
                  /\ pOwner' = "none"
                  /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                  /\ UNCHANGED <<resvSt, releaseCount>>
               \/ /\ IF resvSt = "releasePending"
                        THEN /\ resvSt' = "released"
                             /\ releaseCount' = CapInc(releaseCount)
                        ELSE /\ TRUE
                             /\ UNCHANGED << resvSt, releaseCount >>
                  /\ pc' = [pc EXCEPT !["pworker"] = "PLadder"]
                  /\ UNCHANGED <<pOwner, faults>>
            /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                            pResv, pSettRec, pFinal, freezeKind, cState, 
                            cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                            cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                            joinCount, pSettAppends, cSettAppends, 
                            ranBeforeLineage >>

PReserveOut == /\ pc["pworker"] = "PReserveOut"
               /\ \/ /\ faults < MaxFaults
                     /\ faults' = faults + 1
                     /\ pOwner' = "none"
                     /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                     /\ UNCHANGED <<pState, pResv>>
                  \/ /\ IF pResv = "none"
                           THEN /\ IF pAbort
                                      THEN /\ pResv' = "aborted"
                                      ELSE /\ pResv' = "completed"
                                /\ pState' = "terminalizing"
                           ELSE /\ TRUE
                                /\ UNCHANGED << pState, pResv >>
                     /\ pc' = [pc EXCEPT !["pworker"] = "PSettApp"]
                     /\ UNCHANGED <<pOwner, faults>>
               /\ UNCHANGED << pResp, reqRec, startRec, joinRec, pAbort, 
                               pSettRec, pFinal, resvSt, freezeKind, cState, 
                               cOwner, cMat, lineageRec, cInputRec, cTurn, 
                               cAbort, cResv, cSettRec, cFinal, routeFaults, 
                               cAdmits, joinCount, releaseCount, pSettAppends, 
                               cSettAppends, ranBeforeLineage >>

PSettApp == /\ pc["pworker"] = "PSettApp"
            /\ \/ /\ faults < MaxFaults
                  /\ faults' = faults + 1
                  /\ pOwner' = "none"
                  /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                  /\ UNCHANGED <<pSettRec, pSettAppends>>
               \/ /\ IF pSettRec = "none"
                        THEN /\ pSettRec' = pResv
                             /\ pSettAppends' = CapInc(pSettAppends)
                        ELSE /\ TRUE
                             /\ UNCHANGED << pSettRec, pSettAppends >>
                  /\ pc' = [pc EXCEPT !["pworker"] = "PFin"]
                  /\ UNCHANGED <<pOwner, faults>>
            /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                            pResv, pFinal, resvSt, freezeKind, cState, cOwner, 
                            cMat, lineageRec, cInputRec, cTurn, cAbort, cResv, 
                            cSettRec, cFinal, routeFaults, cAdmits, joinCount, 
                            releaseCount, cSettAppends, ranBeforeLineage >>

PFin == /\ pc["pworker"] = "PFin"
        /\ \/ /\ faults < MaxFaults
              /\ faults' = faults + 1
              /\ pOwner' = "none"
              /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
              /\ UNCHANGED <<pState, pFinal>>
           \/ /\ pFinal' = TRUE
              /\ pState' = "settled"
              /\ pOwner' = "none"
              /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
              /\ UNCHANGED faults
        /\ UNCHANGED << pResp, reqRec, startRec, joinRec, pAbort, pResv, 
                        pSettRec, resvSt, freezeKind, cState, cOwner, cMat, 
                        lineageRec, cInputRec, cTurn, cAbort, cResv, cSettRec, 
                        cFinal, routeFaults, cAdmits, joinCount, releaseCount, 
                        pSettAppends, cSettAppends, ranBeforeLineage >>

PAbort == /\ pc["pworker"] = "PAbort"
          /\ \/ /\ faults < MaxFaults
                /\ faults' = faults + 1
                /\ pOwner' = "none"
                /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                /\ UNCHANGED routeFaults
             \/ /\ IF joinRec
                      THEN /\ IF resvSt = "reserved"
                                 THEN /\ pc' = [pc EXCEPT !["pworker"] = "PRelBegin"]
                                 ELSE /\ IF resvSt = "releasePending"
                                            THEN /\ pc' = [pc EXCEPT !["pworker"] = "PRelease"]
                                            ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PReserveOut"]
                           /\ UNCHANGED routeFaults
                      ELSE /\ IF ChildSettledCanonically /\ startRec
                                 THEN /\ pc' = [pc EXCEPT !["pworker"] = "PJoin"]
                                      /\ UNCHANGED routeFaults
                                 ELSE /\ IF ChildEstablished /\ ~startRec
                                            THEN /\ pc' = [pc EXCEPT !["pworker"] = "PMat"]
                                                 /\ UNCHANGED routeFaults
                                            ELSE /\ IF startRec
                                                       THEN /\ IF ~cAbort
                                                                  THEN /\ pc' = [pc EXCEPT !["pworker"] = "PAbortChild"]
                                                                  ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PAbortWait"]
                                                            /\ UNCHANGED routeFaults
                                                       ELSE /\ IF reqRec
                                                                  THEN /\ \/ /\ routeFaults < MaxRouteFaults
                                                                             /\ routeFaults' = routeFaults + 1
                                                                             /\ pc' = [pc EXCEPT !["pworker"] = "PAbort"]
                                                                          \/ /\ IF resvSt = "reserved"
                                                                                   THEN /\ pc' = [pc EXCEPT !["pworker"] = "PAbortOrphan"]
                                                                                   ELSE /\ IF resvSt = "releasePending"
                                                                                              THEN /\ pc' = [pc EXCEPT !["pworker"] = "PRelease"]
                                                                                              ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PReserveOut"]
                                                                             /\ UNCHANGED routeFaults
                                                                  ELSE /\ IF resvSt = "reserved"
                                                                             THEN /\ pc' = [pc EXCEPT !["pworker"] = "PAbortOrphan"]
                                                                             ELSE /\ IF resvSt = "releasePending"
                                                                                        THEN /\ pc' = [pc EXCEPT !["pworker"] = "PRelease"]
                                                                                        ELSE /\ pc' = [pc EXCEPT !["pworker"] = "PReserveOut"]
                                                                       /\ UNCHANGED routeFaults
                /\ UNCHANGED <<pOwner, faults>>
          /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, pAbort, 
                          pResv, pSettRec, pFinal, resvSt, freezeKind, cState, 
                          cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                          cResv, cSettRec, cFinal, cAdmits, joinCount, 
                          releaseCount, pSettAppends, cSettAppends, 
                          ranBeforeLineage >>

PAbortOrphan == /\ pc["pworker"] = "PAbortOrphan"
                /\ \/ /\ faults < MaxFaults
                      /\ faults' = faults + 1
                      /\ pOwner' = "none"
                      /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                      /\ UNCHANGED <<resvSt, freezeKind>>
                   \/ /\ IF resvSt = "reserved"
                            THEN /\ resvSt' = "releasePending"
                                 /\ freezeKind' = "orphan"
                            ELSE /\ TRUE
                                 /\ UNCHANGED << resvSt, freezeKind >>
                      /\ pc' = [pc EXCEPT !["pworker"] = "PRelease"]
                      /\ UNCHANGED <<pOwner, faults>>
                /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, 
                                pAbort, pResv, pSettRec, pFinal, cState, 
                                cOwner, cMat, lineageRec, cInputRec, cTurn, 
                                cAbort, cResv, cSettRec, cFinal, routeFaults, 
                                cAdmits, joinCount, releaseCount, pSettAppends, 
                                cSettAppends, ranBeforeLineage >>

PAbortChild == /\ pc["pworker"] = "PAbortChild"
               /\ \/ /\ faults < MaxFaults
                     /\ faults' = faults + 1
                     /\ pOwner' = "none"
                     /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                     /\ UNCHANGED cAbort
                  \/ /\ cAbort' = TRUE
                     /\ pc' = [pc EXCEPT !["pworker"] = "PAbortWait"]
                     /\ UNCHANGED <<pOwner, faults>>
               /\ UNCHANGED << pState, pResp, reqRec, startRec, joinRec, 
                               pAbort, pResv, pSettRec, pFinal, resvSt, 
                               freezeKind, cState, cOwner, cMat, lineageRec, 
                               cInputRec, cTurn, cResv, cSettRec, cFinal, 
                               routeFaults, cAdmits, joinCount, releaseCount, 
                               pSettAppends, cSettAppends, ranBeforeLineage >>

PAbortWait == /\ pc["pworker"] = "PAbortWait"
              /\ \/ /\ faults < MaxFaults
                    /\ faults' = faults + 1
                    /\ pOwner' = "none"
                    /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                    /\ UNCHANGED pState
                 \/ /\ IF ChildSettledCanonically
                          THEN /\ pc' = [pc EXCEPT !["pworker"] = "PAbort"]
                               /\ UNCHANGED << pState, pOwner >>
                          ELSE /\ pState' = "suspended"
                               /\ pOwner' = "none"
                               /\ pc' = [pc EXCEPT !["pworker"] = "PIdle"]
                    /\ UNCHANGED faults
              /\ UNCHANGED << pResp, reqRec, startRec, joinRec, pAbort, pResv, 
                              pSettRec, pFinal, resvSt, freezeKind, cState, 
                              cOwner, cMat, lineageRec, cInputRec, cTurn, 
                              cAbort, cResv, cSettRec, cFinal, routeFaults, 
                              cAdmits, joinCount, releaseCount, pSettAppends, 
                              cSettAppends, ranBeforeLineage >>

pworker == PIdle \/ PResume \/ PRespond \/ PLadder \/ PReserveBudget
              \/ PReqApp \/ PResolveAdm \/ PAdmit \/ PMat \/ PLineage
              \/ PReady \/ PStart \/ PSuspend \/ PJoin \/ PRelBegin
              \/ PRelease \/ PReserveOut \/ PSettApp \/ PFin \/ PAbort
              \/ PAbortOrphan \/ PAbortChild \/ PAbortWait

CIdle == /\ pc["cworker"] = "CIdle"
         /\ cState \in {"ready", "input-applied"} /\ cOwner = "none"
         /\ cOwner' = "cworker"
         /\ pc' = [pc EXCEPT !["cworker"] = "CResume"]
         /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                         pAbort, pResv, pSettRec, pFinal, resvSt, freezeKind, 
                         cState, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                         cResv, cSettRec, cFinal, faults, routeFaults, cAdmits, 
                         joinCount, releaseCount, pSettAppends, cSettAppends, 
                         ranBeforeLineage >>

CResume == /\ pc["cworker"] = "CResume"
           /\ \/ /\ faults < MaxFaults
                 /\ faults' = faults + 1
                 /\ cOwner' = "none"
                 /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
              \/ /\ IF cState = "settled"
                       THEN /\ cOwner' = "none"
                            /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
                       ELSE /\ IF cSettRec # "none"
                                  THEN /\ pc' = [pc EXCEPT !["cworker"] = "CFin"]
                                  ELSE /\ IF cResv # "none"
                                             THEN /\ pc' = [pc EXCEPT !["cworker"] = "CSettApp"]
                                             ELSE /\ IF cAbort
                                                        THEN /\ pc' = [pc EXCEPT !["cworker"] = "CReserveAborted"]
                                                        ELSE /\ IF ~cInputRec
                                                                   THEN /\ pc' = [pc EXCEPT !["cworker"] = "CInput"]
                                                                   ELSE /\ IF ~cTurn
                                                                              THEN /\ pc' = [pc EXCEPT !["cworker"] = "CTurn"]
                                                                              ELSE /\ pc' = [pc EXCEPT !["cworker"] = "CReserve"]
                            /\ UNCHANGED cOwner
                 /\ UNCHANGED faults
           /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                           pAbort, pResv, pSettRec, pFinal, resvSt, freezeKind, 
                           cState, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                           cResv, cSettRec, cFinal, routeFaults, cAdmits, 
                           joinCount, releaseCount, pSettAppends, cSettAppends, 
                           ranBeforeLineage >>

CInput == /\ pc["cworker"] = "CInput"
          /\ \/ /\ faults < MaxFaults
                /\ faults' = faults + 1
                /\ cOwner' = "none"
                /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
                /\ UNCHANGED <<cState, cInputRec>>
             \/ /\ cInputRec' = TRUE
                /\ cState' = "input-applied"
                /\ pc' = [pc EXCEPT !["cworker"] = "CResume"]
                /\ UNCHANGED <<cOwner, faults>>
          /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                          pAbort, pResv, pSettRec, pFinal, resvSt, freezeKind, 
                          cMat, lineageRec, cTurn, cAbort, cResv, cSettRec, 
                          cFinal, routeFaults, cAdmits, joinCount, 
                          releaseCount, pSettAppends, cSettAppends, 
                          ranBeforeLineage >>

CTurn == /\ pc["cworker"] = "CTurn"
         /\ \/ /\ faults < MaxFaults
               /\ faults' = faults + 1
               /\ cOwner' = "none"
               /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
               /\ UNCHANGED <<cTurn, ranBeforeLineage>>
            \/ /\ cTurn' = TRUE
               /\ IF ~lineageRec
                     THEN /\ ranBeforeLineage' = TRUE
                     ELSE /\ TRUE
                          /\ UNCHANGED ranBeforeLineage
               /\ pc' = [pc EXCEPT !["cworker"] = "CReserve"]
               /\ UNCHANGED <<cOwner, faults>>
         /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                         pAbort, pResv, pSettRec, pFinal, resvSt, freezeKind, 
                         cState, cMat, lineageRec, cInputRec, cAbort, cResv, 
                         cSettRec, cFinal, routeFaults, cAdmits, joinCount, 
                         releaseCount, pSettAppends, cSettAppends >>

CReserve == /\ pc["cworker"] = "CReserve"
            /\ \/ /\ faults < MaxFaults
                  /\ faults' = faults + 1
                  /\ cOwner' = "none"
                  /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
                  /\ UNCHANGED <<cState, cResv>>
               \/ /\ IF cResv = "none"
                        THEN /\ IF cAbort
                                   THEN /\ cResv' = "aborted"
                                   ELSE /\ cResv' = "completed"
                             /\ cState' = "terminalizing"
                        ELSE /\ TRUE
                             /\ UNCHANGED << cState, cResv >>
                  /\ pc' = [pc EXCEPT !["cworker"] = "CSettApp"]
                  /\ UNCHANGED <<cOwner, faults>>
            /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                            pAbort, pResv, pSettRec, pFinal, resvSt, 
                            freezeKind, cMat, lineageRec, cInputRec, cTurn, 
                            cAbort, cSettRec, cFinal, routeFaults, cAdmits, 
                            joinCount, releaseCount, pSettAppends, 
                            cSettAppends, ranBeforeLineage >>

CReserveAborted == /\ pc["cworker"] = "CReserveAborted"
                   /\ \/ /\ faults < MaxFaults
                         /\ faults' = faults + 1
                         /\ cOwner' = "none"
                         /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
                         /\ UNCHANGED <<cState, cResv>>
                      \/ /\ IF cResv = "none"
                               THEN /\ cResv' = "aborted"
                                    /\ cState' = "terminalizing"
                               ELSE /\ TRUE
                                    /\ UNCHANGED << cState, cResv >>
                         /\ pc' = [pc EXCEPT !["cworker"] = "CSettApp"]
                         /\ UNCHANGED <<cOwner, faults>>
                   /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, 
                                   joinRec, pAbort, pResv, pSettRec, pFinal, 
                                   resvSt, freezeKind, cMat, lineageRec, 
                                   cInputRec, cTurn, cAbort, cSettRec, cFinal, 
                                   routeFaults, cAdmits, joinCount, 
                                   releaseCount, pSettAppends, cSettAppends, 
                                   ranBeforeLineage >>

CSettApp == /\ pc["cworker"] = "CSettApp"
            /\ \/ /\ faults < MaxFaults
                  /\ faults' = faults + 1
                  /\ cOwner' = "none"
                  /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
                  /\ UNCHANGED <<cSettRec, cSettAppends>>
               \/ /\ IF cSettRec = "none"
                        THEN /\ cSettRec' = cResv
                             /\ cSettAppends' = CapInc(cSettAppends)
                        ELSE /\ TRUE
                             /\ UNCHANGED << cSettRec, cSettAppends >>
                  /\ pc' = [pc EXCEPT !["cworker"] = "CFin"]
                  /\ UNCHANGED <<cOwner, faults>>
            /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                            pAbort, pResv, pSettRec, pFinal, resvSt, 
                            freezeKind, cState, cMat, lineageRec, cInputRec, 
                            cTurn, cAbort, cResv, cFinal, routeFaults, cAdmits, 
                            joinCount, releaseCount, pSettAppends, 
                            ranBeforeLineage >>

CFin == /\ pc["cworker"] = "CFin"
        /\ \/ /\ faults < MaxFaults
              /\ faults' = faults + 1
              /\ cOwner' = "none"
              /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
              /\ UNCHANGED <<cState, cFinal>>
           \/ /\ cFinal' = TRUE
              /\ cState' = "settled"
              /\ cOwner' = "none"
              /\ pc' = [pc EXCEPT !["cworker"] = "CNotify"]
              /\ UNCHANGED faults
        /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                        pAbort, pResv, pSettRec, pFinal, resvSt, freezeKind, 
                        cMat, lineageRec, cInputRec, cTurn, cAbort, cResv, 
                        cSettRec, routeFaults, cAdmits, joinCount, 
                        releaseCount, pSettAppends, cSettAppends, 
                        ranBeforeLineage >>

CNotify == /\ pc["cworker"] = "CNotify"
           /\ \/ /\ routeFaults < MaxRouteFaults
                 /\ routeFaults' = routeFaults + 1
                 /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
                 /\ UNCHANGED pState
              \/ /\ IF pState = "suspended"
                       THEN /\ pState' = "input-applied"
                       ELSE /\ TRUE
                            /\ UNCHANGED pState
                 /\ pc' = [pc EXCEPT !["cworker"] = "CIdle"]
                 /\ UNCHANGED routeFaults
           /\ UNCHANGED << pOwner, pResp, reqRec, startRec, joinRec, pAbort, 
                           pResv, pSettRec, pFinal, resvSt, freezeKind, cState, 
                           cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                           cResv, cSettRec, cFinal, faults, cAdmits, joinCount, 
                           releaseCount, pSettAppends, cSettAppends, 
                           ranBeforeLineage >>

cworker == CIdle \/ CResume \/ CInput \/ CTurn \/ CReserve
              \/ CReserveAborted \/ CSettApp \/ CFin \/ CNotify

RScan == /\ pc["recovery"] = "RScan"
         /\ \/ /\ pOwner = "none" /\ pState # "settled"
               /\ IF pSettRec # "none"
                     THEN /\ pResv' = pSettRec
                          /\ pFinal' = TRUE
                          /\ pState' = "settled"
                          /\ UNCHANGED << startRec, pSettRec, resvSt, 
                                          freezeKind, cState, cMat, lineageRec, 
                                          cAbort, routeFaults, cAdmits, 
                                          releaseCount, pSettAppends >>
                     ELSE /\ IF pResv # "none"
                                THEN /\ pSettRec' = pResv
                                     /\ pSettAppends' = CapInc(pSettAppends)
                                     /\ UNCHANGED << pState, startRec, pResv, 
                                                     resvSt, freezeKind, 
                                                     cState, cMat, lineageRec, 
                                                     cAbort, routeFaults, 
                                                     cAdmits, releaseCount >>
                                ELSE /\ IF pAbort
                                           THEN /\ IF ChildEstablished /\ ~startRec
                                                      THEN /\ IF ~cMat
                                                                 THEN /\ cMat' = TRUE
                                                                      /\ UNCHANGED << startRec, 
                                                                                      cState, 
                                                                                      lineageRec >>
                                                                 ELSE /\ IF ~lineageRec
                                                                            THEN /\ lineageRec' = TRUE
                                                                                 /\ UNCHANGED << startRec, 
                                                                                                 cState >>
                                                                            ELSE /\ IF cState = "admitted"
                                                                                       THEN /\ cState' = "ready"
                                                                                            /\ UNCHANGED startRec
                                                                                       ELSE /\ startRec' = TRUE
                                                                                            /\ UNCHANGED cState
                                                                                 /\ UNCHANGED lineageRec
                                                                      /\ cMat' = cMat
                                                           /\ UNCHANGED << pState, 
                                                                           pResv, 
                                                                           resvSt, 
                                                                           freezeKind, 
                                                                           cAbort, 
                                                                           routeFaults, 
                                                                           releaseCount >>
                                                      ELSE /\ IF startRec /\ ~joinRec /\ cSettRec = "none" /\ ~cAbort
                                                                 THEN /\ cAbort' = TRUE
                                                                      /\ UNCHANGED << pState, 
                                                                                      pResv, 
                                                                                      resvSt, 
                                                                                      freezeKind, 
                                                                                      routeFaults, 
                                                                                      releaseCount >>
                                                                 ELSE /\ IF startRec /\ ~joinRec /\ cSettRec # "none" /\ pState = "suspended"
                                                                            THEN /\ pState' = "input-applied"
                                                                                 /\ UNCHANGED << pResv, 
                                                                                                 resvSt, 
                                                                                                 freezeKind, 
                                                                                                 routeFaults, 
                                                                                                 releaseCount >>
                                                                            ELSE /\ IF reqRec /\ cState = "none" /\ resvSt = "reserved"
                                                                                       THEN /\ \/ /\ routeFaults < MaxRouteFaults
                                                                                                  /\ routeFaults' = routeFaults + 1
                                                                                                  /\ UNCHANGED <<resvSt, freezeKind>>
                                                                                               \/ /\ resvSt' = "releasePending"
                                                                                                  /\ freezeKind' = "orphan"
                                                                                                  /\ UNCHANGED routeFaults
                                                                                            /\ UNCHANGED << pState, 
                                                                                                            pResv, 
                                                                                                            releaseCount >>
                                                                                       ELSE /\ IF ~reqRec /\ resvSt = "reserved"
                                                                                                  THEN /\ resvSt' = "releasePending"
                                                                                                       /\ freezeKind' = "orphan"
                                                                                                       /\ UNCHANGED << pState, 
                                                                                                                       pResv, 
                                                                                                                       releaseCount >>
                                                                                                  ELSE /\ IF resvSt = "releasePending"
                                                                                                             THEN /\ resvSt' = "released"
                                                                                                                  /\ releaseCount' = CapInc(releaseCount)
                                                                                                                  /\ UNCHANGED << pState, 
                                                                                                                                  pResv, 
                                                                                                                                  freezeKind >>
                                                                                                             ELSE /\ IF joinRec /\ resvSt = "reserved"
                                                                                                                        THEN /\ resvSt' = "releasePending"
                                                                                                                             /\ freezeKind' = "join"
                                                                                                                             /\ UNCHANGED << pState, 
                                                                                                                                             pResv >>
                                                                                                                        ELSE /\ IF (cState = "none" \/ joinRec) /\ resvSt \in {"none", "released"}
                                                                                                                                   /\ pState # "suspended"
                                                                                                                                   THEN /\ pResv' = "aborted"
                                                                                                                                        /\ pState' = "terminalizing"
                                                                                                                                   ELSE /\ TRUE
                                                                                                                                        /\ UNCHANGED << pState, 
                                                                                                                                                        pResv >>
                                                                                                                             /\ UNCHANGED << resvSt, 
                                                                                                                                             freezeKind >>
                                                                                                                  /\ UNCHANGED releaseCount
                                                                                            /\ UNCHANGED routeFaults
                                                                      /\ UNCHANGED cAbort
                                                           /\ UNCHANGED << startRec, 
                                                                           cState, 
                                                                           cMat, 
                                                                           lineageRec >>
                                                /\ UNCHANGED cAdmits
                                           ELSE /\ IF joinRec /\ resvSt = "reserved"
                                                      THEN /\ resvSt' = "releasePending"
                                                           /\ freezeKind' = "join"
                                                           /\ UNCHANGED << pState, 
                                                                           startRec, 
                                                                           cState, 
                                                                           cMat, 
                                                                           lineageRec, 
                                                                           routeFaults, 
                                                                           cAdmits, 
                                                                           releaseCount >>
                                                      ELSE /\ IF resvSt = "releasePending"
                                                                 THEN /\ resvSt' = "released"
                                                                      /\ releaseCount' = CapInc(releaseCount)
                                                                      /\ UNCHANGED << pState, 
                                                                                      startRec, 
                                                                                      cState, 
                                                                                      cMat, 
                                                                                      lineageRec, 
                                                                                      routeFaults, 
                                                                                      cAdmits >>
                                                                 ELSE /\ IF reqRec /\ cState = "none" /\ ~startRec
                                                                            THEN /\ \/ /\ routeFaults < MaxRouteFaults
                                                                                       /\ routeFaults' = routeFaults + 1
                                                                                       /\ UNCHANGED <<cState, cAdmits>>
                                                                                    \/ /\ cState' = "admitted"
                                                                                       /\ cAdmits' = CapInc(cAdmits)
                                                                                       /\ UNCHANGED routeFaults
                                                                                 /\ UNCHANGED << pState, 
                                                                                                 startRec, 
                                                                                                 cMat, 
                                                                                                 lineageRec >>
                                                                            ELSE /\ IF ChildEstablished /\ ~startRec
                                                                                       THEN /\ IF ~cMat
                                                                                                  THEN /\ cMat' = TRUE
                                                                                                       /\ UNCHANGED << startRec, 
                                                                                                                       cState, 
                                                                                                                       lineageRec >>
                                                                                                  ELSE /\ IF ~lineageRec
                                                                                                             THEN /\ lineageRec' = TRUE
                                                                                                                  /\ UNCHANGED << startRec, 
                                                                                                                                  cState >>
                                                                                                             ELSE /\ IF cState = "admitted"
                                                                                                                        THEN /\ cState' = "ready"
                                                                                                                             /\ UNCHANGED startRec
                                                                                                                        ELSE /\ startRec' = TRUE
                                                                                                                             /\ UNCHANGED cState
                                                                                                                  /\ UNCHANGED lineageRec
                                                                                                       /\ cMat' = cMat
                                                                                            /\ UNCHANGED pState
                                                                                       ELSE /\ IF startRec /\ ~joinRec /\ cSettRec = "none"
                                                                                                  /\ pState = "input-applied"
                                                                                                  THEN /\ pState' = "suspended"
                                                                                                  ELSE /\ IF startRec /\ ~joinRec /\ cSettRec # "none"
                                                                                                             /\ pState = "suspended"
                                                                                                             THEN /\ pState' = "input-applied"
                                                                                                             ELSE /\ TRUE
                                                                                                                  /\ UNCHANGED pState
                                                                                            /\ UNCHANGED << startRec, 
                                                                                                            cState, 
                                                                                                            cMat, 
                                                                                                            lineageRec >>
                                                                                 /\ UNCHANGED << routeFaults, 
                                                                                                 cAdmits >>
                                                                      /\ UNCHANGED << resvSt, 
                                                                                      releaseCount >>
                                                           /\ UNCHANGED freezeKind
                                                /\ UNCHANGED << pResv, cAbort >>
                                     /\ UNCHANGED << pSettRec, pSettAppends >>
                          /\ UNCHANGED pFinal
               /\ UNCHANGED <<cResv, cSettRec, cFinal, cSettAppends>>
            \/ /\ cOwner = "none" /\ cState \notin {"none", "settled"}
               /\ IF cSettRec # "none"
                     THEN /\ cResv' = cSettRec
                          /\ cFinal' = TRUE
                          /\ cState' = "settled"
                          /\ UNCHANGED << cMat, cSettRec, cSettAppends >>
                     ELSE /\ IF cResv # "none"
                                THEN /\ cSettRec' = cResv
                                     /\ cSettAppends' = CapInc(cSettAppends)
                                     /\ UNCHANGED << cState, cMat, cResv >>
                                ELSE /\ IF cAbort
                                           THEN /\ cResv' = "aborted"
                                                /\ cState' = "terminalizing"
                                                /\ cMat' = cMat
                                           ELSE /\ IF cState = "admitted" /\ ChildSelfRepairAllowed
                                                      THEN /\ IF ~cMat
                                                                 THEN /\ cMat' = TRUE
                                                                      /\ UNCHANGED cState
                                                                 ELSE /\ cState' = "ready"
                                                                      /\ cMat' = cMat
                                                      ELSE /\ TRUE
                                                           /\ UNCHANGED << cState, 
                                                                           cMat >>
                                                /\ cResv' = cResv
                                     /\ UNCHANGED << cSettRec, cSettAppends >>
                          /\ UNCHANGED cFinal
               /\ UNCHANGED <<pState, startRec, pResv, pSettRec, pFinal, resvSt, freezeKind, lineageRec, cAbort, routeFaults, cAdmits, releaseCount, pSettAppends>>
         /\ pc' = [pc EXCEPT !["recovery"] = "RScan"]
         /\ UNCHANGED << pOwner, pResp, reqRec, joinRec, pAbort, cOwner, 
                         cInputRec, cTurn, faults, joinCount, ranBeforeLineage >>

recovery == RScan

AbLoop == /\ pc["aborter"] = "AbLoop"
          /\ ~pAbort /\ pState # "settled"
          /\ pAbort' = TRUE
          /\ pc' = [pc EXCEPT !["aborter"] = "Done"]
          /\ UNCHANGED << pState, pOwner, pResp, reqRec, startRec, joinRec, 
                          pResv, pSettRec, pFinal, resvSt, freezeKind, cState, 
                          cOwner, cMat, lineageRec, cInputRec, cTurn, cAbort, 
                          cResv, cSettRec, cFinal, faults, routeFaults, 
                          cAdmits, joinCount, releaseCount, pSettAppends, 
                          cSettAppends, ranBeforeLineage >>

aborter == AbLoop

Next == pworker \/ cworker \/ recovery \/ aborter

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(pworker)
        /\ WF_vars(cworker)
        /\ WF_vars(recovery)

\* END TRANSLATION

-----------------------------------------------------------------------------
(***************************************************************************)
(* Invariants (safety).                                                     *)
(***************************************************************************)

TypeOK ==
  /\ pState \in {"input-applied", "suspended", "terminalizing", "settled"}
  /\ cState \in {"none", "admitted", "ready", "input-applied", "terminalizing", "settled"}
  /\ resvSt \in {"none", "reserved", "releasePending", "released"}
  /\ freezeKind \in {"none", "join", "orphan"}
  /\ pResv \in {"none", "completed", "aborted"}
  /\ pSettRec \in {"none", "completed", "aborted"}
  /\ cResv \in {"none", "completed", "aborted"}
  /\ cSettRec \in {"none", "completed", "aborted"}
  /\ faults \in 0..MaxFaults
  /\ routeFaults \in 0..MaxRouteFaults

\* SUB-016: the deterministic child idempotency key admits at most ONE child
\* per parent Tool Call across every establishment replay and crash.
OneChildPerToolCall == cAdmits <= 1

\* SUB-019/SUB-023: the parent join batch commits only against the child's
\* CANONICAL Settlement, exactly once, and only through a canonical lineage
\* and start link (fail-closed verification).
JoinRequiresChildSettlement ==
  /\ joinRec => (cSettRec # "none" /\ startRec /\ lineageRec)
  /\ joinCount <= 1

\* Spec SS12 step 6 / SS14: the reservation's frozen accounting decision is
\* applied exactly once; release only follows a freeze; the freeze is either
\* the canonical join or the zero-consumed decision for a provably childless
\* reservation under abort -- budget is never available twice.
ReservationConservation ==
  /\ releaseCount <= 1
  /\ resvSt = "released" => (releaseCount = 1 /\ freezeKind # "none")
  /\ resvSt \in {"none", "reserved"} => (releaseCount = 0 /\ freezeKind = "none")
  /\ freezeKind = "join" => joinRec
  /\ freezeKind = "orphan" => (cState = "none" /\ pAbort)

\* SS13.1 request-abort-and-join: the parent NEVER settles -- aborted or
\* otherwise -- while an established child is unjoined or its reservation
\* release is incomplete; exactly one canonical settlement each.
AbortJoinsBeforeParentSettles ==
  /\ pSettRec # "none" => ((cState = "none" \/ joinRec)
                            /\ resvSt \in {"none", "released"})
  /\ pSettAppends <= 1
  /\ cSettAppends <= 1
  /\ pSettRec # "none" => pResv = pSettRec
  /\ cSettRec # "none" => cResv = cSettRec

\* SS12 establishment ladder order: reservation before request, request
\* before admission, lineage before the start link.
EstablishmentOrder ==
  /\ reqRec => resvSt # "none"
  /\ cState # "none" => reqRec
  /\ startRec => (lineageRec /\ cState # "none")

\* Plan SS7(a): a child never runs a Turn before its immutable lineage record
\* is canonical.  VIOLATED under the current discipline
\* (SubagentEstablishmentRace.cfg); HOLDS under AwaitParentEstablishment
\* (SubagentEstablishmentFix.cfg).
ChildTurnRequiresLineage == ~ranBeforeLineage

-----------------------------------------------------------------------------
(***************************************************************************)
(* Liveness, under the documented assumptions: weak fairness of the parent  *)
(* worker, child worker, and recovery; bounded crash and routed-call fault  *)
(* budgets (the authoritative admission owner eventually answers            *)
(* definitively, and a lost child-settled wake is eventually replayed by    *)
(* recovery -- SUB-031 and spec SS12 step 10).                              *)
(***************************************************************************)

ParentEventuallySettles == <>(pState = "settled")

ChildEventuallySettles ==
  (cState = "admitted") ~> (cState = "settled")

=============================================================================
