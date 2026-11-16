# Handling in-progress packets during draft upgrade

This guide explains how to handle **packets that are in progress** when the draft changes are deployed, so that they complete with the new draft flow. It covers the steps to follow around the deployment, which packets to restart, and how to validate the result.

## 1. Why in-progress packets need handling
With the draft changes, the ID Repo draft is handled differently:

| | Before the draft changes | After the draft changes |
|---|---|---|
| Stage that creates the draft | UIN Generator (after dedupe and manual adjudication) | **Create Draft**, right after Quality Classifier |
| ID Repo draft APIs | `/identity/draft/...` (v1) | `/identity/draft/v2/...` |
| Draft storage location | Old location | New location |

A packet that is in progress when the draft changes are deployed either has a draft in the old location, or has no draft while its next stage now expects one. If it simply continues from where it stopped, **it fails**.

To fix this, such packets are restarted from the securezone stage. They then pass through **Create Draft**, which deletes any old draft and creates a new one in the new location.

## 2. Rule for in-progress packets
| Packet | Action | Done by |
|---|---|---|
| **After Quality Classifier and not yet published** to ID Repo (before Finalization completes) | **Restart from securezone**, so a new draft is created | `reprocess.py` |
| At Packet Receiver, or **failed** before Create Draft | Restart from securezone (the Reprocessor does not pick these up) | `reprocess.py` |
| **Up to Quality Classifier** (before Create Draft) | Resume from its current stage. It passes Create Draft normally. | Reprocessor |
| **Already published** (Finalization completed, Credential Requestor, Message Sender, Printing) | Resume from its current stage. No draft is needed. | Reprocessor |
| **PAUSED**: stopped by a pause rule for a set time | No action. In the default configuration, packets are paused after Packet Classifier, which is **before** Create Draft. When the pause ends, the workflow manager job applies the pause rule's resume action:<br>- `RESUME_PROCESSING`: the packet becomes RESUMABLE and the Reprocessor continues it from where it paused.<br>- `RESUME_FROM_BEGINNING`: the packet restarts from Packet Uploader.<br>- `STOP_PROCESSING`: the packet ends as REJECTED (default rule for a hotlisted operator).<br>In the first two cases the packet passes Create Draft and gets a new draft. | Workflow manager job, then Reprocessor |
| **PAUSED_FOR_ADDITIONAL_INFO**: waiting for a correction packet | No action. In the default configuration, additional information is requested only at Quality Classifier, which is **before** Create Draft. When the correction packet is processed, the workflow manager marks the original packet RESUMABLE and the Reprocessor continues it from where it paused, so its next stage is Create Draft and it gets a new draft. If no correction packet arrives within the wait time set in the camel route (`PAUSE_FOR`, default 15 days), the workflow manager job marks the packet REJECTED. | Workflow manager, then Reprocessor |
| Finished (PROCESSED, FAILED, REJECTED) | None | — |

## 3. Role of each component during the change
| Component | Role | State during the script run |
|---|---|---|
| Packet receiver / sync | Accepts new packets | **Stopped** |
| Registration Processor stages | Process packets | Running, with the draft changes |
| Kafka (Registration Processor topics) | Carries packets between stages | **Cleared** before deployment.<br><br>**Why:** if the topics still have messages, the stages pick them up as soon as the new version starts, and those packets continue from the middle of the flow (their old stage). At the same time, the script restarts the same packets from the beginning (securezone). The same RID is then processed twice in parallel, which can move the packet to a FAILED state. Messages from the old flow also skip Create Draft, so the packet would have no new draft.<br><br>After clearing, each packet is started from one place only: the script or the Reprocessor. |
| Reprocessor | Resumes packets from their current stage | **Stopped**, started after the script |
| Workflow manager (including `WorkflowActionJob`) | Completes packets (processed, failed, rejected), pauses them, and resumes PAUSED packets when the pause ends | **Running**, with the draft changes. Stopped only while Kafka is cleared (steps 3–7). |
| ID Repo | Stores drafts (v2 APIs) | Running, with the draft changes |
| `reprocess.py` | Restarts the selected packets from securezone | Runs |

Only the Reprocessor must stay stopped until the script has finished. Otherwise it may resume a packet from its old stage before the script restarts it.

The workflow manager can run during the script: when a pause ends, it only marks the packet RESUMABLE in the database and sends nothing. The Reprocessor sends RESUMABLE packets on, and it is stopped. The workflow manager is also needed to complete the restarted packets.

## 4. Steps
Choose the approach from the value of `securezone.routing.enabled` in the deployed `registration-processor-default.properties` (mosip-config). If the property is not set, the value is `true`.

| Approach | When |
|---|---|
| **Approach 1** | `securezone.routing.enabled=true` (default) |
| **Approach 2** | `securezone.routing.enabled=false` |

### Approach 1: securezone routing enabled (default)
#### Before the maintenance window
1. **Choose the approach**: check `securezone.routing.enabled` in the deployed `registration-processor-default.properties` (not set means `true`). Use Approach 1 for `true` and Approach 2 for `false`.
2. **Review customisations** against section 5 and adjust the query if needed.
3. **Choose `FROM_TS`**: the oldest last-activity time (`latest_trn_dtimes`) of packets to restart. Base it on the longest time a packet can stay in the pipeline in your deployment (for example while waiting for manual adjudication). If a packet can stay up to 2 months, set `FROM_TS` to 2 months before the deployment, so those packets are also restarted from securezone. Packets with no activity since before `FROM_TS` are not picked, and the Reprocessor would resume them without a new draft. So set `FROM_TS` at or before the result of this query, which shows the oldest in-progress packet:
   ```sql
   SELECT min(latest_trn_dtimes) FROM regprc.registration
   WHERE status_code IN ('PROCESSING', 'REPROCESS', 'RESUMABLE');
   ```
4. **Validate in a lower environment**: on the old version, keep packets stuck at different stages (before Create Draft, after Quality Classifier, at Finalization, PAUSED). Then follow all the steps in section 4: deploy the draft changes and run the utility. Check that every packet reaches PROCESSED and that the restarted ones have a new draft. Include a packet that was already sent to ABIS (at or after Bio Dedupe): after the restart, the ABIS must accept the new request with a new reference ID, and the packet must pass Bio Dedupe without matching its own earlier entry. Use the same `securezone.routing.enabled` value as production, so that the chosen approach is the one validated.
5. **Install the script** on a machine that can reach the API gateway and the `mosip_regprc` database (see the **Prerequisites** section of the reprocess script README).

#### During the maintenance window
| # | Step | Check |
|---|---|---|
| 1 | **Stop traffic**: block packet upload and sync, or scale down the packet receiver. | No new rows in `regprc.registration` |
| 2 | **Stop the Reprocessor.** | Pod stopped |
| 3 | **Stop all Registration Processor stages and the workflow manager** (both consume from Kafka). | No consumers on the regproc Kafka consumer groups |
| 4 | **Record `CUTOVER_TS`**: the date and time at which steps 1–3 were completed, i.e. when traffic and all stages were stopped, in the same time zone as `latest_trn_dtimes` (normally UTC), e.g. `2026-11-20 18:30:00`. The query restarts only packets last updated before this time. Keep the same value for all runs. | Value noted |
| 5 | **Save the "before" counts**: the number of in-progress packets per stage and status, taken now that everything is stopped and before anything changes. Run the query in section 7 and keep the result. It is used to check the query (section 6) and to compare after the run (section 7). | Result saved |
| 6 | **Clear the Registration Processor Kafka topics** (delete the messages, or reset the consumer group offsets to latest). | Lag = 0 |
| 7 | **Deploy the draft changes** (code and mosip-config) as described in the release notes. Start the stages and the workflow manager. Keep the Reprocessor stopped. | Pods healthy |
| 8 | **Prepare the query** in `config.py`: replace `FROM_TS` and `CUTOVER_TS`, and apply any customisation from section 5. | — |
| 9 | **Run the checks** in section 6, including the dry run. | Rows as expected |
| 10 | **Run the script**: `./reprocess.py --db --loop` | Exit code 0 (or 2, see the reprocess script README) |
| 11 | **Start the Reprocessor.** | Running |
| 12 | **Allow traffic** again. | New packets received |
| 13 | **Validate** (section 7). | All checks pass |

### Approach 2: securezone routing disabled
Follow all the steps of Approach 1. Only the points below differ.

**How the flag changes processing:** when the script calls securezone, securezone marks the packet SUCCESS at `SecurezoneNotificationStage` in both cases.
- **Enabled:** securezone also sends the packet to the next stage, so it is processed immediately, during the script run.
- **Disabled:** securezone does not send it. The packet waits at securezone SUCCESS, and the Reprocessor sends it on once it is started (step 11) and the packet has been idle for `registration.processor.reprocess.elapse.time` (default 15 minutes).

**What differs when disabled:**

| Item | Difference |
|---|---|
| Steps | None. The Reprocessor stays off until step 11, as for enabled routing. |
| Query | None (see below). |
| When restarted packets move | Only after step 11, in Reprocessor batches (`registration.processor.reprocess.fetchsize`, default 2500, every few minutes). |
| End-to-end check (section 6) and "During the run" check (section 7) | Use the Approach 2 checks there: packets wait at securezone `SUCCESS` until step 11. `CREATE_DRAFT` rows appear only after the Reprocessor has started. |
| "After the run" and final validation (section 7) | Same checks; allow more time, because packets move in Reprocessor batches. |
| Retry limit | Packets whose `reg_process_retry_count` is above `registration.processor.reprocess.attempt.count` (default 20) are not picked by the Reprocessor and stay at securezone SUCCESS. Handle them as in section 8. |

**Why the same query works:** `SecurezoneNotificationStage` is in the query's last list, so packets at securezone with SUCCESS are never picked by the script; they are left to the Reprocessor. This covers packets that were already waiting at securezone before the deployment and packets the script has just restarted. A restarted packet is also excluded by `CUTOVER_TS`, because securezone sets its `latest_trn_dtimes` to the current time.

## 5. Query and customisation
This is the default `query` in `config.py`. Replace `FROM_TS` and `CUTOVER_TS`, and change it only if the country has customised the workflow (see **Customisation** below):
```sql
SELECT reg_id, process, workflow_instance_id, iteration, source
FROM regprc.registration
WHERE status_code IN ('PROCESSING', 'REPROCESS', 'RESUMABLE')
  AND latest_trn_dtimes >= 'FROM_TS'
  AND latest_trn_dtimes <  'CUTOVER_TS'
  AND reg_stage_name NOT IN ('CredentialRequestorStage', 'MessageSenderStage', 'PrintingStage')
  AND NOT (reg_stage_name = 'FinalizationStage' AND latest_trn_status_code = 'SUCCESS')
  AND (reg_stage_name NOT IN ('SecurezoneNotificationStage', 'PacketUploaderStage', 'PacketValidatorStage',
                              'PacketClassifierStage', 'CMDValidatorStage', 'OperatorValidatorStage',
                              'SupervisorValidatorStage', 'IntroducerValidatorStage', 'QualityClassifierStage')
       OR latest_trn_status_code NOT IN ('SUCCESS', 'REPROCESS', 'IN_PROGRESS'))
LIMIT 1000
```

| Condition | Meaning |
|---|---|
| `status_code IN (...)` | Only packets in progress. Finished and paused packets are left out. |
| `latest_trn_dtimes` range | Only packets from before the deployment, and not older than `FROM_TS` (see preparation step 3). `CUTOVER_TS` must stay the same for all runs: a restarted packet gets a newer time, so it is not picked again. |
| First `NOT IN` and `NOT (... FinalizationStage ...)` | Leaves out published packets. The Reprocessor completes them. |
| Last condition | Leaves out packets up to Quality Classifier: the Reprocessor resumes them. **Except failed ones** (`latest_trn_status_code` FAILED / ERROR / REJECTED), which the script restarts. When a stage fails a packet, it sends a message through Kafka to the workflow manager, which sets the final status (`status_code` FAILED or REJECTED). That message was lost when Kafka was cleared, so the packet stays in progress, and the Reprocessor does not pick failed packets. The script restarts it; it is processed again, and either continues, or fails again and this time the workflow manager sets the final status. |
| `LIMIT 1000` | Batch size. `--loop` repeats until no rows are left. |

`PacketReceiverStage` is intentionally **not** in the last list. By default (`mosip.registration.processor.reprocessor.exclude-stage-names`), the Reprocessor never picks up packets at that stage, so the script must restart them.

A stage that is not in any list (for example a custom stage) is **restarted by default**. This is the safe choice for a packet that is not yet published.

### Customisation
The query matches the default camel routes and properties. If the country has changed them, check the **deployed** configuration:

| Customisation | Change in the query |
|---|---|
| Custom stage **after Finalization** | Add it to the published-stages list (first `NOT IN`). |
| Custom stage **before Create Draft** | Optional: add it to the last list so the Reprocessor resumes it. If not added, it is restarted, which is also safe. **Exception:** if the stage is in `mosip.registration.processor.reprocessor.exclude-stage-names`, do not add it. The Reprocessor never picks up that stage, so the packets would be skipped by both the script and the Reprocessor and stay stuck. |
| Custom stage **between Create Draft and Finalization** | No change. It is restarted. |
| Create Draft placed **earlier** in a route than in the default (e.g. right after Packet Validator) | **Required**: remove from the last list the stages that now come after Create Draft. Example: if Create Draft is after Packet Validator, remove `PacketClassifierStage`, `CMDValidatorStage`, `OperatorValidatorStage`, `SupervisorValidatorStage`, `IntroducerValidatorStage` and `QualityClassifierStage`. Otherwise packets at those stages are left to the Reprocessor, which resumes them past Create Draft, so they get no new draft. |
| `mosip.registration.processor.reprocessor.exclude-stage-names` changed | **Required**: remove those stages from the last list. The Reprocessor never picks them up. |
| **Pause rule after Create Draft** (`mosip.regproc.camelbridge.pause-settings`). By default, packets pause before Create Draft, so no action is needed. | A packet paused at a stage after Create Draft has no draft in the new location. When its pause ends, it continues from that stage (`RESUME_PROCESSING`), never passes Create Draft, and fails. **Required**: before step 7 (when the workflow manager starts again), find the PAUSED packets whose `reg_stage_name` is after Create Draft and set their `default_resume_action` to `RESUME_FROM_BEGINNING`. They then restart from the beginning when the pause ends, pass Create Draft and get a new draft. Test this in a lower environment first. |
| **Additional-info request after Create Draft** (`pause-and-request-additional-info` in a camel route). By default, it is requested only at Quality Classifier, before Create Draft, so no action is needed. | A packet waiting at a stage after Create Draft has no draft in the new location. When the correction packet arrives, it always continues from that stage (this cannot be changed by configuration), never passes Create Draft, and fails. **Required**: before step 7 (when the workflow manager starts again), note the PAUSED_FOR_ADDITIONAL_INFO packets whose `reg_stage_name` is after Create Draft. If they fail after resuming, restart them from securezone with `./reprocess.py --registration "<rid>, <process>, <workflow_instance_id>, <iteration>, <source>"`. Test this in a lower environment first. |

Note: `reg_stage_name` holds the stage class name (e.g. `PacketUploaderStage`), not the transaction type code (e.g. `UPLOAD_PACKET`).

### Configuration to check
These properties are not part of the query. They decide **the stage a packet starts from when it is restarted**. That stage must come **before** Create Draft, so the restarted packet gets a new draft. The defaults are correct; act only if the country has changed them.

| Property | What it does | Default | Action |
|---|---|---|---|
| `registration.processor.reprocess.restart-from-stage` | Stage from which the **Reprocessor** restarts a packet, instead of resuming it, when the packet matches `registration.processor.reprocess.restart-trigger-filter` (for example a packet stuck at Bio Dedupe). | `SecurezoneNotificationStage` | If it is set to a stage after Create Draft, change it to a stage before Create Draft. |
| `mosip.regproc.workflow-manager.action.resumefrombeginning.stage` | Stage from which the **workflow manager** restarts a packet for the `RESUME_FROM_BEGINNING` action (Workflow Action API, or a paused packet whose resume action is `RESUME_FROM_BEGINNING`). | `PacketUploaderStage` | If it is set to a stage after Create Draft, change it to a stage before Create Draft. |

### What the country must be aware of
- **Dedupe runs again** for restarted packets. A packet needs a manual adjudication decision again only if dedupe finds a match again, even if it already had a decision.
- **NEW packets that had an old draft get a new UIN.** The UIN in the old draft was never published.
- **Some UPDATE packets may end as FAILED (stale packet)** when a newer packet for the same UIN was already processed.
- **ABIS:** restarted packets that were already sent to ABIS are sent again with a new reference ID. The ABIS must accept it. The old entry stays in ABIS, and late responses to old requests may still arrive.
- **Resuming a PAUSED packet manually** through the Workflow Action API (this API accepts only PAUSED packets) for a packet received before the deployment:
  - Paused **before** Create Draft (the default): `RESUME_PROCESSING` or `RESUME_FROM_BEGINNING`. Both pass Create Draft.
  - Paused **after** Create Draft (custom pause rule): use `RESUME_FROM_BEGINNING`, so the packet restarts and passes Create Draft. `RESUME_PROCESSING` would continue it from its current stage without a new draft, and it would fail.
- **Old drafts** remain in the old storage location. No clean-up is required.
- **Published packets that failed at their last stage:** a packet already published to ID Repo (at Finalization or a later stage) whose last stage failed just before the deployment. The message that would have marked it FAILED in the workflow was cleared from Kafka, so it stays in progress. The script does not pick it (it is already published), and the Reprocessor does not pick failed packets. Such packets are rare. They appear in the "After the run" query and need to be handled one by one.

## 6. Checks before the bulk run
| Check | How | Expected |
|---|---|---|
| Query is valid | `./reprocess.py --db --dry-run` | Rows printed, no error |
| Number of packets to restart | Run the query with `count(*)` and without `LIMIT` | You know how many packets the script will send before starting. Use it to estimate the run time (about count × `delay` seconds) and to spot a wrong query: for example 0, although the "before" counts (step 5) show packets at stages after Quality Classifier (e.g. `DemoDedupeStage`, `BioDedupeStage`, `ManualAdjudicationStage`). |
| Query uses an index | `ANALYZE regprc.registration;` then `EXPLAIN (ANALYZE, BUFFERS) <query>` | No `Seq Scan on registration` |
| End-to-end on one packet | `./reprocess.py --registration "<rid>, <process>, <workflow_instance_id>, <iteration>, <source>"` for a packet after Quality Classifier | **Approach 1:** a `CREATE_DRAFT` row with `SUCCESS` appears in `regprc.registration_transaction` for the RID, and the packet reaches PROCESSED unless it needs manual adjudication. **Approach 2:** the packet is at `SecurezoneNotificationStage` with `SUCCESS`. It moves on, and the `CREATE_DRAFT` row appears, only after the Reprocessor starts (step 11). |
| Services are up | Registration Processor and ID Repo pods | Healthy |

## 7. Monitoring and validation
**During the run:**
- Follow `out.log` (one line per packet with `SUCCESS` or `FAILED`).
- **Approach 1:** restarted packets reach Create Draft during the run. Check the results:
  ```sql
  SELECT status_code, count(*) FROM regprc.registration_transaction
  WHERE trn_type_code = 'CREATE_DRAFT' AND cr_dtimes >= 'CUTOVER_TS'
  GROUP BY 1;
  ```
  Expected: mostly `SUCCESS`, and the count grows during the run.
- **Approach 2:** restarted packets wait at securezone until the Reprocessor starts (step 11). Check that they are waiting there:
  ```sql
  SELECT count(*) FROM regprc.registration
  WHERE reg_stage_name = 'SecurezoneNotificationStage'
    AND latest_trn_status_code = 'SUCCESS'
    AND latest_trn_dtimes >= 'CUTOVER_TS';
  ```
  Expected: about the number of packets the script reported as `SUCCESS`. After step 11 this count goes down, and the Approach 1 query starts showing `CREATE_DRAFT` rows.

**"Before" counts** (step 5): in-progress packets per stage and status before the deployment, in the same time range as the script (from `FROM_TS`). Keep the result.
```sql
SELECT reg_stage_name, status_code, latest_trn_status_code, count(*)
FROM regprc.registration
WHERE status_code NOT IN ('PROCESSED', 'FAILED', 'REJECTED')
  AND latest_trn_dtimes >= 'FROM_TS'
GROUP BY 1, 2, 3 ORDER BY 1;
```
No `CUTOVER_TS` condition is needed here: everything is stopped at step 5, so every packet is before `CUTOVER_TS`.

**After the run**: shows packets from before the deployment that have not moved yet. The Reprocessor picks packets in batches every few minutes, so run this query a few times: the count should go down to 0.
```sql
-- Expected: 0 rows.
SELECT reg_stage_name, status_code, latest_trn_status_code, count(*)
FROM regprc.registration
WHERE status_code IN ('PROCESSING', 'REPROCESS', 'RESUMABLE')
  AND latest_trn_dtimes >= 'FROM_TS'
  AND latest_trn_dtimes <  'CUTOVER_TS'
GROUP BY 1, 2, 3;
```
A packet that was restarted or resumed gets a `latest_trn_dtimes` after `CUTOVER_TS`, so it no longer appears here. PAUSED packets are not included: they move only when their pause ends.

**Final validation:** the upgrade is complete when all four are true.

1. **The script sent every packet.** Exit code is `0`.
   If it is `2`, rerun the failed packets: `./reprocess.py --file failed_<ts>.txt`.
2. **No old packet is stuck.** The "After the run" query above returns 0 rows.
3. **Old packets completed correctly.**
   - **What we check:** the final status of the old packets that were restarted or resumed. Point 2 only shows packets that were not picked up; a packet that was picked up and then failed (e.g. at Create Draft) is visible only here.
   - **Query:** packets received before the deployment that have moved since, grouped by status (`cr_dtimes` is in UTC, like `latest_trn_dtimes`):
     ```sql
     SELECT status_code, count(*) FROM regprc.registration
     WHERE cr_dtimes < 'CUTOVER_TS' AND latest_trn_dtimes >= 'CUTOVER_TS'
     GROUP BY 1;
     ```
   - **Expected:** most are PROCESSED. A few FAILED / REJECTED are normal (see section 5, "What the country must be aware of"). PROCESSING / REPROCESS / RESUMABLE are still running: check again later. Many FAILED means the draft flow is not working.
4. **New packets work.** One new test packet reaches PROCESSED.

If any check fails, see section 8.

## 8. Troubleshooting
**Script errors** (exit codes, `RUN STOPPED` messages, failed packets, `failed_<ts>.txt` and `remaining_<ts>.txt`): see the **Error handling** and **Exit codes** sections of the reprocess script README.

**Upgrade issues:**

| Symptom | Cause | Action |
|---|---|---|
| `invalid input syntax for type timestamp: "FROM_TS"` | Placeholders not replaced | Do step 8 |
| Query is slow | Missing statistics or index | `ANALYZE regprc.registration;`. If still slow, add a temporary index on `(status_code, latest_trn_dtimes)` and drop it after the run |
| Packets still in progress after the run, count no longer going down | Reprocessor not running; packet status not picked by the Reprocessor; or `reg_process_retry_count` already above `registration.processor.reprocess.attempt.count` (default 20), so the Reprocessor skips it | Check step 11. For packets over the retry limit, restart them with `./reprocess.py --file` |
