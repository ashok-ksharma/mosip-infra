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
| **PAUSED**: stopped by a pause rule for a set time | No action. In the default configuration, packets are paused after Packet Classifier, which is **before** Create Draft. When the pause time ends, the workflow manager job marks the packet RESUMABLE and the Reprocessor continues it. It then passes Create Draft and gets a new draft. | Workflow manager job, then Reprocessor |
| **PAUSED_FOR_ADDITIONAL_INFO**: waiting for a correction packet | No action. In the default configuration, additional information is requested only at Quality Classifier, which is **before** Create Draft. When the correction packet is processed, the workflow manager marks the original packet RESUMABLE and the Reprocessor continues it from where it paused, so its next stage is Create Draft and it gets a new draft. If no correction packet arrives in time, the workflow manager job applies the packet's default resume action. | Workflow manager, then Reprocessor |
| Finished (PROCESSED, FAILED, REJECTED) | None | — |

## 3. Role of each component during the change
| Component | Role | State during the script run |
|---|---|---|
| Packet receiver / sync | Accepts new packets | **Stopped** |
| Registration Processor stages | Process packets | Running, with the draft changes |
| Kafka (Registration Processor topics) | Carries packets between stages | **Cleared** before deployment.<br><br>**Why:** if the topics still have messages, the stages pick them up as soon as the new version starts, and those packets continue from the middle of the flow (their old stage). At the same time, the script restarts the same packets from the beginning (securezone). The same RID is then processed twice in parallel, which can move the packet to a FAILED state. Messages from the old flow also skip Create Draft, so the packet would have no new draft.<br><br>After clearing, each packet is started from one place only: the script or the Reprocessor. |
| Reprocessor | Resumes packets from their current stage | **Stopped**, started after the script |
| Workflow manager job (`WorkflowActionJob`) | Resumes PAUSED packets | **Stopped**, started after the script |
| ID Repo | Stores drafts (v2 APIs) | Running, with the draft changes |
| `reprocess.py` | Restarts the selected packets from securezone | Runs |

The Reprocessor and the workflow manager job must stay stopped until the script has finished. Otherwise they may resume a packet from its old stage before the script restarts it.

## 4. Steps
### Before the maintenance window
1. **Review customisations** against section 5 and adjust the query if needed.
2. **Choose `FROM_TS`**: the oldest last-activity time (`latest_trn_dtimes`) of packets to restart. Base it on the longest time a packet can stay in the pipeline in your deployment (for example while waiting for manual adjudication). If a packet can stay up to 2 months, set `FROM_TS` to 2 months before the deployment, so those packets are also restarted from securezone. Packets with no activity since before `FROM_TS` are not picked. To see how far back in-progress packets go:
   ```sql
   SELECT min(latest_trn_dtimes) FROM regprc.registration
   WHERE status_code IN ('PROCESSING', 'REPROCESS', 'RESUMABLE');
   ```
3. **Validate in a lower environment**: on the old version, keep packets stuck at different stages (before Create Draft, after Quality Classifier, at Finalization, PAUSED). Then follow all the steps in section 4: deploy the draft changes and run the utility. Check that every packet reaches PROCESSED and that the restarted ones have a new draft.
4. **Install the script** on a machine that can reach the API gateway and the `mosip_regprc` database (see the **Prerequisites** section of the reprocess script README).

### During the maintenance window
| # | Step | Check |
|---|---|---|
| 1 | **Stop traffic**: block packet upload and sync, or scale down the packet receiver. | No new rows in `regprc.registration` |
| 2 | **Stop the Reprocessor** and the **workflow manager job**. | Pods stopped |
| 3 | **Stop all Registration Processor stages.** | No consumers on the regproc Kafka consumer groups |
| 4 | **Record `CUTOVER_TS`**: the date and time at which steps 1–3 were completed, i.e. when traffic and all stages were stopped, in the same time zone as `latest_trn_dtimes` (normally UTC), e.g. `2026-11-20 18:30:00`. The query restarts only packets last updated before this time. Keep the same value for all runs. | Value noted |
| 5 | **Save the "before" counts**: the number of in-progress packets per stage and status, taken now that everything is stopped and before anything changes. Run the query in section 7 and keep the result. It is used to check the query (section 6) and to compare after the run (section 7). | Result saved |
| 6 | **Clear the Registration Processor Kafka topics** (delete the messages, or reset the consumer group offsets to latest). | Lag = 0 |
| 7 | **Deploy the draft changes** (code and mosip-config) as described in the release notes. Keep the Reprocessor and workflow manager job stopped. | Pods healthy |
| 8 | **Set the query** in `config.py` (section 5), replacing `FROM_TS` and `CUTOVER_TS`. | — |
| 9 | **Run the checks** in section 6, including the dry run. | Rows as expected |
| 10 | **Run the script**: `./reprocess.py --db --loop` | Exit code 0 (or 2, see section 8) |
| 11 | **Start the Reprocessor** and the **workflow manager job**. | Running |
| 12 | **Allow traffic** again. | New packets received |
| 13 | **Validate** (section 7). | All checks pass |

## 5. Query and customisation
Set this as `query` in `config.py`:
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
| `latest_trn_dtimes` range | Only packets from before the deployment, and not older than `FROM_TS` (see preparation step 2). `CUTOVER_TS` must stay the same for all runs: a restarted packet gets a newer time, so it is not picked again. |
| First `NOT IN` and `NOT (... FinalizationStage ...)` | Leaves out published packets. The Reprocessor completes them. |
| Last condition | Leaves out packets up to Quality Classifier: the Reprocessor resumes them. **Except failed ones** (`latest_trn_status_code` FAILED / ERROR / REJECTED), which the script restarts. When a stage fails a packet, it sends a message through Kafka to the workflow manager, which sets the final status (`status_code` FAILED or REJECTED). That message was lost when Kafka was cleared, so the packet stays in progress, and the Reprocessor does not pick failed packets. The script restarts it; it fails again at the same check, and this time the workflow manager sets the final status. |
| `LIMIT 1000` | Batch size. `--loop` repeats until no rows are left. |

`PacketReceiverStage` is intentionally **not** in the last list. The Reprocessor never picks up packets at that stage, so the script must restart them.

A stage that is not in any list (for example a custom stage) is **restarted by default**. This is the safe choice for a packet that is not yet published.

### Customisation
The query matches the default camel routes and properties. If the country has changed them, check the **deployed** configuration:

| Customisation | Change in the query |
|---|---|
| Custom stage **after Finalization** | Add it to the published-stages list (first `NOT IN`). |
| Custom stage **before Create Draft** | Optional: add it to the last list so the Reprocessor resumes it. If not added, it is restarted, which is also safe. |
| Custom stage **between Create Draft and Finalization** | No change. It is restarted. |
| Create Draft placed **earlier** in a route than in the default (e.g. right after Packet Validator) | **Required**: remove from the last list the stages that now come after Create Draft. Example: if Create Draft is after Packet Validator, remove `PacketClassifierStage`, `CMDValidatorStage`, `OperatorValidatorStage`, `SupervisorValidatorStage`, `IntroducerValidatorStage` and `QualityClassifierStage`. Otherwise packets at those stages are left to the Reprocessor, which resumes them past Create Draft, so they get no new draft. |
| `mosip.registration.processor.reprocessor.exclude-stage-names` changed | **Required**: remove those stages from the last list. The Reprocessor never picks them up. |
| **Pause rule after Create Draft** (`mosip.regproc.camelbridge.pause-settings`). By default, packets pause before Create Draft, so no action is needed. | A packet paused at a stage after Create Draft has no draft in the new location. When its pause ends, it continues from that stage (`RESUME_PROCESSING`), never passes Create Draft, and fails. **Required**: before step 11, find the PAUSED packets whose `reg_stage_name` is after Create Draft and set their `default_resume_action` to `RESUME_FROM_BEGINNING`. They then restart from the beginning when the pause ends, pass Create Draft and get a new draft. Test this in a lower environment first. |
| **Additional-info request after Create Draft** (`pause-and-request-additional-info` in a camel route). By default, it is requested only at Quality Classifier, before Create Draft, so no action is needed. | A packet waiting at a stage after Create Draft has no draft in the new location. When the correction packet arrives, it always continues from that stage (this cannot be changed by configuration), never passes Create Draft, and fails. **Required**: before step 11, note the PAUSED_FOR_ADDITIONAL_INFO packets whose `reg_stage_name` is after Create Draft. If they fail after resuming, restart them from securezone with `./reprocess.py --registration "<rid>, <process>, <workflow_instance_id>, <iteration>, <source>"`. |

Note: `reg_stage_name` holds the stage class name (e.g. `PacketUploaderStage`), not the transaction type code (e.g. `UPLOAD_PACKET`).

### Configuration to check
These properties are not part of the query. They decide **the stage a packet starts from when it is restarted**. That stage must come **before** Create Draft, so the restarted packet gets a new draft. The defaults are correct; act only if the country has changed them.

| Property | What it does | Default | Action |
|---|---|---|---|
| `registration.processor.reprocess.restart-from-stage` | Stage from which the **Reprocessor** restarts a packet, instead of resuming it, when the packet matches `registration.processor.reprocess.restart-trigger-filter` (for example a packet stuck at Bio Dedupe). | `SecurezoneNotificationStage` | If it is set to a stage after Create Draft, change it to a stage before Create Draft. |
| `mosip.regproc.workflow-manager.action.resumefrombeginning.stage` | Stage from which the **workflow manager** restarts a packet for the `RESUME_FROM_BEGINNING` action (Workflow Action API, or a paused packet whose resume action is `RESUME_FROM_BEGINNING`). | `PacketUploaderStage` | If it is set to a stage after Create Draft, change it to a stage before Create Draft. |

### What the country must be aware of
- **Dedupe and manual adjudication run again** for restarted packets. Packets that were waiting for, or already had, a manual adjudication decision need a new decision.
- **NEW packets that had an old draft get a new UIN.** The UIN in the old draft was never published.
- **Some UPDATE packets may end as FAILED (stale packet)** when a newer packet for the same UIN was already processed.
- **Late ABIS responses** to old requests may still arrive for restarted packets.
- **Resuming a PAUSED packet manually** through the Workflow Action API (this API accepts only PAUSED packets) for a packet received before the deployment:
  - Paused **before** Create Draft (the default): `RESUME_PROCESSING` or `RESUME_FROM_BEGINNING`. Both pass Create Draft.
  - Paused **after** Create Draft (custom pause rule): use `RESUME_FROM_BEGINNING`, so the packet restarts and passes Create Draft. `RESUME_PROCESSING` would continue it from its current stage without a new draft, and it would fail.
- **Old drafts** remain in the old storage location. No clean-up is required.

## 6. Checks before the bulk run
| Check | How | Expected |
|---|---|---|
| Query is valid | `./reprocess.py --db --dry-run` | Rows printed, no error |
| Number of packets to restart | Run the query with `count(*)` and without `LIMIT` | You know how many packets the script will send before starting. Use it to estimate the run time (about count × `delay` seconds) and to spot a wrong query: for example 0, although the "before" counts (step 5) show packets at stages after Quality Classifier (e.g. `DemoDedupeStage`, `BioDedupeStage`, `ManualAdjudicationStage`). |
| Query uses an index | `ANALYZE regprc.registration;` then `EXPLAIN (ANALYZE, BUFFERS) <query>` | No `Seq Scan on registration` |
| End-to-end on one packet | `./reprocess.py --registration "<rid>, <process>, <workflow_instance_id>, <iteration>, <source>"` for a packet after Quality Classifier | Packet reaches PROCESSED with a new draft |
| Services are up | Registration Processor and ID Repo pods | Healthy |

## 7. Monitoring and validation
**During the run:**
- Follow `out.log` (one line per packet with `SUCCESS` or `FAILED`).
- Check a few restarted RIDs in `regprc.registration_transaction`: a `CREATE_DRAFT` row with `SUCCESS` should appear.

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
   - **Query:** packets received before the deployment that have moved since, grouped by status:
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
