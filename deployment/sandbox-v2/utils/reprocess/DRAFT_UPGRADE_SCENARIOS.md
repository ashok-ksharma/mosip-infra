# Draft upgrade test scenarios

Scenarios to run in a lower environment to confirm that in-progress packets are handled correctly during the draft upgrade. All packets are prepared in **one session on the old version**, then the environment is upgraded **once** and all packets are checked together.

## How it works
**To make a packet stuck at a stage:** stop the pod group that runs the **next** stage, then send the packet. The packet completes the stages before it and waits in Kafka for the stopped group. When the upgrade clears Kafka, that waiting message is removed, so the packet is stuck exactly as in production.

Example: stop group 4 (Demo Dedupe) and send a packet. It passes Quality Classifier, then waits because group 4 is stopped.

**Why the scenarios start from the last stage:** all packets are prepared in one session, so stopped groups add up.
- Start with the scenario that needs the packet to travel furthest, while all groups are running.
- For each next scenario, stop a group earlier in the flow. The new packet stops there, and the packets already stuck further on stay where they are.
- If you went the other way (an early group first), later packets could never get past it.

**Rules:**
- Do not start a stopped group again before the upgrade. Its waiting packets would move on and the scenario would be lost.
- Note the RID of each packet with its scenario number, to check it after the upgrade.

### Stage groups
This is the stage grouping in the **new** version. In the **old** version, group 7 also runs **UIN Generator**, and Create Draft does not exist. Use the old grouping while preparing the scenarios.

| Group | Stages |
|---|---|
| 1 | Packet Receiver |
| 2 | Securezone Notification, Quality Classifier, Create Draft, Message Sender |
| 3 | ABIS Handler, ABIS Middleware, Bio Dedupe, Manual Adjudication |
| 4 | Biometric Authentication, Demo Dedupe |
| 5 | Packet Validator, CMD Validator, Operator Validator, Supervisor Validator, Introducer Validator |
| 6 | Packet Uploader, Packet Classifier, Verification |
| 7 | Biometric Extraction, Finalization, Credential Requestor (old version: also UIN Generator) |

## Preparation (old version, all pods running)
**First:** stop the **Reprocessor**, so it does not move the stuck packets.

| # | Scenario | How to reach it | Picked by script |
|---|---|---|---|
| 1 | Published packet | Stop the ID Repo **credential request generator** service; send a NEW packet. It is published at Finalization, then stays at Credential Requestor with REPROCESS. Start the service again before the next scenario. Do not use this UIN for authentication tests. | No |
| 2 | Waiting for manual adjudication | 1. Stop the mock MV service.<br>2. Send a packet whose biometrics match an existing identity. It waits in the MA queue.<br>3. Keep the mock MV service stopped until the upgrade.<br>4. During the upgrade (step 6), clear the MA queue, then start the mock MV service. | Yes |
| 3 | PAUSED | 1. Set `mosip.regproc.camelbridge.pause-settings` in `registration-processor-default.properties` to:<br>`[{"ruleId":"PAUSE","matchExpression":"$.tags[?(@['AGE_GROUP'] == 'ADULT')]","pauseFor":604800,"defaultResumeAction":"RESUME_PROCESSING","fromAddress":"eventbus://packet-classifier-new-bus-out","ruleDescription":"Adult packet paused"}]`<br>This pauses every adult packet for 7 days (`pauseFor` is in seconds: 604800 = 7 days).<br>2. Restart the camel bridge.<br>3. Send a NEW adult packet. It pauses after Packet Classifier for 7 days (`status_code = 'PAUSED'`).<br>4. Set the property back to its original value and restart the camel bridge, so the next scenarios are not paused. | No |
| 4 | PAUSED_FOR_ADDITIONAL_INFO | Send a packet that needs a correction at Quality Classifier. Do not send the correction yet. | No |
| 5 | Old (v1) draft | Goal: the packet stops right after UIN Generator, which creates the v1 draft. Stopping group 7 doesn't work, because UIN Generator also runs in it. **Why PMS:** Biometric Extraction cannot run without PMS, so stopping PMS makes the packet wait there. PMS cannot be stopped at the start, because the packet also needs it to be sent to ABIS (step 2).<br>1. Stop **ABIS**.<br>2. Send a NEW packet of a **new person** (biometrics not registered before), so that ABIS finds no match. Its data share is created (uses PMS) and the ABIS requests wait in the queue.<br>3. Stop **PMS**.<br>4. Start ABIS. It answers with no match; the packet passes Bio Dedupe and UIN Generator (v1 draft created), then waits at Biometric Extraction with REPROCESS.<br>5. Start PMS again before the next scenario.<br>Keep steps 2–4 short: the data share URL expires. Check `registration_transaction`: UIN Generator SUCCESS, then Biometric Extraction REPROCESS. | Yes |
| 6 | Waiting for ABIS (NEW, UPDATE, LOST) | 1. Stop ABIS.<br>2. Send one NEW, one UPDATE (with biometrics) and one LOST packet (of a registered person). They wait in the ABIS queue.<br>3. Keep ABIS stopped until the upgrade.<br>4. During the upgrade (step 6), clear the ABIS request and response queues, then start ABIS. | Yes |
| 7 | Failed or normal packet before Create Draft | 1. Stop **group 5**.<br>2. Send two packets. Both wait after Packet Uploader.<br>3. Leave the first packet as it is (**normal**).<br>4. Mark the second packet as **failed** (keep `status_code` and `latest_trn_dtimes` unchanged):<br>`UPDATE regprc.registration SET reg_stage_name = 'PacketValidatorStage', latest_trn_status_code = 'FAILED' WHERE reg_id = '<RID>';` | Normal: No<br>Failed: Yes |
| 8 | At Packet Receiver | Stop **group 2**; send a packet. It waits after Packet Receiver (`reg_stage_name = 'PacketReceiverStage'`). | Yes |

## Upgrade and run
Follow the draft upgrade guide once for all packets: stop traffic, record `CUTOVER_TS`, clear Kafka and the ABIS, MA and MV queues, deploy, dry run, run `./reprocess.py --db --loop`, start the Reprocessor when the lag is normal.

The dry run must list exactly the packets marked **Yes** above.

**Routing value:** run the test with the same `securezone.routing.enabled` value as production. If it is `false` (Approach 2), the packets picked by the script wait at `SecurezoneNotificationStage` SUCCESS after the run, until the Reprocessor starts (step 11). Then they continue and reach the expected results below.

## Expected results
| # | Expected after the upgrade |
|---|---|
| 1 | After the Reprocessor starts (step 11), it retries Credential Requestor; no new draft; PROCESSED |
| 2 | Restarted from securezone; passes Create Draft; sent to ABIS and MA again; PROCESSED or REJECTED according to the mock MV decision |
| 3 | After step 11, resume it with the Workflow Action API (`RESUME_PROCESSING`); the Reprocessor sends it on; passes Create Draft; PROCESSED |
| 4 | Send the correction packet after the upgrade: the packet resumes, passes Create Draft, PROCESSED. (Without a correction it ends REJECTED after `PAUSE_FOR`, 15 days by default; not checked in this session.) |
| 5 | Old draft discarded and a new one created; sent to ABIS again with a new reference ID, without matching its own earlier entry; PROCESSED with a new UIN |
| 6 | All three packets are restarted from securezone, pass Create Draft, are sent to ABIS again and end PROCESSED. |
| 7 | Normal: Reprocessor resumes it; passes Create Draft; PROCESSED.<br>Failed: restarted from securezone by the script; passes Create Draft; PROCESSED (the packet itself is valid) |
| 8 | Restarted from securezone; Packet Uploader re-checks it; PROCESSED |

## Final checks
- **Rerun:** run `./reprocess.py --db --loop` again. The query returns 0 rows and nothing is sent twice.
