# LLD — Clinical-Trials Data Pipeline on AWS

Low-level design. Read [hld.md](hld.md) first. Items marked **(to build)** don't exist in code yet. Items marked
**(verify in Stage N)** are assumptions to confirm against the account or AWS docs when that stage is built.

---

## 1. Repository layout (target)
```
src/                      # pure Python, unit-tested locally, shipped to Glue as a wheel
  fetch.py                # ingest: API → raw pages + manifest            (done)
  state.py                # watermark read/write/overlap                  (done)
  storage.py              # LocalStorage / S3Storage, one interface       (to build)
  normalize.py            # raw study → curated row; Parquet writer       (to build)
  load_ods.py             # curated Parquet → Postgres upsert             (to build)
glue/                     # thin Glue entry scripts: parse args → call src/
  ingest_job.py  transform_job.py  load_ods_job.py
sql/
  ods_schema.sql          # Postgres DDL (applied by load-ods on start, idempotent)
  redshift_schema.sql     # Redshift DDL + load_trials() procedure
infra/                    # Terraform (flat files, one per concern)
  main.tf variables.tf outputs.tf
  s3.tf iam.tf glue.tf sfn.tf scheduler.tf sns.tf monitoring.tf
  network.tf rds.tf redshift.tf
  state_machine.asl.json  # templated with templatefile()
tests/                    # pytest, no network, no AWS
pyproject.toml            # builds the src/ wheel for Glue
Makefile                  # package / upload / plan / apply / destroy
```

**Runtime target:** Glue Python Shell runs **Python 3.9** (verify in Stage 4), so code in `src/` must stay
3.9-compatible (`from __future__ import annotations`, no `match`). Local dev and CI can use 3.12.

---

## 2. Storage abstraction — `src/storage.py` (to build)
All modules read and write through one small interface, so tests use a temp folder and Glue uses S3 with the
same code.

```python
class Storage(Protocol):
    def write_text(self, key: str, text: str) -> None: ...
    def read_text(self, key: str) -> str | None: ...          # None if missing
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def read_bytes(self, key: str) -> bytes: ...
    def list(self, prefix: str) -> list[str]: ...
    def delete_prefix(self, prefix: str) -> None: ...

LocalStorage(root: Path)          # keys map to files under root
S3Storage(bucket: str, client=None)  # boto3 put_object / get_object / list_objects_v2 / delete_objects
```
Changes this implies for existing code:
- `fetch.ingest(storage, run_date, …)` replaces `out_dir`. Before writing it calls
  `delete_prefix("raw/date=<run_date>/")`, so a re-run can't leave stray pages from an earlier, larger run.
- `state.read_watermark(storage)` / `write_watermark(storage, …)` use the key `state/watermark.json`.
- **Skipped runs write a manifest** `{"status": "skipped", "data_timestamp": …}`. Full runs write
  `"status": "complete"`. The state machine branches on this.
- In AWS the ingest job **does not** advance the watermark; the state machine does it last (§6). The local CLI
  keeps `--incremental` advancing it, for demos.

---

## 3. Glue jobs

| Job | Entry script | Calls | VPC connection | Capacity | Timeout | Arguments |
|-----|--------------|-------|----------------|----------|---------|-----------|
| `ctp-ingest` | `glue/ingest_job.py` | `fetch.ingest` | **None** (needs internet for the API) | 0.0625 DPU | 30 min | `--run_date`, `--bucket`, `--since` (optional seed), `--condition` (optional) |
| `ctp-transform` | `glue/transform_job.py` | `normalize.transform_partition` | None | 0.0625 DPU | 20 min | `--run_date`, `--bucket` |
| `ctp-load-ods` | `glue/load_ods_job.py` | `load_ods.load_partition` | **`ctp-vpc`** (reaches RDS) | 0.0625 DPU | 20 min | `--run_date`, `--bucket`, `--db_secret_arn`, `--db_host`, `--db_name` |

Common settings: `Command.Name = pythonshell`, `PythonVersion = 3.9`, `MaxRetries = 0`, `MaxConcurrentRuns = 1`,
`--extra-py-files s3://<bucket>/artifacts/ctpipeline-<ver>-py3-none-any.whl`, `--enable-continuous-cloudwatch-log
true`. Third-party libraries (`requests`, `pyarrow`, `psycopg2-binary`): use the built-in analytics library set
where it covers them, otherwise add them with `--additional-python-modules` (verify in Stage 4).

Entry script pattern (each is ~15 lines, no logic):
```python
import sys
from awsglue.utils import getResolvedOptions
from src.storage import S3Storage
from src import fetch

args = getResolvedOptions(sys.argv, ["run_date", "bucket"])
manifest = fetch.ingest(storage=S3Storage(args["bucket"]), run_date=args["run_date"])
print(manifest)  # shows up in CloudWatch Logs
```
An uncaught exception fails the Glue run, which Step Functions sees as `States.TaskFailed`.

---

## 4. Data contracts

### 4.1 S3 keys
```
s3://ctp-<account>-<region>/
  raw/date=2026-11-22/page-0001.json … page-NNNN.json   # JSON array of full study records
  raw/date=2026-11-22/_manifest.json
  curated/date=2026-11-22/trials.parquet
  state/watermark.json
  artifacts/ctpipeline-0.2.0-py3-none-any.whl, glue/*.py
```
Bucket: SSE-S3 encryption, all public access blocked, versioning off. Lifecycle: `raw/` moves to Infrequent
Access after 30 days.

### 4.2 Manifest (`_manifest.json`)
```json
{ "status": "complete",              // or "skipped"
  "run_date": "2026-11-22", "fetched_at": "2026-11-22T11:00:41+00:00",
  "data_timestamp": "2026-11-22T10:00:04",
  "condition": null, "updated_since": "2026-11-20", "max_records": null,
  "total_count": 1873, "expected": 1873, "received": 1873,
  "files": ["page-0001.json", "page-0002.json"] }
```

### 4.3 Watermark (`state/watermark.json`)
```json
{ "data_timestamp": "2026-11-22T10:00:04", "loaded_through": "2026-11-22", "run_date": "2026-11-22" }
```

### 4.4 Curated row (Parquet schema) — `normalize.py`
| Column | Type | Source in `protocolSection` | Rule |
|--------|------|-----------------------------|------|
| `nct_id` | string, not null | `identificationModule.nctId` | primary key |
| `title` | string | `identificationModule.briefTitle` | trimmed |
| `conditions` | string | `conditionsModule.conditions[]` | joined with `"; "` |
| `interventions` | string | `armsInterventionsModule.interventions[].name` | joined with `"; "` |
| `phase` | string | `designModule.phases[]` | joined with `"/"`, e.g. `PHASE1/PHASE2`; null if none |
| `overall_status` | string | `statusModule.overallStatus` | as-is, e.g. `RECRUITING` |
| `sponsor` | string | `sponsorCollaboratorsModule.leadSponsor.name` | |
| `start_date` | date | `statusModule.startDateStruct.date` | `YYYY-MM` → first of month |
| `last_update_date` | date, not null | `statusModule.lastUpdatePostDateStruct.date` | decides which version wins |
| `summary` | string | `descriptionModule.briefSummary` | whitespace collapsed |
| `embed_text` | string | title + summary + conditions + interventions | for the Stage 8 RAG chatbot |
| `run_date` | date | job argument | lineage |

Within a partition, de-duplicate by `nct_id` and keep the highest `last_update_date`. A record with no `nct_id`
is dropped and counted; if more than 1% are dropped, the job fails.

---

## 5. Databases

### 5.1 ODS — RDS Postgres (`sql/ods_schema.sql`)
```sql
CREATE TABLE IF NOT EXISTS trials (
  nct_id            text PRIMARY KEY,
  title             text,
  conditions        text,
  interventions     text,
  phase             text,
  overall_status    text,
  sponsor           text,
  start_date        date,
  last_update_date  date NOT NULL,
  summary           text,
  embed_text        text,
  source_run_date   date NOT NULL,
  loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS trials_status_idx ON trials (overall_status);
CREATE INDEX IF NOT EXISTS trials_updated_idx ON trials (last_update_date);
```
**Upsert** (`load_ods.load_partition`): read `curated/date=X/trials.parquet` and run a batched
`execute_values` insert into the table, all in one transaction:
```sql
INSERT INTO trials (nct_id, title, …, last_update_date, summary, embed_text, source_run_date)
VALUES %s
ON CONFLICT (nct_id) DO UPDATE SET
  title = EXCLUDED.title, …, last_update_date = EXCLUDED.last_update_date,
  source_run_date = EXCLUDED.source_run_date, loaded_at = now()
WHERE trials.last_update_date <= EXCLUDED.last_update_date;   -- never overwrite newer with older
```
Connection: host/db from job args; user/password read from Secrets Manager at start-up; `sslmode=require`.
The job applies the DDL on start (`IF NOT EXISTS`, so it's safe every run).

RDS settings: Postgres 16, `db.t4g.micro`, 20 GB gp3, private subnets, `publicly_accessible = false`,
`manage_master_user_password = true` (RDS keeps the password in Secrets Manager), `skip_final_snapshot = true`
and `deletion_protection = false` (learning project), backups 1 day.

### 5.2 Warehouse — Redshift Serverless (`sql/redshift_schema.sql`)
```sql
CREATE TABLE IF NOT EXISTS trials (
  nct_id varchar(16) NOT NULL, title varchar(1000), conditions varchar(4000), interventions varchar(4000),
  phase varchar(64), overall_status varchar(64), sponsor varchar(512), start_date date,
  last_update_date date NOT NULL, summary varchar(65535), embed_text varchar(65535), run_date date,
  PRIMARY KEY (nct_id)
) DISTSTYLE AUTO SORTKEY (last_update_date);

CREATE OR REPLACE PROCEDURE load_trials(p_run_date varchar(10)) AS $$
BEGIN
  CREATE TEMP TABLE stage (LIKE trials);          -- columns match the Parquet order exactly
  EXECUTE 'COPY stage FROM ''s3://ctp-<account>-<region>/curated/date=' || p_run_date ||
          '/'' IAM_ROLE DEFAULT FORMAT AS PARQUET';
  MERGE INTO trials USING stage s ON trials.nct_id = s.nct_id
    WHEN MATCHED THEN UPDATE SET title = s.title, …, last_update_date = s.last_update_date, run_date = s.run_date
    WHEN NOT MATCHED THEN INSERT VALUES (s.nct_id, s.title, …, s.run_date);
  DROP TABLE stage;
END;
$$ LANGUAGE plpgsql;
```
Workgroup: base capacity 8 RPU (the minimum), in the private subnets; the namespace's default IAM role can read
`curated/`. The DDL is applied once after `terraform apply`, from the Redshift query editor or a Data API call
in the Makefile.

---

## 6. Step Functions state machine (`infra/state_machine.asl.json`)
Standard workflow, `TimeoutSeconds: 7200`. `${…}` placeholders are filled by Terraform `templatefile()`.
Input contract: `{"scheduled_time": "2026-11-22T11:00:00Z"}` (the scheduler sends this; manual runs pass the same).

```json
{
  "Comment": "Daily clinical-trials pipeline",
  "TimeoutSeconds": 7200,
  "StartAt": "Init",
  "States": {
    "Init": {
      "Type": "Pass",
      "Parameters": {
        "run_date.$": "States.ArrayGetItem(States.StringSplit($.scheduled_time, 'T'), 0)",
        "bucket": "${bucket}",
        "load_warehouse": ${enable_redshift}
      },
      "Next": "Ingest"
    },
    "Ingest": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${ingest_job}",
                      "Arguments": { "--run_date.$": "$.run_date", "--bucket.$": "$.bucket" } },
      "ResultPath": null,
      "Retry": [
        { "ErrorEquals": ["Glue.ConcurrentRunsExceededException", "Glue.AWSGlueException"],
          "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2.0 },
        { "ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 300, "MaxAttempts": 1 }
      ],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" }],
      "Next": "ReadManifest"
    },
    "ReadManifest": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
      "Parameters": { "Bucket.$": "$.bucket",
                      "Key.$": "States.Format('raw/date={}/_manifest.json', $.run_date)" },
      "ResultSelector": { "body.$": "States.StringToJson($.Body)" },
      "ResultPath": "$.manifest",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" }],
      "Next": "HasNewData"
    },
    "HasNewData": {
      "Type": "Choice",
      "Choices": [{ "Variable": "$.manifest.body.status", "StringEquals": "skipped", "Next": "NoNewData" }],
      "Default": "Transform"
    },
    "NoNewData": { "Type": "Succeed" },
    "Transform": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${transform_job}",
                      "Arguments": { "--run_date.$": "$.run_date", "--bucket.$": "$.bucket" } },
      "ResultPath": null,
      "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException", "Glue.AWSGlueException"],
                  "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2.0 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" }],
      "Next": "LoadODS"
    },
    "LoadODS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${load_ods_job}",
                      "Arguments": { "--run_date.$": "$.run_date", "--bucket.$": "$.bucket" } },
      "ResultPath": null,
      "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException", "Glue.AWSGlueException"],
                  "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2.0 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" }],
      "Next": "WarehouseEnabled"
    },
    "WarehouseEnabled": {
      "Type": "Choice",
      "Choices": [{ "Variable": "$.load_warehouse", "BooleanEquals": true, "Next": "LoadWarehouse" }],
      "Default": "BuildWatermark"
    },
    "LoadWarehouse": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
      "Parameters": { "WorkgroupName": "${rs_workgroup}", "Database": "trials",
                      "Sql": "CALL load_trials(:run_date)",
                      "Parameters": [{ "Name": "run_date", "Value.$": "$.run_date" }] },
      "ResultSelector": { "id.$": "$.Id" },
      "ResultPath": "$.rs",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" }],
      "Next": "WaitForWarehouse"
    },
    "WaitForWarehouse": { "Type": "Wait", "Seconds": 15, "Next": "CheckWarehouse" },
    "CheckWarehouse": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:describeStatement",
      "Parameters": { "Id.$": "$.rs.id" },
      "ResultSelector": { "id.$": "$.Id", "status.$": "$.Status" },
      "ResultPath": "$.rs",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" }],
      "Next": "WarehouseDone"
    },
    "WarehouseDone": {
      "Type": "Choice",
      "Choices": [
        { "Variable": "$.rs.status", "StringEquals": "FINISHED", "Next": "BuildWatermark" },
        { "Or": [ { "Variable": "$.rs.status", "StringEquals": "FAILED" },
                  { "Variable": "$.rs.status", "StringEquals": "ABORTED" } ],
          "Next": "WarehouseFailed" }
      ],
      "Default": "WaitForWarehouse"
    },
    "WarehouseFailed": {
      "Type": "Pass",
      "Parameters": { "run_date.$": "$.run_date",
                      "error": { "Error": "RedshiftLoadFailed", "Cause.$": "$.rs.status" } },
      "Next": "NotifyFailure"
    },
    "BuildWatermark": {
      "Type": "Pass",
      "Parameters": {
        "data_timestamp.$": "$.manifest.body.data_timestamp",
        "loaded_through.$": "States.ArrayGetItem(States.StringSplit($.manifest.body.data_timestamp, 'T'), 0)",
        "run_date.$": "$.run_date"
      },
      "ResultPath": "$.watermark",
      "Next": "AdvanceWatermark"
    },
    "AdvanceWatermark": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:s3:putObject",
      "Parameters": { "Bucket.$": "$.bucket", "Key": "state/watermark.json",
                      "ContentType": "application/json",
                      "Body.$": "States.JsonToString($.watermark)" },
      "ResultPath": null,
      "Catch": [{ "ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure" }],
      "End": true
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${alerts_topic_arn}",
        "Subject": "clinical-trials pipeline FAILED",
        "Message.$": "States.Format('run_date={} error={} cause={}', $.run_date, $.error.Error, $.error.Cause)"
      },
      "Next": "Failed"
    },
    "Failed": { "Type": "Fail", "Error": "PipelineFailed" }
  }
}
```
Notes:
- `ResultPath: null` on the Glue tasks keeps the run context (`run_date`, `bucket`) unchanged for the next state.
- The Glue `States.TaskFailed` retry is on **Ingest only**, to ride out a short API outage. Transform and load
  failures are almost always code or data problems, so they alert straight away.
- `WarehouseFailed` builds an `error` object so `NotifyFailure` sends the same message format as for any other
  failure.

---

## 7. Scheduling — EventBridge Scheduler
| Setting | Value |
|---------|-------|
| Expression | `cron(0 11 * * ? *)`, timezone UTC (the registry refreshes ~10:00 UTC) |
| Flexible time window | OFF |
| Target | `states:StartExecution` on the state machine |
| Input | `{"scheduled_time": "<aws.scheduler.scheduled-time>"}` |
| Retry policy | 2 attempts, max event age 1 h |
| State | `ENABLED` (Terraform variable `schedule_enabled` to pause) |

Manual backfill or re-run (equivalent CLI):
```bash
aws stepfunctions start-execution --state-machine-arn <arn> --input '{"scheduled_time":"2026-11-22T11:00:00Z"}'
```
First-ever run: there's no watermark yet. Either start the ingest job once with `--since 2026-11-01`, or upload
a seed `state/watermark.json` (`make seed-watermark SINCE=2026-11-01`).

---

## 8. Network (`network.tf`)
```
VPC 10.20.0.0/16 — no internet gateway on the private subnets, no NAT
  private subnets ×3 (3 AZs)      → RDS subnet group, Redshift workgroup, Glue connection (one subnet)
  S3 gateway endpoint             → free; load-ods reads curated/ via this
  Secrets Manager interface endpoint (1 AZ) → load-ods reads the DB password (~$0.01/hr; destroyed with the stack)
Security groups
  sg-glue : ingress all TCP from itself (Glue requirement); egress all
  sg-rds  : ingress 5432 from sg-glue
  sg-rs   : ingress 5439 from sg-glue (unused today; for future Glue → Redshift)
```
- Only `ctp-load-ods` is attached to the VPC. `ctp-ingest` and `ctp-transform` run with no VPC connection, so
  they have AWS-managed internet access and reach S3 over public endpoints.
- If a VPC-attached Glue job fails to write logs or call AWS APIs, add CloudWatch Logs and Glue interface
  endpoints (verify in Stage 4).
- Redshift Serverless subnet and AZ requirements: verify in Stage 6 (3 subnets in 3 AZs is the safe choice).

---

## 9. IAM (least privilege, `iam.tf`)
| Role | Trusted by | Permissions |
|------|------------|-------------|
| `ctp-glue` | `glue.amazonaws.com` | `s3:GetObject/PutObject/DeleteObject` on `raw/*`, `curated/*`, `state/*`; `s3:GetObject` on `artifacts/*`; `s3:ListBucket` on the bucket; `secretsmanager:GetSecretValue` on the RDS secret; `logs:*` on `/aws-glue/*`; EC2 network-interface actions needed by the Glue connection (`ec2:CreateNetworkInterface`, `DescribeNetworkInterfaces`, `DeleteNetworkInterface`, `Describe{Subnets,SecurityGroups,VpcEndpoints,RouteTables,Vpcs}`). |
| `ctp-sfn` | `states.amazonaws.com` | `glue:StartJobRun`, `GetJobRun`, `GetJobRuns`, `BatchStopJobRun` on the 3 jobs; `s3:GetObject` on `raw/*`, `s3:PutObject` on `state/watermark.json`; `sns:Publish` on the alerts topic; `redshift-data:ExecuteStatement`, `DescribeStatement`; `redshift-serverless:GetCredentials` on the workgroup; `events:PutTargets/PutRule/DescribeRule` (required for `.sync` integrations). |
| `ctp-scheduler` | `scheduler.amazonaws.com` | `states:StartExecution` on the state machine. |
| Redshift namespace default role | `redshift.amazonaws.com` | `s3:GetObject`, `s3:ListBucket` on `curated/*`. |
| Deploy user/role (you) | — | Terraform needs broad rights on these services. Use a separate admin/SSO role, never root keys. |

---

## 10. Monitoring and alerting (`sns.tf`, `monitoring.tf`)
- SNS topic `ctp-alerts` with an email subscription (`var.alert_email`). AWS sends a confirmation link that
  must be clicked.
- The state machine's `NotifyFailure` covers every in-pipeline failure.
- CloudWatch alarm on the state machine metrics `ExecutionsFailed ≥ 1` **or** `ExecutionsTimedOut ≥ 1`
  (period 1 day) → SNS. This catches failures outside the Catch paths, such as a bad input contract.
- Optional "didn't run" alarm: `ExecutionsStarted < 1` over 1 day with missing data treated as breaching.
- Logs: `/aws-glue/python-jobs/output` and `/error`; Step Functions logging at level `ERROR` to
  `/aws/states/ctp-pipeline`, 14-day retention.

---

## 11. Cost (rough, us-east-1; check current pricing)
| Item | Daily usage | Approx. cost |
|------|-------------|--------------|
| Glue Python Shell | 3 jobs × ~3–8 min × 0.0625 DPU × $0.44/DPU-h | < $0.01/day |
| Step Functions Standard | ~15 state transitions | ~$0 |
| EventBridge Scheduler, SNS, S3 requests | tiny | ~$0 |
| S3 storage | +20–60 MB/day | cents/month |
| Secrets Manager secret + interface endpoint | always-on while deployed | ~$0.40/month + ~$0.24/day |
| RDS db.t4g.micro | 24 h | free tier (12 months), else ~$0.40/day |
| Redshift Serverless | 8 RPU × ~1 min per load | a few cents per load, plus storage; **destroy when idle** |

---

## 12. Terraform
Variables: `region`, `project = "ctp"`, `alert_email`, `enable_rds = true`, `enable_redshift = false`,
`schedule_enabled = true`, `artifact_version`.

```bash
make package          # build the wheel, sync glue/*.py + wheel to s3://<bucket>/artifacts/
make plan             # terraform -chdir=infra plan
make apply            # terraform -chdir=infra apply
make destroy          # terraform -chdir=infra destroy
terraform -chdir=infra apply -var enable_redshift=false   # drop the costly part, keep the rest
```
The S3 bucket for artifacts exists before `package` runs, so bootstrap in two steps: `terraform apply
-target=aws_s3_bucket.lake`, then `make package && make apply`. The Terraform state stays local (a
learning project; no remote state backend).

---

## 13. Testing per layer
| Layer | Test |
|-------|------|
| `fetch`, `state` | Done: fake HTTP session; paging, retry, count check, watermark logic. |
| `storage` | `LocalStorage` against `tmp_path`; `S3Storage` against a stubbed boto3 client (`botocore.stub.Stubber`). |
| `normalize` | Fixture study JSON → expected row; partial dates; missing modules; de-duplication; drop threshold. |
| `load_ods` | Local Docker Postgres (`postgres:16`); load twice, same row count; an older record doesn't overwrite a newer one. |
| State machine | Validate the JSON definition (`aws stepfunctions validate-state-machine-definition`); optionally Step Functions Local with mocked Glue. |
| End to end | `make apply`, start one execution with ingest seeded to a 1-day window; check the manifest counts, curated row count, ODS row count, watermark advanced, and that a forced failure sends an email. Then `make destroy`. |
