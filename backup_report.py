import boto3
import csv
import io
from datetime import datetime, timedelta
import botocore

S3_BUCKET = "<Bucket Name For the Reports>"
BASE_PREFIX = "<Bucket Base Prefix>" 

INSTANCES_FILE = "instances.txt"

OUTPUT_CSV = "consolidated_backup_report.csv"

START_DATE = datetime() #Start date for pulling backup report example: (2024, 10, 1)
END_DATE = datetime() #End Date for pulling backup report example: (2025, 2, 28)

CSV_RESOURCE_ARN_COLUMN = "Resource ARN"
CSV_JOB_STATUS_COLUMN = "Job Status"

REGION = "us-east-1"

s3 = boto3.client("s3")
sts = boto3.client("sts")
ec2 = boto3.client("ec2", region_name=REGION)

ACCOUNT_ID = sts.get_caller_identity()["Account"]

def load_instance_ids(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f if line.strip()]

def build_ec2_arn(instance, region, account_id):
    return f"arn:aws:ec2:{region}:{account_id}:instance/{instance}"

def build_valid_arns(instance_ids, region, account_id):
    arn_to_id = {}
    for instance in instance_ids:
        arn = build_ec2_arn(instance, region, account_id)
        arn_to_id[arn] = instance
    return arn_to_id

def build_daily_prefix(date_obj):
    year_str = date_obj.strftime("%Y")
    month_str = date_obj.strftime("%m")
    day_str = date_obj.strftime("%d")
    return f"{BASE_PREFIX}/{year_str}/{month_str}/{day_str}/"

def list_objects_in_prefix(bucket, prefix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                keys.append(obj["Key"])
    return keys

def describe_instance_details(instance_ids):
    details_map = {}
    if not instance_ids:
        return details_map

    chunk_size = 100
    global_invalid_ids = []

    for i in range(0, len(instance_ids), chunk_size):
        chunk = instance_ids[i : i + chunk_size]

        try:
            response = ec2.describe_instances(InstanceIds=chunk)
        except botocore.exceptions.ClientError as e:
            error_code = e.response["Error"].get("Code", "")
            if error_code == "InvalidInstanceID.NotFound":
                msg = e.response["Error"].get("Message", "")
                print(f"WARNING: InvalidInstanceID.NotFound for chunk {chunk}. Error: {msg}")

                if "The instance IDs" in msg and "do not exist" in msg:
                    start = msg.find("IDs")
                    start = msg.find("'", start) + 1
                    end = msg.find("'", start)
                    invalid_str = msg[start:end]
                    potential_ids = [x.strip() for x in invalid_str.split(",")]
                    for bad_id in potential_ids:
                        if bad_id in chunk:
                            global_invalid_ids.append(bad_id)

                chunk = [instance for instance in chunk if instance not in global_invalid_ids]

                if not chunk:
                    continue

                try:
                    response = ec2.describe_instances(InstanceIds=chunk)
                except:
                    raise
            else:
                raise

        for reservation in response.get("Reservations", []):
            for inst in reservation.get("Instances", []):
                instance = inst["InstanceId"]
                launch_time = inst.get("LaunchTime", "Unknown")
                if launch_time != "Unknown":
                    launch_time = launch_time.isoformat()

                tag_dict = {}
                for t in inst.get("Tags", []):
                    tag_dict[t["Key"]] = t["Value"]

                details_map[instance] = {
                    "LaunchTime": launch_time,
                    "Tags": tag_dict
                }

    if global_invalid_ids:
        print(f"\nWARNING: The following instance IDs are invalid or terminated:\n  {global_invalid_ids}")
    return details_map, set(global_invalid_ids)

def parse_csv_from_s3(bucket, key, arn_to_id_map, arn_column, job_status_column, found_arns):
    print(f"    --> Downloading and parsing CSV: {key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))
    print(f"    --> CSV Headers: {reader.fieldnames}")

    matching_rows = []
    for row in reader:
        row_arn = row.get(arn_column)
        if not row_arn:
            continue  

        if row_arn not in arn_to_id_map:
            continue 

        job_status = row.get(job_status_column, "").strip().lower()
        if job_status == "running":
            continue 

        matching_rows.append(row)
        found_arns.add(row_arn)

    return matching_rows

def main():
    instance_ids = load_instance_ids(INSTANCES_FILE)
    if not instance_ids:
        print(f"ERROR: No instance IDs found in {INSTANCES_FILE}")
        return

    arn_to_id_map = build_valid_arns(instance_ids, REGION, ACCOUNT_ID)

    print("Loaded instance IDs and their ARNs:")
    for arn, instance in arn_to_id_map.items():
        print(f"  - {instance} => {arn}")

    all_instance_ids = list(arn_to_id_map.values())
    instance_details, invalid_ids = describe_instance_details(all_instance_ids)

    found_arns = set()

    current_date = START_DATE
    all_matched_rows = []

    while current_date <= END_DATE:
        daily_prefix = build_daily_prefix(current_date)
        print(f"\nSearching S3 for date: {current_date.strftime('%Y-%m-%d')}")
        print(f"Prefix: {daily_prefix}")

        s3_keys = list_objects_in_prefix(S3_BUCKET, daily_prefix)
        if not s3_keys:
            print("  -> No objects found for this date.")
            current_date += timedelta(days=1)
            continue

        for key in s3_keys:
            if key.endswith("/"):
                continue  

            if key.lower().endswith(".csv"):
                matched_rows = parse_csv_from_s3(
                    S3_BUCKET,
                    key,
                    arn_to_id_map,
                    CSV_RESOURCE_ARN_COLUMN,
                    CSV_JOB_STATUS_COLUMN,
                    found_arns
                )
                if matched_rows:
                    print(f"      -> Found {len(matched_rows)} matching rows.")
                    all_matched_rows.extend(matched_rows)
                else:
                    print("      -> No matching ARNs in this CSV (or all were 'Running').")
            else:
                print(f"      -> Skipping non-CSV file: {key}")

        current_date += timedelta(days=1)

    all_fields = set()
    for row in all_matched_rows:
        all_fields.update(row.keys())

    all_fields.add("Launch Time")
    all_fields.add("Backup Tag")

    fieldnames = list(all_fields)

    wrote_header = False
    if all_matched_rows:
        print(f"\nTotal matched rows across all days: {len(all_matched_rows)}")
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            wrote_header = True

            for row in all_matched_rows:
                writer.writerow(row)
        print(f"Wrote matched rows to {OUTPUT_CSV}")
    else:
        print("\nNo matching CSV records found in the given date range.")
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            wrote_header = True
        print(f"Created empty CSV with header: {OUTPUT_CSV}")

    missing_arns = set(arn_to_id_map.keys()) - found_arns
    if missing_arns:
        print("\nSome ARNs were not found in any CSV:")
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not wrote_header:
                writer.writeheader()
                wrote_header = True

            for arn in missing_arns:
                instance_id = arn_to_id_map[arn]

                details = instance_details.get(instance_id, {})
                launch_time = details.get("LaunchTime", "Unknown")
                tags_dict = details.get("Tags", {})

                backup_plan_value = tags_dict.get("BackupPlan", None)
                if backup_plan_value is None:
                    final_backup_tag = "No Backup Tag"
                else:
                    if not backup_plan_value.strip():
                        final_backup_tag = "Backup Tag Blank"
                    else:
                        final_backup_tag = backup_plan_value

                if instance_id in invalid_ids:
                    job_status_str = "No Backups. Instance is terminated."
                else:
                    job_status_str = "No Backups Found within Date Range for this Instance"

                row_dict = {col: "" for col in fieldnames}
                row_dict[CSV_RESOURCE_ARN_COLUMN] = arn
                row_dict[CSV_JOB_STATUS_COLUMN] = job_status_str
                row_dict["Launch Time"] = launch_time
                row_dict["Backup Tag"] = final_backup_tag

                writer.writerow(row_dict)
                print(f"  - Missing: {arn} ({job_status_str})")
    else:
        print("\nAll ARNs were found at least once in the CSV data.")

    print(f"\nFinal consolidated CSV located at: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
