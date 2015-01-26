import os
import os.path
from subprocess import check_output, CalledProcessError
from tempfile import mkstemp

CRON_BEGIN    = "#--BEGIN TELEMETRY SCHEDULED JOBS--"
CRON_DISCLAIMER = """#### DO NOT EDIT THIS SECTION BY HAND ####
#### YOUR CHANGES WILL BE OVERWRITTEN ####"""
CRON_END      = "#--END TELEMETRY SCHEDULED JOBS--"
CRONTAB_CMD = "/usr/bin/crontab"
CRON_RUNNER_LOCATION = os.path.dirname(os.path.realpath(__file__)) + "/jobs/run.sh"

def get_existing_crontab():
    # Note: annoyingly, OSX's crontab exits non-zero if there
    #       is no existing crontab for the user. So if you see:
    #  CalledProcessError: Command '['/usr/bin/crontab', '-l']' returned
    #  non-zero exit status 1
    #       Then you should manually 'crontab -e' first and save an empty one.
    return check_output([CRONTAB_CMD, "-l"])

def job_to_line(job, runner_location=CRON_RUNNER_LOCATION):
    time = " ".join((job["schedule_minute"],
            job["schedule_hour"], job["schedule_day_of_month"],
            job["schedule_month"], job["schedule_day_of_week"]))
    return "{0} {1} {2}".format(time, CRON_RUNNER_LOCATION, job['id'])

def update_crontab(jobs, existing_crontab=None):
    # Get current crontab
    if existing_crontab is None:
        existing_crontab = get_existing_crontab()

    existing_crontab_lines = existing_crontab.splitlines()
    # Snip out BEGIN to END
    new_crontab_lines = []
    include_line = True
    for i in range(len(existing_crontab_lines)):
        line = existing_crontab_lines[i]
        if line == CRON_BEGIN:
            include_line = False
        elif line == CRON_END:
            include_line = True
        elif include_line:
            new_crontab_lines.append(line)

    # Always put the automated section at the end of the file.
    new_crontab_lines.append(CRON_BEGIN)
    new_crontab_lines.append(CRON_DISCLAIMER)
    for job in jobs:
        new_crontab_lines.append("# Specification for job {id}: {name}, owned by {owner}:".format(**job))
        new_crontab_lines.append(job_to_line(job))
    new_crontab_lines.append(CRON_END)
    return "\n".join(new_crontab_lines)
    # save updated crontab

def save_crontab(crontab_text):
    # write a temp file with the updated contents
    temp_cron_fh, temp_cron_filename = mkstemp(text=True)
    f = os.fdopen(temp_cron_fh, "w")
    print "Writing new crontab to", temp_cron_filename
    f.write(crontab_text)
    f.close()
    # Install the new crontab
    result = check_output([CRONTAB_CMD, temp_cron_filename])
    print "Crontab result:", result

    # Delete the temp file.
    os.remove(temp_cron_filename)

# A utility function to populate a map for testing.
def _make_job(job_id, name, owner, minute, hour, dom, mon, dow, cmd):
    return {
        "id": job_id,
        "name": name,
        "owner": owner,
        "schedule_minute": minute,
        "schedule_hour": hour,
        "schedule_day_of_month": dom,
        "schedule_month": mon,
        "schedule_day_of_week": dow,
        "schedule_command": cmd
    }


if __name__ == "__main__":
    test_crontab_new = """
# m h  dom mon dow   command
2 12,20 * * * /home/ubuntu/run_s3_cache_update.sh
3 * * * * /home/ubuntu/run_anomaly_detection.sh

5 16 * * * /home/ubuntu/run_flash_export.sh
5 16 * * * /home/ubuntu/run_slowsql_export.sh
5 16 * * * /home/ubuntu/run_mainthreadio_export.sh
5 16 * * 0 /home/ubuntu/run_anr_export.sh"""

    test_crontab_existing = """
# m h  dom mon dow   command
2 12,20 * * * /home/ubuntu/run_s3_cache_update.sh
3 * * * * /home/ubuntu/run_anomaly_detection.sh

5 16 * * * /home/ubuntu/run_flash_export.sh
#--BEGIN TELEMETRY SCHEDULED JOBS--
# Specification for job 1: t1, owned by old@mozilla.com:
0 10 * * * foof.sh
# Specification for job 2: t2, owned by old@mozilla.com:
0 17 * * 0 barf.sh
#--END TELEMETRY SCHEDULED JOBS--
5 16 * * * /home/ubuntu/run_slowsql_export.sh
5 16 * * * /home/ubuntu/run_mainthreadio_export.sh
5 16 * * 0 /home/ubuntu/run_anr_export.sh"""

    expected_crontab = """
# m h  dom mon dow   command
2 12,20 * * * /home/ubuntu/run_s3_cache_update.sh
3 * * * * /home/ubuntu/run_anomaly_detection.sh

5 16 * * * /home/ubuntu/run_flash_export.sh
5 16 * * * /home/ubuntu/run_slowsql_export.sh
5 16 * * * /home/ubuntu/run_mainthreadio_export.sh
5 16 * * 0 /home/ubuntu/run_anr_export.sh
#--BEGIN TELEMETRY SCHEDULED JOBS--
#### DO NOT EDIT THIS SECTION BY HAND ####
#### YOUR CHANGES WILL BE OVERWRITTEN ####
# Specification for job 1: t1, owned by mark@mozilla.com:
0 12 * * * foo.sh
# Specification for job 2: t2, owned by mark@mozilla.com:
0 13 * * 0 bar.sh
# Specification for job 3: t3, owned by mark@mozilla.com:
0 11 1 * * baz.sh
#--END TELEMETRY SCHEDULED JOBS--"""
    jobs = [_make_job(1, "t1", "mark@mozilla.com", '0', '12', '*', '*', '*', 'foo.sh'),
            _make_job(2, "t2", "mark@mozilla.com", '0', '13', '*', '*', '0', 'bar.sh'),
            _make_job(3, "t3", "mark@mozilla.com", '0', '11', '1', '*', '*', 'baz.sh')]
    new_crontab = update_crontab(jobs, test_crontab_new)
    if new_crontab != expected_crontab:
        print "------- New telemetry schedule --------"
        print "Error: doesn't match expected output."
        print "<<< Expected:"
        print expected_crontab
        print ">>> Actual"
        print new_crontab

    new_crontab = update_crontab(jobs, test_crontab_existing)
    if new_crontab != expected_crontab:
        print "------- Existing telemetry schedule --------"
        print "Error: doesn't match expected output."
        print "<<< Expected:"
        print expected_crontab
        print ">>> Actual"
        print new_crontab

