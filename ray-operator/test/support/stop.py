import time
from ray.job_submission import JobSubmissionClient

print("Sleep 20 seconds to enable the RayJob to transition to RUNNING state")
time.sleep(20)

client = JobSubmissionClient("http://127.0.0.1:8265")
job_details = client.list_jobs()
print("Number of Jobs: " + str(len(job_details)))
for job_detail in job_details:
    print("Stop Job: " + job_detail.submission_id)
    client.stop_job(job_detail.submission_id)
