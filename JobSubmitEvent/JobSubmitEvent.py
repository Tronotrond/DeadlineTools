from Deadline.Events import DeadlineEventListener
from Deadline.Scripting import ClientUtils, RepositoryUtils
#from Deadline.Jobs import *

def GetDeadlineEventListener():
    return JobSubmitEventListener()

def CleanupDeadlineEventListener(deadlinePlugin):
    deadlinePlugin.Cleanup()


class JobSubmitEventListener(DeadlineEventListener):
    def __init__(self):
        # Set up the event callbacks here
        super().__init__() # Required in Deadline 10.3 and later.
        ClientUtils.LogText("JobEventSubmitter initialized")
        self.OnJobSubmittedCallback += self.OnJobSubmitted
        self.OnJobImportedCallback += self.OnJobSubmitted
        #self.OnJobResumedCallback += self.TestFunction
        
        self.group_limit_mappings = {}
        
    def Cleanup(self):
        del self.OnJobSubmittedCallback
        del self.OnJobImportedCallback
        

    def configGroupLimitMappings(self):
        # Retrieve mappings from the event plugin configuration settings
        group_limit_mapping_string = self.GetConfigEntryWithDefault("GroupLimitMappings", "")
        self.group_limit_mappings = {}
        
        # Parse the mappings if they're not empty
        if group_limit_mapping_string:
            for mapping in group_limit_mapping_string.split(";"):
                group, limit = mapping.split(":")
                self.group_limit_mappings[group.strip()] = limit.strip()

    def OnJobSubmitted(self, job):
        ClientUtils.LogText("New Job Submitted")
        # Retrieve current limit groups on the job
        current_limit_groups = job.JobLimitGroups
        self.configGroupLimitMappings()
               
        # Check if any limit groups are already set
        if current_limit_groups:
            ClientUtils.LogText(f"Job {job.JobId} already has limit groups set: {current_limit_groups}. Skipping modification.")
            return
            

        # Identify the appropriate limit groups based on the job group
        job_group = job.JobGroup.lower()  # Convert to lowercase for easier matching
        new_limit_groups = []

        for group_keyword, limit_group in self.group_limit_mappings.items():
            if group_keyword in job_group:
                new_limit_groups.append(limit_group)

        # Apply limit groups if any were matched
        if new_limit_groups:
            job.SetJobLimitGroups(new_limit_groups)
            RepositoryUtils.SaveJob(job)
            ClientUtils.LogText(f"Added limit groups {new_limit_groups} to job {job.JobId} based on group '{job_group}'.")
        else:
            ClientUtils.LogText(f"No limit group added to job {job.JobId}; group '{job_group}' did not match any criteria.")
            
    def TestFunction(self, job):
        ClientUtils.LogText("Test function")