<#
    Script created by Micha Vermeer for Damen Shipyards

    This script will calculate the recommended StagingPathQuotaInMB for each DFS-R in $DomainName.
    See also: https://msdn.microsoft.com/en-us/library/cc754229(v=ws.11).aspx#bkmk_optimize
    
    TODO:
    - Check if the DFS-R is read-only, if so then StagingPathQuotaInMB should be only equal to the 16 largest files instead of 32.
    - Run job on the DC if there are no site DFSR servers


#>

# Function to make it easy to write to a log file

Function Write-Log {
    [CmdletBinding()]

    Param(
    [Parameter(Mandatory=$True)]
    [string]
    $Message,

    [Parameter(Mandatory=$False)]
    [ValidateSet("INFO","WARN","ERROR","FATAL","DEBUG")]
    [String]
    $Level,

    [Parameter(Mandatory=$False)]
    [string]
    $logfile = "$env:HOMEDRIVE$env:HOMEPATH\DFSLogfileV2 $(get-date -Format 'yyyy-MM-dd').log"
    )

    $Stamp = (Get-Date).toString("yyyy/MM/dd HH:mm:ss")

    If(!$Level) {
        $Level = "INFO"
    }            

    $Line = "$Stamp $Level $Message"
    If($logfile) {
        Add-Content $logfile -Value $Line -Encoding UTF8 #-PassThru # Comment the -passthrough parameter if you want less output to console. It will still output to the logfile.
    }
    Else {
        Write-Output $Line
    }
}

Write-Log -Message "Running..."

# Clean/define variables

$DfsrMembers = ""
$AllDfsServers = ""
$SiteDfsServer = ""
$SiteDfsFolders = ""
$SiteDfsFolder = ""
$SiteDfsServerJobsRunning = ""
$SiteDfsServerJobsAll = ""
$RunningJobs = ""
$CompleteJobs = ""
$CompleteJob = ""
$JobData = ""
$DfsRepFolder = ""
$Dfs32LargestFilesRounded = ""
$DfsMinimumStagingQuota = ""
$SiteRepPartnerQuotas = ""
$SiteRepPartnerQuota = ""
$JobErrors = ""
$JobError = ""



# Initiallize the array that will contain the data to write to the CSV file, and setting other variables

$DFSArray = @()
$DomainName = "schelde.com"
$increment = 4096 # The minimum amount (in MB) that a staging quota should be regardless of size of files
$RunningTime = 0

# Get all the servers, or put -groupname to the next line to specifically only run this for that DFSR group. For example: -GroupName "sites.damen.local\appdata\damen"

$DfsrMembers = Get-DfsrMember -DomainName $DomainName

$AllDfsServers = ($DfsrMembers).ComputerName | Select-Object -Unique   # Get all the servers in the replication group(s)

foreach ($SiteDfsServer in $AllDfsServers) 
    {

    if ($SiteDfsServer -notmatch "FIS001") # Skip any servers in the DC
        {
        
        $SiteDfsFolders = $DfsrMembers | ?{$_.ComputerName -match $SiteDfsServer} | Get-DfsReplicatedFolder -DomainName $DomainName # This gets us the PS Object that contains the DFSN Path
            
        foreach ($SiteDfsFolder in $SiteDfsFolders)
            {
            Write-Log -Message "------------------------------------------------"
            Write-Log -Message "Processing: $($SiteDfsFolder.DfsnPath)"

            # Making sure we don't start more then running 20 jobs on a particular server, because of the session limit of 25
            
            $SiteDfsServerJobsRunning = Get-Job | ?{$_.State -eq "Running" -and $_.Location -eq $SiteDfsServer} # How many jobs are we already running on this server?
                        
            While ($SiteDfsServerJobsRunning.Count -eq 20) # Going to throttle the amount of running jobs on the site server
                {
                Start-Sleep 15 # Wait 15 seconds because we have 20 running jobs
                $SiteDfsServerJobsRunning = Get-Job | ?{$_.State -eq "Running" -and $_.Location -eq $SiteDfsServer} # Checking again
                }

            # So we have less then 20 jobs running, so we can add another

            Invoke-Command -ComputerName $SiteDfsServer -ArgumentList $SiteDfsFolder -AsJob -JobName "$($SiteDfsFolder.DfsnPath)" `
                { param($SiteDfsFolder)                     
                (Get-ChildItem $SiteDfsFolder.DfsnPath -Recurse -ErrorAction SilentlyContinue | `
                Sort-Object Length -Descending | `
                Select-Object -First 32 | `
                Measure-Object -Property Length -Sum).Sum /1mb
                }

            $SiteDfsServerJobsRunning = Get-Job | ?{$_.State -eq "Running" -and $_.Location -eq $SiteDfsServer} # Checking the amount of running jobs again
            $SiteDfsServerJobsAll = Get-Job | ?{$_.Location -eq $SiteDfsServer} # We also need to check the total (running or otherwise) because we don't need to start jobs when there is no more work to be done.

            Write-Log -Message "We have $($SiteDfsServerJobsRunning.Count) running job(s) on $SiteDfsServer." -Level DEBUG
            Write-Log -Message "We have added $($SiteDfsServerJobsAll.Count) total job(s) on $SiteDfsServer." -Level DEBUG

            }
        }

    else # Server is a FIS001 aka a DC server
    {
    # TODO: Run a job if there is no site server.
    }

}

Write-Log -Message "Started all the jobs, now we need to wait untill they have all finished, but a maximum of 1 hour"

$RunningJobs = Get-Job | ?{$_.State -eq "Running"} # Where is my whip?

While ($RunningJobs.Count -ne 0 -and $RunningTime -lt 60) # Wait until all the jobs are finished, but not longer then 60 minutes
    {
    Write-Log -Message "We have uncompleted jobs, and we are waiting for these:"
    Get-Job | ?{$_.State -eq "Running"} | Format-Table -AutoSize | Out-File  -append "$env:HOMEDRIVE$env:HOMEPATH\DFSLogfileV2 $(get-date -Format 'yyyy-MM-dd').log" -Encoding utf8
    
    Start-Sleep 60 # wait 1 minute for the next update
    $RunningTime++ # Keeping time
    
    $RunningJobs = Get-Job | ?{$_.State -eq "Running"}
            
    }

Write-Log -Message "All jobs are Finished or we ran out of time"

$CompleteJobs = Get-Job | ?{$_.State -eq "Completed"} 

foreach ($CompleteJob in $CompleteJobs) # Looping through every completed job, write to log file, and execute the change.
    {

    $JobData = Receive-Job $CompleteJob -Keep # So, how many bytes did you say?
    $DfsRepFolder = Get-DfsReplicatedFolder -DomainName $DomainName | ?{$_.DfsnPath -eq $CompleteJob.Name} # We used the DFSNPatch for the name of the job, so we can match it again

    # Round to whole MB
    $Dfs32LargestFilesRounded = [math]::Round($JobData+1,0,1) # Rounding up like I learned at school, 0.445 -> 1

    Write-Log "Stagingfolder $($CompleteJob.Name) properties:" -Level DEBUG # I like logging
    Write-Log "The 32 largest files together are $Dfs32LargestFilesRounded MB" -Level DEBUG
    $DfsMinimumStagingQuota = [Math]::Ceiling($Dfs32LargestFilesRounded / $Increment) * $Increment; # Anything below 4096 becomes 4096, anything below 8192 becomes 8192, etc with 4092 increments.

    $SiteRepPartnerQuotas = $DfsRepFolder | Get-DfsrMembership -DomainName $DomainName | ?{$_.foldername -eq "$($DfsRepfolder.foldername)" -and "$($_.computername)" -notmatch "FIS001"}
        
    foreach ($SiteRepPartnerQuota in $SiteRepPartnerQuotas) 
        {

        Write-Log "The current DFS Staging Quota on server $($SiteRepPartnerQuota.ComputerName) is set at $($SiteRepPartnerQuota.StagingPathQuotaInMB) MB" -Level DEBUG
        Write-Log "The minimum size of the quota should be $($DfsMinimumStagingQuota) MB" -Level DEBUG

        # Creating a new entry in the array that we use to create the CSV at the end of the run.
        
        Write-Log -Message "Done calculating, adding to Array"

        $DFSArray += @{ `
            'Group Name'=$DfsRepFolder.GroupName; `
            'DFS Path'=$DfsRepFolder.DfsnPath; `
            'Current Staging Path Quota (MB)'=$SiteRepPartnerQuota.StagingPathQuotaInMB; `
            'Actual Size (MB)'= $Dfs32LargestFilesRounded; `
            'New Quota Size (MB)'=$DfsMinimumStagingQuota
                }    
        }

    # Here is the real work being done, adjusting the actual StagingPathQuotaInMB to the value we calculated it should be. Remove -WhatIf at the end to make it actually do things

    # 

    Write-Log -Message "Setting the Quota on $($DfsRepFolder.DfsnPath)"
    $DfsRepFolder | Get-DfsrMembership -DomainName $DomainName | ?{$_.ComputerName -eq $CompleteJob.Location -and $_.FolderName -match "$($CompleteJob.name | Split-Path -Leaf)"} | Set-DfsrMembership -StagingPathQuotaInMB $DfsMinimumStagingQuota -force -WhatIf
    } 

# Exporting the data we collected to CSV. You'll find the CSV in your homefolder ex.: D:\users\amver04\

Write-Log -Message "Finished, writing out the CSV file..."

$DFSArray | foreach {New-Object psobject -Property $_} | Export-Csv -NoTypeInformation "$env:HOMEDRIVE$env:HOMEPATH\DFSArray $(get-date -Format 'yyyy-MM-dd HHMM').csv" -Delimiter ";"

# Removing the succesfull jobs from the system
$CompleteJobs | Remove-Job

# Writing out the jobs with errors

$JobErrors = Get-Job | ?{$_.State -ne "completed"}

foreach ($JobError in $JobErrors)
    {
    Write-Log -Message "For Server $($JobError.Location) there are the following error messages:"
    Write-Log -Message "$($JobError.ChildJobs.JobStateInfo.Reason.Message)"
    }

Remove-Job * -Force # Cleaning up

Write-Log -Message "Done"

