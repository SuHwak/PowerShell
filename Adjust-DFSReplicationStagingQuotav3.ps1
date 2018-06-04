<#
    Script created by Micha Vermeer for Damen Shipyards

    This script will calculate the recommended StagingPathQuotaInMB for each DFS-R in $DomainName.
    See also: https://msdn.microsoft.com/en-us/library/cc754229(v=ws.11).aspx#bkmk_optimize
    
    TODO:
    - Check if the DFS-R is read-only, if so then StagingPathQuotaInMB should be only equal to the 16 largest files instead of 32.
    - Convert script to use jobs instead of just processing everything serially
        - Run only the jobs on the remote servers so we can run them quicker then all on the 1 or 2 servers in the DC.


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
        Add-Content $logfile -Value $Line -Encoding UTF8 -PassThru
    }
    Else {
        Write-Output $Line
    }
}

Write-Log -Message "Running..."

# Clean variables

$DfsrMembers = ""
$DfsRepfolder = ""
$DCRepPartnerQuotas = ""
$DCRepPartnerQuota = ""
$SiteRepPartnerQuotas = ""
$SiteRepPartnerQuota = ""
$CompleteJobs = ""

# Initiallize the array that will contain the data to write to the CSV file, and setting other variables

$DFSArray = @()
$DomainName = "schelde.com"
$increment = 4096 # The minimum amount (in MB) that a staging quota should be regardless of size of files


# Get all the servers

$DfsrMembers = Get-DfsrMember -DomainName $DomainName #-GroupName "sites.damen.local\appdata\damen" # To get the servers of the replication group

$DfsRepfolders = $DfsrMembers | Get-DfsReplicatedFolder -DomainName $DomainName

$AllDfsServers = ($DfsrMembers).ComputerName | select -Unique   # Each server in all replication groups

foreach ($SiteDfsServer in $AllDfsServers)
    {

    if ($SiteDfsServer -notmatch "FIS001") # Skip any servers in the DC
        {
        
        $SiteDfsFolders = $DfsrMembers | ?{$_.ComputerName -match $SiteDfsServer} | Get-DfsReplicatedFolder -DomainName $DomainName 
            
        foreach ($SiteDfsFolder in $SiteDfsFolders)
            {
            Write-Log -Message "------------------------------------------------"
            Write-Log -Message "Processing: $($SiteDfsFolder.DfsnPath)"

            # This DFS Replication folder has more then 1 replication partner (Server)
            
            # Making sure we don't start more then running 20 jobs on a particular server, because of the session limit of 25
            
            $SiteDfsServerJobsRunning = Get-Job | ?{$_.State -eq "Running" -and $_.Location -eq $SiteDfsServer}

            While ($SiteDfsServerJobsRunning.Count -eq 20) # Going to throttle the amount of running jobs on the site server
                {
                Start-Sleep 15
                $SiteDfsServerJobsRunning = Get-Job | ?{$_.State -eq "Running" -and $_.Location -eq $SiteDfsServer}
                }

            Invoke-Command -ComputerName $SiteDfsServer -ArgumentList $SiteDfsFolder -AsJob -JobName "$($SiteDfsFolder.DfsnPath)" `
                { param($SiteDfsFolder)                     
                (Get-ChildItem $SiteDfsFolder.DfsnPath -Recurse -ErrorAction SilentlyContinue | `
                Sort-Object Length -Descending | `
                Select-Object -First 32 | `
                Measure-Object -Property Length -Sum).Sum /1mb
                }

            $SiteDfsServerJobsRunning = Get-Job | ?{$_.State -eq "Running" -and $_.Location -eq $SiteDfsServer}
            $SiteDfsServerJobsAll = Get-Job | ?{$_.Location -eq $SiteDfsServer}

            Write-Log -Message "We have $($SiteDfsServerJobsRunning.Count) running job(s) on $SiteDfsServer."
            Write-Log -Message "We have added $($SiteDfsServerJobsAll.Count) total job(s) on $SiteDfsServer."
                
            }

             
        }

    else # Server is a FIS001 aka a DC server
    {
    # Run the jobs if there is no site server.
    }

}

Write-Log -Message "Started all the jobs, now we need to wait untill they have all finished"

$RunningJobs = Get-Job | ?{$_.State -eq "Running"}

While ($RunningJobs.Count -ne 0)
    {
    Write-Log -Message "We have uncompleted jobs, and we are waiting for these:"
    Get-Job | ?{$_.State -eq "Running"} | Format-Table -AutoSize | Out-File  -append "$env:HOMEDRIVE$env:HOMEPATH\DFSLogfileV2 $(get-date -Format 'yyyy-MM-dd').log" -Encoding utf8

    Start-Sleep 15
    $RunningJobs = Get-Job | ?{$_.State -eq "Running"}
    
    }

Write-Log -Message "All jobs are Finished"

foreach ($CompleteJob in $CompleteJobs) 
    {

    $JobData = Receive-Job $CompleteJob -Keep 

        # Round to whole MB
        $Dfs32LargestFilesRounded = [math]::Round($JobData+1,0,1) 

        if($Dfs32LargestFilesRounded -gt 4096)  # This part checks the total size of $Dfs32LargestFilesRounded and puts at least 4096 MB
                                                # or any increment of 4096 MB above that as integer into DfsMinimumStagingQuota. So 4097 MB will be rounded to 8192 MB.
                                                # So 4097 MB will be rounded to 8192 MB.
        {

          Write-Log "Value is greater then 4096, namely $Dfs32LargestFilesRounded" -Level DEBUG
          $DfsMinimumStagingQuota = [Math]::Ceiling($Dfs32LargestFilesRounded / $Increment) * $Increment;

        }
 
        else                                    # The 32 files make up less then 4096 MB
        {

          Write-Log "Value is lower then 4096, namely $Dfs32LargestFilesRounded" -Level DEBUG
          $DfsMinimumStagingQuota = $Increment  # Which is 4096    

        }
        
        Write-Log "Stagingfolder $($CompleteJob.Name) properties:" -Level DEBUG
        Write-Log "Actual size of the 32 largest files is $($Dfs32LargestFilesRounded) MB" -Level DEBUG
                
        $DCRepPartnerQuotas = $DfsRepFolder | Get-DfsrMembership -DomainName $DomainName | ?{$_.foldername -eq "$($dfsrepFolder.foldername)" -and "$($_.computername)" -match "FIS001"}
        $SiteRepPartnerQuotas = $DfsRepFolder | Get-DfsrMembership -DomainName $DomainName | ?{$_.foldername -eq "$($dfsrepFolder.foldername)" -and "$($_.computername)" -notmatch "FIS001"}
        
        foreach ($DCRepPartnerQuota in $DCRepPartnerQuotas) 
            {

            Write-Log "The current DFS Staging Quota on server $($DCRepPartnerQuota.ComputerName) is set at $($DCRepPartnerQuota.StagingPathQuotaInMB) MB" -Level DEBUG

            }
        foreach ($SiteRepPartnerQuota in $SiteRepPartnerQuotas) 
            {

            Write-Log "The current DFS Staging Quota on server $($SiteRepPartnerQuota.ComputerName) is set at $($SiteRepPartnerQuota.StagingPathQuotaInMB) MB" -Level DEBUG

            }

        Write-Log "The minimum size of the quota should be $($DfsMinimumStagingQuota) MB" -Level DEBUG
        
        # Creating a new entry in the array that we use to create the CSV at the end of the run.
        
        Write-Log -Message "Done calculating, adding to Array"
        
        $DFSArray += @{ `
            'Group Name'=$DfsRepFolder.GroupName; `
            'DFS Path'=$DfsRepFolder.DfsnPath; `
            'Current Staging Path Quota (MB)'=$DCRepPartnerQuota.StagingPathQuotaInMB; `
            'Actual Size (MB)'= $Dfs32LargestFilesRounded; `
            'New Quota Size (MB)'=$DfsMinimumStagingQuota
                }

        # Here is the real work being done, adjusting the actual StagingPathQuotaInMB to the value we calculated it should be. Remove -WhatIf at the end to make it actually do things

        $DfsRepFolder | Get-DfsrMembership -DomainName $DomainName | ?{$_.foldername -eq "$($dfsrepFolder.foldername)"} | Set-DfsrMembership -StagingPathQuotaInMB $DfsMinimumStagingQuota -force -WhatIf
    } 

# Exporting the data we collected to CSV. You'll find the CSV in your homefolder ex.: D:\users\amver04\

Write-Log -Message "Finished, writing out the CSV file..."

$DFSArray | foreach {New-Object psobject -Property $_} | Export-Csv -NoTypeInformation "$env:HOMEDRIVE$env:HOMEPATH\DFSArray $(get-date -Format 'yyyy-MM-dd hhmm').csv"

# Removing the succesfull jobs from the system
$CompleteJobs | Remove-Job

# Writing out the jobs with errors

$JobErrors = Get-Job | ?{$_.State -ne "completed"}

foreach ($JobError in $JobErrors)
    {
    Write-Log -Message "For Server $($JobError.Location) there are the following error messages:"
    Write-Log -Message "$($JobError.ChildJobs.Error)"
    }

Write-Log -Message "Done"