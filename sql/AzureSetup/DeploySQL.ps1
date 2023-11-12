# Assign parameters from config file. 
# Note - following assumes execution is in the folder containing the config file.
$ParameterFile=([xml](Get-Content -Path .\ConfigFile.xml )).root

$SubscriptionName=$ParameterFile.SubscriptionName #Name of subscription in Azure
$AuthorizedUser=$ParameterFile.AuthorizedUser #Name of authorized user for key vault
$ResourceGroupName=$ParameterFile.ResourceGroupName #Name of resource group to be created
$Location=$ParameterFile.Location #Region of Azure Resource Group
$NewSQLServerName=$ParameterFile.NewSQLServerName #Name of SQL Server to be created
$UsageDatabaseName=$ParameterFile.UsageDatabaseName #Name of database to be created for storing usage data in SQL Server
$ServerAdmin=$ParameterFile.ServerAdmin #Azure user name of server admin
$NewKeyVaultName=$ParameterFile.NewKeyVaultName #Name of Key Vault to be created

# SQL Admin cred set up
$AdminPassword = Read-Host "Enter SQL Admin password (must meet Azure SQL strong password requirements): " -AsSecureString
$SQLServerAdminUserName = ConvertTo-SecureString $ServerAdmin -AsPlainText -Force
$SQLServerCredentials = New-Object System.Management.Automation.PSCredential ($ServerAdmin,$AdminPassword)

# Create resource group
Write-Host "Creating resource group"
New-AzResourceGroup -Name $ResourceGroupName -Location $Location

# Create key vault
Write-Host "Creating key vault"
New-AzKeyVault -Name $NewKeyVaultName -ResourceGroupName $ResourceGroupName -Location $Location 
Set-AzKeyVaultAccessPolicy -VaultName $NewKeyVaultName -UserPrincipalName $AuthorizedUser -PermissionsToSecrets get,set,list
Set-AzKeyVaultSecret -VaultName $NewKeyVaultName -Name 'SQLServerAdminUserName' -SecretValue $SQLServerAdminUserName
Set-AzKeyVaultSecret -VaultName $NewKeyVaultName -Name 'SQLServerAdminPassword' -SecretValue $AdminPassword

# Create new sql server  
Write-Host "Creating SQL Server"
New-AzSqlServer -ResourceGroupName $ResourceGroupName -Location $Location -ServerName $NewSQLServerName -ServerVersion "12.0" -SqlAdministratorCredentials $SQLServerCredentials

# Firewall Rules
New-AzSqlServerFirewallRule -ResourceGroupName $ResourceGroupName -ServerName $NewSQLServerName -AllowAllAzureIPs
# Example of setting up a firewall rules for a specific range.  
#New-AzSqlServerFirewallRule -ResourceGroupName $ResourceGroupName -ServerName $NewSQLServerName -FirewallRuleName "VPNIPRange" -StartIpAddress "xxx.xx.xxx.xxx" -EndIpAddress "xxx.xx.xxx.xxx" 

# Create database for server.  Default to Basic tier.
Write-Host "Creating database"
New-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $NewSQLServerName -DatabaseName $UsageDatabaseName -Edition Basic
