#requires -Version 5.1
<#
.SYNOPSIS
    ScaleLedger Client의 부팅 작업과 공용 바탕화면 시작·종료 바로가기를 등록합니다.
.DESCRIPTION
    관리자 Windows PowerShell에서 실행하는 신규 설치용 스크립트입니다.
    프로그램 폴더의 소스, .venv, config.toml과 암호 있는 표준 실행 계정을
    먼저 준비해야 합니다. 실행 계정에는 일괄 작업 로그온 권한이 필요합니다.
    기존 예약 작업과 바로가기는 덮어쓰지 않으며 프로그램을 즉시 시작하지 않습니다.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$taskName = 'ScaleLedger Client'
$taskRegistered = $false
$changesStarted = $false
$credential = $null
$plainPassword = $null

function Add-DirectoryAccess {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][System.Security.Principal.SecurityIdentifier]$Identity,
        [Parameter(Mandatory = $true)][System.Security.AccessControl.FileSystemRights]$Rights
    )

    $acl = Get-Acl -LiteralPath $Path
    $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $Identity,
        $Rights,
        [System.Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit',
        [System.Security.AccessControl.PropagationFlags]::None,
        [System.Security.AccessControl.AccessControlType]::Allow
    )
    $acl.AddAccessRule($rule)
    Set-Acl -LiteralPath $Path -AclObject $acl
}

try {
    if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
        throw '이 설치 스크립트는 Windows에서만 실행할 수 있습니다.'
    }
    $installer = [System.Security.Principal.WindowsPrincipal]::new(
        [System.Security.Principal.WindowsIdentity]::GetCurrent()
    )
    if (-not $installer.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Windows PowerShell을 관리자 권한으로 열고 다시 실행해 주세요.'
    }

    $programRoot = (Get-Item -LiteralPath (Join-Path $PSScriptRoot '..')).FullName
    if ($programRoot.StartsWith('\\')) {
        throw '프로그램은 네트워크 공유가 아닌 PC의 로컬 디스크에 설치해 주세요.'
    }
    $programDrive = [System.IO.DriveInfo]::new([System.IO.Path]::GetPathRoot($programRoot))
    if ($programDrive.DriveType -ne [System.IO.DriveType]::Fixed) {
        throw '프로그램은 PC의 로컬 고정 드라이브에 설치해 주세요. 네트워크 매핑 드라이브와 이동식 드라이브는 지원하지 않습니다.'
    }
    $python = Join-Path $programRoot '.venv\Scripts\python.exe'
    $pythonw = Join-Path $programRoot '.venv\Scripts\pythonw.exe'
    $entryPoint = Join-Path $programRoot 'main.py'
    $config = Join-Path $programRoot 'config.toml'
    foreach ($path in @($python, $pythonw, $entryPoint, $config)) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "설치 준비 파일이 없습니다: $path"
        }
    }
    $dataPath = Join-Path $programRoot 'data'
    $logsPath = Join-Path $programRoot 'logs'
    foreach ($path in @($dataPath, $logsPath)) {
        if ((Test-Path -LiteralPath $path) -and -not (Test-Path -LiteralPath $path -PathType Container)) {
            throw "데이터 또는 로그 폴더 경로에 다른 파일이 있습니다: $path"
        }
    }

    $desktopPath = [Environment]::GetFolderPath([Environment+SpecialFolder]::CommonDesktopDirectory)
    if (-not (Test-Path -LiteralPath $desktopPath -PathType Container)) {
        throw '공용 바탕화면 폴더를 찾을 수 없습니다.'
    }
    $shortcutPath = Join-Path $desktopPath 'ScaleLedger Client 시작.lnk'
    $stopShortcutPath = Join-Path $desktopPath 'ScaleLedger Client 종료.lnk'
    foreach ($path in @($shortcutPath, $stopShortcutPath)) {
        if (Test-Path -LiteralPath $path) {
            throw "기존 바로가기가 있습니다. 신규 설치를 중단합니다: $path"
        }
    }

    $scheduler = New-Object -ComObject 'Schedule.Service'
    $scheduler.Connect()
    $taskFolder = $scheduler.GetFolder('\')
    foreach ($existingTask in $taskFolder.GetTasks(1)) {
        if ($existingTask.Name -eq $taskName) {
            throw "기존 예약 작업 '$taskName'이 있습니다. 신규 설치를 중단합니다."
        }
    }
    $shell = New-Object -ComObject 'WScript.Shell'

    Write-Host '서버 주소와 로그 설정을 검사합니다.'
    & $python -B $entryPoint --check-config
    if ($LASTEXITCODE -ne 0) {
        throw '설정 검사에 실패했습니다. config.toml과 Python 의존성을 확인해 주세요.'
    }

    $credential = Get-Credential -Message 'PC 부팅 시 클라이언트를 실행할 표준 계정과 Windows 암호를 입력해 주세요. 이 계정으로 바탕화면 바로가기를 사용합니다.'
    if ($null -eq $credential -or $credential.Password.Length -eq 0) {
        throw '암호 있는 실행 계정이 필요합니다. 설치를 중단합니다.'
    }
    $accountName = $credential.UserName
    if ($accountName.StartsWith('.\')) {
        $accountName = $env:COMPUTERNAME + $accountName.Substring(1)
    }
    $account = [System.Security.Principal.NTAccount]::new($accountName)
    $accountSid = $account.Translate([System.Security.Principal.SecurityIdentifier])
    if ($accountSid.Value -in @('S-1-5-18', 'S-1-5-19', 'S-1-5-20') -or $accountSid.Value.EndsWith('-500')) {
        throw '기본 Administrator 또는 시스템 서비스 계정 대신 암호 있는 표준 사용자 계정을 사용해 주세요.'
    }
    $accountName = $accountSid.Translate([System.Security.Principal.NTAccount]).Value

    $definition = $scheduler.NewTask(0)
    $definition.RegistrationInfo.Description = 'PC 부팅 시 계량 클라이언트를 시작합니다.'
    $definition.Principal.UserId = $accountSid.Value
    $definition.Principal.LogonType = 1 # TASK_LOGON_PASSWORD
    $definition.Principal.RunLevel = 0 # TASK_RUNLEVEL_LUA (Limited)
    $trigger = $definition.Triggers.Create(8) # TASK_TRIGGER_BOOT
    $trigger.Enabled = $true
    $action = $definition.Actions.Create(0) # TASK_ACTION_EXEC
    $action.Path = $pythonw
    $action.Arguments = '"{0}"' -f $entryPoint
    $action.WorkingDirectory = $programRoot

    $settings = $definition.Settings
    $settings.Enabled = $true
    $settings.AllowDemandStart = $true
    $settings.ExecutionTimeLimit = 'PT0S'
    $settings.MultipleInstances = 2 # TASK_INSTANCES_IGNORE_NEW
    $settings.RestartInterval = 'PT1M'
    $settings.RestartCount = 3
    $settings.DisallowStartIfOnBatteries = $false
    $settings.StopIfGoingOnBatteries = $false
    $settings.RunOnlyIfNetworkAvailable = $false
    $settings.RunOnlyIfIdle = $false
    $settings.IdleSettings.StopOnIdleEnd = $false
    $settings.StartWhenAvailable = $true

    $changesStarted = $true
    foreach ($path in @($dataPath, $logsPath)) {
        if (-not (Test-Path -LiteralPath $path -PathType Container)) {
            New-Item -ItemType Directory -Path $path | Out-Null
        }
    }
    Add-DirectoryAccess -Path $programRoot -Identity $accountSid -Rights ReadAndExecute
    Add-DirectoryAccess -Path $dataPath -Identity $accountSid -Rights Modify
    Add-DirectoryAccess -Path $logsPath -Identity $accountSid -Rights Modify

    # 등록 시 ACL을 함께 지정합니다. 실행 계정의 작업 수정 권한은 추가하지 않습니다.
    # TASK_CREATE | TASK_DONT_ADD_PRINCIPAL_ACE: 기존 작업을 덮어쓰지 않습니다.
    $taskSecurity = 'D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GRGX;;;{0})' -f $accountSid.Value
    try {
        $plainPassword = $credential.GetNetworkCredential().Password
        $null = $taskFolder.RegisterTaskDefinition(
            $taskName, $definition, (2 -bor 16), $accountName, $plainPassword, 1, $taskSecurity
        )
        $taskRegistered = $true
    }
    finally {
        $plainPassword = $null
        $credential = $null
    }

    if (Test-Path -LiteralPath $shortcutPath) {
        throw "기존 바로가기가 있습니다. 바로가기 생성을 중단합니다: $shortcutPath"
    }
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = Join-Path ([Environment]::SystemDirectory) 'schtasks.exe'
    $shortcut.Arguments = '/run /tn "{0}"' -f $taskName
    $shortcut.WorkingDirectory = $programRoot
    $shortcut.Description = '등록된 ScaleLedger Client 작업을 시작합니다.'
    $shortcut.WindowStyle = 7
    $shortcut.Save()

    if (Test-Path -LiteralPath $stopShortcutPath) {
        throw "기존 바로가기가 있습니다. 바로가기 생성을 중단합니다: $stopShortcutPath"
    }
    $stopShortcut = $shell.CreateShortcut($stopShortcutPath)
    $stopShortcut.TargetPath = $pythonw
    $stopShortcut.Arguments = '"{0}" --stop' -f $entryPoint
    $stopShortcut.WorkingDirectory = $programRoot
    $stopShortcut.Description = '실행 중인 ScaleLedger Client에 정상 종료를 요청합니다.'
    $stopShortcut.WindowStyle = 7
    $stopShortcut.Save()

    Write-Host "설치가 완료되었습니다. 예약 작업: $taskName"
    Write-Host "프로그램 폴더: $programRoot"
    Write-Host "실행 계정: $accountName"
    Write-Host "시작 바로가기: $shortcutPath"
    Write-Host "종료 바로가기: $stopShortcutPath"
    Write-Host '지금 시작하려면 실행 계정으로 로그인한 뒤 바탕화면의 시작 바로가기를 눌러 주세요. 다음 PC 부팅부터는 로그인 전에 자동 실행합니다.'
    Write-Host '프로그램을 종료하려면 같은 실행 계정에서 바탕화면의 종료 바로가기를 눌러 주세요.'
}
catch {
    Write-Host ("설치에 실패했습니다: {0}" -f $_.Exception.Message) -ForegroundColor Red
    if ($taskRegistered) {
        Write-Host "예약 작업 '$taskName'은 등록되었습니다. 바로가기와 폴더 권한을 설치 담당자가 확인해 주세요." -ForegroundColor Yellow
    }
    elseif ($changesStarted) {
        Write-Host '생성한 데이터·로그 폴더와 추가한 접근 권한은 남아 있습니다. 오류를 확인한 뒤 다시 실행해 주세요.' -ForegroundColor Yellow
    }
    exit 1
}
finally {
    $plainPassword = $null
    $credential = $null
}
