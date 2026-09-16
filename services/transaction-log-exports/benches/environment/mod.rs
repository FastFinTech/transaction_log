//! Machine metadata shared by all benchmark executables. See README.md.

use std::{process::Command, thread};

use serde_json::{Value, json};

/// Emit one machine-readable line before any warmup or timing. Missing probes
/// remain null rather than inventing values or preventing a useful measurement.
pub fn print() {
    let mut machine = probe();
    machine["schema_version"] = json!(1);
    machine["architecture"] = json!(std::env::consts::ARCH);
    machine["platform"] = json!(std::env::consts::OS);
    machine["available_parallelism"] = json!(thread::available_parallelism().ok().map(usize::from));
    println!("ENVIRONMENT {machine}");
}

fn output(program: &str, args: &[&str]) -> Option<String> {
    let result = Command::new(program).args(args).output().ok()?;
    result
        .status
        .success()
        .then(|| String::from_utf8_lossy(&result.stdout).trim().to_owned())
}

#[cfg(target_os = "windows")]
fn probe() -> Value {
    // CIM reports physical/logical processor counts separately. Capture only
    // performance-relevant facts, not usernames, hostnames or hardware serials.
    let script = r#"
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$cpu = @(Get-CimInstance Win32_Processor)
$os = Get-CimInstance Win32_OperatingSystem
$system = Get-CimInstance Win32_ComputerSystem
$powerPlan = (powercfg /getactivescheme | Out-String).Trim()
[ordered]@{
    cpu_model = (($cpu | Select-Object -ExpandProperty Name -Unique) -join '; ')
    physical_cores = [int](($cpu | Measure-Object NumberOfCores -Sum).Sum)
    logical_processors = [int](($cpu | Measure-Object NumberOfLogicalProcessors -Sum).Sum)
    memory_bytes = [uint64]$system.TotalPhysicalMemory
    available_memory_bytes = [uint64]$os.FreePhysicalMemory * 1024
    os_name = $os.Caption
    os_version = $os.Version
    power_plan = $powerPlan
} | ConvertTo-Json -Compress
"#;
    output(
        "powershell",
        &["-NoProfile", "-NonInteractive", "-Command", script],
    )
    .and_then(|text| serde_json::from_str(&text).ok())
    .filter(Value::is_object)
    .unwrap_or_else(|| json!({"probe_note": "Windows CIM metadata unavailable"}))
}

#[cfg(target_os = "linux")]
fn probe() -> Value {
    use std::{collections::BTreeSet, fs};

    let cpu = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let memory = fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let os = fs::read_to_string("/etc/os-release").unwrap_or_default();
    let field = |text: &str, name: &str| -> Option<String> {
        text.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim() == name).then(|| value.trim().to_owned())
        })
    };
    let cores: BTreeSet<_> = cpu
        .split("\n\n")
        .filter_map(|processor| {
            Some((
                field(processor, "physical id")?,
                field(processor, "core id")?,
            ))
        })
        .collect();
    let memory_bytes = |name| -> Option<u64> {
        field(&memory, name)?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?
            .checked_mul(1024)
    };
    let logical = cpu
        .lines()
        .filter(|line| {
            line.split_once(':')
                .is_some_and(|(key, _)| key.trim() == "processor")
        })
        .count();
    json!({
        "cpu_model": field(&cpu, "model name").or_else(|| field(&cpu, "Hardware")),
        "physical_cores": (!cores.is_empty()).then_some(cores.len()),
        "logical_processors": (logical > 0).then_some(logical),
        "memory_bytes": memory_bytes("MemTotal"),
        "available_memory_bytes": memory_bytes("MemAvailable"),
        "os_name": os.lines().find_map(|line| line.strip_prefix("PRETTY_NAME=")).map(|s| s.trim_matches('"')),
        "os_version": output("uname", &["-r"]),
        "power_plan": fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").ok().map(|s| s.trim().to_owned()),
    })
}

#[cfg(target_os = "macos")]
fn probe() -> Value {
    let number = |name| output("sysctl", &["-n", name]).and_then(|s| s.parse::<u64>().ok());
    json!({
        "cpu_model": output("sysctl", &["-n", "machdep.cpu.brand_string"]),
        "physical_cores": number("hw.physicalcpu"),
        "logical_processors": number("hw.logicalcpu"),
        "memory_bytes": number("hw.memsize"),
        "available_memory_bytes": null,
        "os_name": "macOS",
        "os_version": output("sw_vers", &["-productVersion"]),
        "power_plan": null,
    })
}

#[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
fn probe() -> Value {
    json!({"os_version": output("uname", &["-a"]), "probe_note": "Limited metadata on this platform"})
}
