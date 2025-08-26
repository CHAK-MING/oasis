use anyhow::{Context, Result};
use tokio::process::Command;

pub struct CommandExecutor {
    command_timeout_sec: u64,
}

impl CommandExecutor {
    pub fn new() -> Self {
        Self {
            command_timeout_sec: 300, // 硬编码 5 分钟超时
        }
    }

    pub async fn execute(
        &self,
        command: &str,
        args: &[String],
        envs: &std::collections::HashMap<String, String>,
    ) -> Result<(i32, String, String)> {
        let mut cmd = Command::new(command);
        cmd.args(args);
        if !envs.is_empty() {
            cmd.envs(envs);
        }

        let timeout = std::time::Duration::from_secs(self.command_timeout_sec);
        let child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("spawn command")?;

        let wait = child.wait_with_output();
        let out = tokio::time::timeout(timeout, wait)
            .await
            .context("timeout waiting for command output")??;

        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let code = out.status.code().unwrap_or(-1);

        Ok((code, stdout, stderr))
    }
}
