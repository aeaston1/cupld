use std::io::{BufRead, Write};

use crate::skill_install::SkillInstallTarget;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum McpPromptChoice {
    Codex,
    Claude,
    OtherPrintOnly,
    Skip,
}

pub(crate) fn prompt_for_mcp_choice<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
) -> Result<McpPromptChoice, String> {
    writeln!(output, "Install MCP config too?").map_err(io_error)?;
    writeln!(output, "  1) Codex").map_err(io_error)?;
    writeln!(output, "  2) Claude").map_err(io_error)?;
    writeln!(output, "  3) Other / print config only").map_err(io_error)?;
    writeln!(output, "  4) Skip").map_err(io_error)?;
    write!(output, "Choice [1-4, default 4]: ").map_err(io_error)?;
    output.flush().map_err(io_error)?;

    let mut line = String::new();
    match input.read_line(&mut line).map_err(io_error)? {
        0 => Ok(McpPromptChoice::Skip),
        _ => match line.trim() {
            "1" => Ok(McpPromptChoice::Codex),
            "2" => Ok(McpPromptChoice::Claude),
            "3" => Ok(McpPromptChoice::OtherPrintOnly),
            "" | "4" => Ok(McpPromptChoice::Skip),
            other => Err(format!("expected a choice from 1-4, got `{other}`")),
        },
    }
}

impl McpPromptChoice {
    pub(crate) fn target(self) -> Option<SkillInstallTarget> {
        match self {
            Self::Codex => Some(SkillInstallTarget::Codex),
            Self::Claude => Some(SkillInstallTarget::Claude),
            Self::OtherPrintOnly | Self::Skip => None,
        }
    }
}

fn io_error(error: std::io::Error) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::{McpPromptChoice, prompt_for_mcp_choice};
    use std::io::Cursor;

    #[test]
    fn prompt_choices_are_testable_with_injected_io() {
        let mut input = Cursor::new(b"3\n");
        let mut output = Vec::new();

        let choice = prompt_for_mcp_choice(&mut input, &mut output).unwrap();

        assert_eq!(choice, McpPromptChoice::OtherPrintOnly);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .contains("Install MCP config too?")
        );
    }
}
