use crate::config::AppPaths;
use crate::error::Result;

pub fn run_phase1_preview(paths: &AppPaths) -> Result<()> {
    println!("+-- ECHO CLI ---------------------------------------------+");
    println!("| local music / sharp terminal / Phase 1 foundation       |");
    println!("+---------------------------------------------------------+");
    println!("|  / search     echo-cli search <query>                   |");
    println!("|  :scan        echo-cli scan <folder>                    |");
    println!("|  doctor       echo-cli doctor                           |");
    println!("|  devices      echo-cli devices                          |");
    println!("+---------------------------------------------------------+");
    println!(
        "|  db           {:<43} |",
        shrink(&paths.database_path.display().to_string(), 43)
    );
    println!("+---------------------------------------------------------+");
    println!("Ratatui mode lands in Phase 3; this preview keeps startup instant.");
    Ok(())
}

fn shrink(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }

    if width <= 3 {
        return ".".repeat(width);
    }

    let tail: String = value
        .chars()
        .rev()
        .take(width - 3)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("...{tail}")
}
