use crate::cli::OutputMode;

pub trait Renderable {
    fn render_table(&self, no_color: bool) -> String;
    fn render_json(&self) -> serde_json::Value;
    fn render_raw(&self) -> String;
}

pub fn print_output(value: &dyn Renderable, mode: OutputMode, no_color: bool) {
    let text = match mode {
        OutputMode::Table => value.render_table(no_color),
        OutputMode::Json => serde_json::to_string_pretty(&value.render_json()).unwrap(),
        OutputMode::Raw => value.render_raw(),
    };
    print!("{text}");
}
