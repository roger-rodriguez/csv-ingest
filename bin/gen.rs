use clap::{Arg, Command};
use std::io::{self, Write};

fn main() -> anyhow::Result<()> {
    let matches = Command::new("gen")
        .arg(
            Arg::new("rows")
                .long("rows")
                .value_parser(clap::value_parser!(u64))
                .required(true),
        )
        .arg(
            Arg::new("with_header")
                .long("with-header")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(Arg::new("cols").long("cols").default_value("3"))
        .arg(Arg::new("delim").long("delim").default_value(","))
        .get_matches();

    let rows: u64 = *matches.get_one("rows").unwrap();
    let with_header = matches.get_flag("with_header");
    let cols: usize = matches.get_one::<String>("cols").unwrap().parse()?;
    let delim = matches.get_one::<String>("delim").unwrap();

    let mut out = io::BufWriter::new(io::stdout().lock());

    if with_header {
        write!(&mut out, "sku")?;
        for i in 1..cols {
            write!(&mut out, "{}col{}", delim, i)?;
        }
        writeln!(&mut out)?;
    }

    // Very simple deterministic data: sku, col1, col2, ...
    for i in 0..rows {
        write!(&mut out, "SKU{:010}", i)?;
        for c in 1..cols {
            write!(&mut out, "{}v{}_{}", delim, c, i)?;
        }
        writeln!(&mut out)?;
        if i % 10_000 == 0 {
            out.flush()?;
        } // keep buffers moving on huge runs
    }

    out.flush()?;
    Ok(())
}
