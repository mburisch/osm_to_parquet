use std::io::Result;

fn run_protoc() -> Result<()> {
    let filenames: Vec<_> = glob::glob("protos/*.proto")
        .unwrap()
        .map(|f| f.unwrap())
        .collect();

    for filename in &filenames {
        println!("cargo::rerun-if-changed={}", filename.display());
    }

    let filename_refs: Vec<&std::path::Path> = filenames.iter().map(|p| p.as_path()).collect();
    prost_build::compile_protos(&filename_refs, &["protos/"])?;
    Ok(())
}

fn main() -> Result<()> {
    run_protoc()?;
    Ok(())
}
