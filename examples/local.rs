use bytes::Bytes;
use dstore::local::Store;
use std::io;
use std::io::{stdin, BufRead, Write};

pub struct REPL {
    store: Store,
}

impl REPL {
    async fn new(store: Store) -> Self {
        Self { store }
    }

    async fn parse_input(&mut self, cmd: String) -> Result<(), Box<dyn std::error::Error>> {
        // Meta commands start with `.`.
        if cmd.starts_with(".") {
            match MetaCmdResult::run(&cmd) {
                MetaCmdResult::Unrecognized => Ok(println!("db: meta command not found: {}", cmd)),
                MetaCmdResult::Success => Ok(()),
            }
        } else {
            let words = cmd.split(" ").collect::<Vec<&str>>();
            match words[0].to_lowercase().as_ref() {
                "set" | "put" | "insert" | "in" | "i" => {
                    let key = words[1].to_string();
                    let value = Bytes::from(words[2..].join(" "));
                    if let Err(e) = self.store.insert(key, value).await {
                        eprintln!("{}", e);
                    }

                    Ok(())
                }
                "get" | "select" | "output" | "out" | "o" => {
                    let key = words[1].to_string();
                    match self.store.get(&key).await {
                        Ok(value) => {
                            println!("db: {} -> {}", key, String::from_utf8(value.to_vec())?)
                        }
                        Err(e) => eprintln!("{}", e),
                    }

                    Ok(())
                }
                "del" | "delete" | "rem" | "remove" | "rm" | "d" => {
                    // Removes only from local
                    let key = words[1].to_string();
                    Ok(self.store.remove(key))
                }
                _ => Ok(eprintln!("Unknown command!")),
            }
        }
    }
}
pub enum MetaCmdResult {
    Success,
    Unrecognized,
}

impl MetaCmdResult {
    /// Execute Meta commands on the REPL.
    pub fn run(cmd: &String) -> Self {
        match cmd.as_ref() {
            ".exit" => std::process::exit(0),
            ".version" => {
                if let Some(ver) = option_env!("CARGO_PKG_VERSION") {
                    println!("You are using KVDB v{}", ver);
                }
                Self::Success
            }
            _ => Self::Unrecognized,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client_addr = "http://127.0.0.1:50051".parse().unwrap();
    let local_store = Store::new(client_addr).await?;
    let mut repl = REPL::new(local_store).await;

    print!("dstore v0.1.0\nThis is an experimental database, do contribute to further developments at https://github.com/vyuham/dstore. \nUse `.exit` to exit the repl\ndb > ");
    io::stdout().flush().expect("Error");

    for cmd in stdin().lock().lines() {
        match cmd {
            Ok(cmd) => {
                repl.parse_input(cmd.trim().to_string()).await?;
            }
            Err(_) => eprint!("Error in reading command, exiting REPL."),
        }
        print!("db > ");
        io::stdout().flush().expect("Error");
    }

    Ok(())
}
