use bytes::Bytes;
use dstore::local::Node;
use std::{
    error::Error,
    io::{self, stdin, BufRead, Write},
};

pub struct REPL {
    node: Node,
}

impl REPL {
    async fn new(node: Node) -> Result<Self, Box<dyn Error>> {
        Ok(Self { node })
    }

    async fn parse_input(&mut self, cmd: String) -> Result<(), Box<dyn Error>> {
        // Meta commands start with `.`.
        if cmd.starts_with(".") {
            match MetaCmdResult::run(&cmd) {
                MetaCmdResult::Unrecognized => Ok(println!("db: meta command not found: {}", cmd)),
                MetaCmdResult::Success => Ok(()),
            }
        } else {
            let words: Vec<String> = cmd.split(" ").map(|x| x.to_string()).collect();
            match words[0].to_lowercase().as_ref() {
                "set" | "put" | "insert" | "in" | "i" => {
                    let key = Bytes::from(words[1].clone());
                    let value = Bytes::from(words[2..].join(" "));
                    if let Err(e) = self.node.insert(key, value).await {
                        eprintln!("{}", e);
                    }

                    Ok(())
                }
                "get" | "select" | "output" | "out" | "o" => {
                    let key = Bytes::from(words[1].clone());
                    match self.node.get(&key).await {
                        Ok(value) => {
                            println!(
                                "db: {} -> {}",
                                String::from_utf8(key.to_vec())?,
                                String::from_utf8(value.to_vec())?
                            )
                        }
                        Err(e) => eprintln!("{}", e),
                    }

                    Ok(())
                }
                "del" | "delete" | "rem" | "remove" | "rm" | "d" => {
                    // Removes only from local
                    let key = Bytes::from(words[1].clone());
                    if let Err(e) = self.node.remove(&key).await {
                        eprintln!("{}", e);
                    }

                    Ok(())
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
    pub fn run(cmd: &str) -> Self {
        match cmd {
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
async fn main() -> Result<(), Box<dyn Error>> {
    let global_addr = "127.0.0.1:50051".to_string();
    let local_addr = "127.0.0.1:50052".to_string();
    let local_store = Node::new(global_addr, local_addr).await?;
    let mut repl = REPL::new(local_store).await?;

    print!("dstore v0.1.0 (addr: {})\nThis is an experimental database, do contribute to further developments at https://github.com/vyuham/dstore. \nUse `.exit` to exit the repl\ndb > ", repl.node.addr);
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
