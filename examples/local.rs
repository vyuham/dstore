use bytes::Bytes;
use dstore::Local;
use std::{
    error::Error,
    io::{self, stdin, BufRead, Write},
    sync::Arc,
};
use tokio::sync::Mutex;

/// Read Evaluate Print Loop for demo purposes
pub struct REPL {
    local: Arc<Mutex<Local>>,
}

impl REPL {
    /// Initializes Local and provides a reference
    async fn new(local: Arc<Mutex<Local>>) -> Self {
        Self { local }
    }

    /// Runs the Command Line Interface REPL
    async fn run(&mut self) {
        print!("dstore v0.1.0 (uid: {})\nThis is an experimental database, do contribute to further developments at https://github.com/vyuham/dstore. \nUse `.exit` to exit the repl\ndb > ", self.local.lock().await.addr);
        io::stdout().flush().expect("Error");
        for cmd in stdin().lock().lines() {
            match cmd {
                Ok(cmd) => {
                    self.parse_input(cmd.trim().to_string()).await;
                }
                Err(_) => eprint!("Error in reading command, exiting REPL."),
            }
            print!("db > ");
            io::stdout().flush().expect("Error");
        }
    }

    /// Convert REPL input into actionable commands
    async fn parse_input(&mut self, cmd: String) -> Result<(), Box<dyn Error>> {
        // Meta commands start with `.`.
        if cmd.starts_with(".") {
            match cmd.as_str() {
                ".exit" => std::process::exit(0),
                ".version" => {
                    if let Some(ver) = option_env!("CARGO_PKG_VERSION") {
                        println!("You are using KVDB v{}", ver);
                    }

                    Ok(())
                }
                _ => Ok(eprintln!("Unsuccessful parsing!")),
            }
        } else {
            // Split commands into `operation key values..` for execution, a key can't be space-seperated.
            // `get` and `del` don't take values as input, values can be space separated strings
            let words: Vec<String> = cmd.split(" ").map(|x| x.to_string()).collect();
            match words[0].to_lowercase().as_ref() {
                "set" | "put" | "insert" | "in" | "i" => {
                    let key = Bytes::from(words[1].clone());
                    let value = Bytes::from(words[2..].join(" "));
                    if let Err(e) = self.local.lock().await.insert(key, value).await {
                        eprintln!("{}", e);
                    }

                    Ok(())
                }
                "get" | "select" | "output" | "out" | "o" => {
                    let key = Bytes::from(words[1].clone());
                    match self.local.lock().await.get(&key).await {
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
                    if let Err(e) = self.local.lock().await.remove(&key).await {
                        eprintln!("{}", e);
                    }

                    Ok(())
                }
                _ => Ok(eprintln!("Unknown command!")),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a Local with a certain UID, connected to Global on defined address.
    // Store reference counted pointer for future use
    let global_addr = "127.0.0.1:50051".to_string();
    let local_addr = "127.0.0.1:50052".to_string(); // UID for Local
    let local_store = Local::new(global_addr, local_addr).await?;

    // Create REPL interface with reference counted pointer to Local
    REPL::new(local_store).await.run().await;

    Ok(())
}
