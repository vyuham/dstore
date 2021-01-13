use std::collections::HashMap;
use std::io;
use std::io::{stdin, BufRead, Write};
use bytes::Bytes;
use tonic::{transport::Channel, Request};

use dstore::dstore_proto::dstore_client::DstoreClient;
use dstore::dstore_proto::{GetArg, SetArg};

struct Store {
    db: HashMap<String, Bytes>,
    global: DstoreClient<Channel>,
}

impl Store {
    async fn new(addr: String) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            db: HashMap::new(),
            global: DstoreClient::connect(addr).await?,
        })
    }

    async fn parse_input(&mut self, cmd: String) {
        // Meta commands start with `.`.
        if cmd.starts_with(".") {
            match MetaCmdResult::run(&cmd) {
                MetaCmdResult::Unrecognized => println!("db: meta command not found: {}", cmd),
                MetaCmdResult::Success => {}
            }
        } else {
            let words = cmd.split(" ").collect::<Vec<&str>>();
            match words[0].to_lowercase().as_ref() {
                "set" | "put" | "insert" | "in" | "i" => {
                    if self.db.contains_key(words[1]) {
                        eprintln!("Key occupied!");
                    } else {
                        let key = words[1].to_string();
                        let value = words[2..].join(" ");
                        let req = Request::new(SetArg {
                            key: key.clone(),
                            value: value.as_bytes().to_vec(),
                        });
                        let res = self.global.set(req).await.unwrap();
                        if res.into_inner().success {
                            self.db.insert(key, Bytes::from(value));
                            eprintln!("Database updated");
                        } else {
                            let req = Request::new(GetArg { key: key.clone() });
                            let res = self.global.get(req).await.unwrap().into_inner();
                            self.db.insert(key, Bytes::from(res.value));
                            eprintln!("(Updated local) Key occupied!");
                        }
                    }
                }
                "get" | "select" | "output" | "out" | "o" => {
                    let key = words[1].to_string();
                    match self.db.get(&key) {
                        Some(value) => println!("db: {} -> {}", key, String::from_utf8(value.to_vec()).unwrap()),
                        None => {
                            let req = Request::new(GetArg { key: key.clone() });
                            let res = self.global.get(req).await.unwrap().into_inner();
                            if res.success {
                                println!("global: {} -> {}\t(Updating Local)", key, String::from_utf8(res.value.clone()).unwrap());
                                // Update local
                                self.db.insert(key, Bytes::from(res.value));
                            } else {
                                eprintln!("Key-Value mapping doesn't exist");
                            }
                        }
                    }
                }
                "del" | "delete" | "rem" | "remove" | "rm" | "d" => {
                    // Removes only from local
                    let key = words[1];
                    match self.db.get(key) {
                        Some(value) => {
                            eprintln!("({} -> {}) Removing local mapping!", key, String::from_utf8(value.to_vec()).unwrap());
                            self.db.remove(key);
                        }
                        None => eprintln!("Key-Value mapping doesn't exist"),
                    }
                }
                _ => eprintln!("Unknown command!"),
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
    let mut local_store = Store::new(client_addr).await?;

    print!("dstore v0.1.0\nThis is an experimental database, do contribute to further developments at https://github.com/vyuham/dstore. \nUse `.exit` to exit the repl\ndb > ");
    io::stdout().flush().expect("Error");

    for cmd in stdin().lock().lines() {
        match cmd {
            Ok(cmd) => {
                local_store.parse_input(cmd.trim().to_string()).await;
            }
            Err(_) => eprint!("Error in reading command, exiting REPL."),
        }
        print!("db > ");
        io::stdout().flush().expect("Error");
    }

    Ok(())
}
