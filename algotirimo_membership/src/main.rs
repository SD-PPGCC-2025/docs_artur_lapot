use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    pub id: String,
    pub address: SocketAddr,
    pub heartbeat_counter: u64,
    pub last_seen: u64,
    pub status: MemberStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemberStatus {
    Alive,
    Suspected,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Heartbeat { member_id: String, counter: u64 },
    Gossip { members: Vec<Member> },
    Join { member: Member },
    Leave { member_id: String },
}

pub struct MembershipService {
    local_member: Member,
    members: Arc<Mutex<HashMap<String, Member>>>,
    socket: UdpSocket,
    failure_timeout: Duration,
    gossip_interval: Duration,
    shutdown: Arc<AtomicBool>,
}

impl MembershipService {
    pub fn new(id: String, address: SocketAddr) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(address)?;
        socket.set_nonblocking(true)?;
        
        let local_member = Member {
            id: id.clone(),
            address,
            heartbeat_counter: 0,
            last_seen: Self::current_time(),
            status: MemberStatus::Alive,
        };
        
        let mut members = HashMap::new();
        members.insert(id.clone(), local_member.clone());
        
        Ok(MembershipService {
            local_member,
            members: Arc::new(Mutex::new(members)),
            socket,
            failure_timeout: Duration::from_secs(10),
            gossip_interval: Duration::from_secs(2),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }
    
    pub fn start(&mut self) {
        self.start_heartbeat_sender();
        self.start_message_receiver();
        self.start_failure_detector();
        self.start_gossip_sender();
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.leave_cluster();
        // Dar tempo para as mensagens de saída serem enviadas
        thread::sleep(Duration::from_millis(500));
    }
    
    fn start_heartbeat_sender(&mut self) {
        let members = Arc::clone(&self.members);
        let socket = self.socket.try_clone().expect("Failed to clone socket");
        let mut local_member = self.local_member.clone();
        let shutdown = Arc::clone(&self.shutdown);
        
        thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
                
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                
                local_member.heartbeat_counter += 1;
                local_member.last_seen = Self::current_time();
                
                // Atualizar membro local na lista
                {
                    let mut members_guard = members.lock().unwrap();
                    members_guard.insert(local_member.id.clone(), local_member.clone());
                }
                
                // Enviar heartbeat para todos os outros membros
                let members_snapshot = {
                    let members_guard = members.lock().unwrap();
                    members_guard.clone()
                };
                
                let heartbeat_msg = Message::Heartbeat {
                    member_id: local_member.id.clone(),
                    counter: local_member.heartbeat_counter,
                };
                
                for (_, member) in members_snapshot.iter() {
                    if member.id != local_member.id && matches!(member.status, MemberStatus::Alive) {
                        if let Ok(serialized) = bincode::serialize(&heartbeat_msg) {
                            let _ = socket.send_to(&serialized, member.address);
                        }
                    }
                }
            }
            println!("Heartbeat sender stopped");
        });
    }
    
    fn start_message_receiver(&self) {
        let members = Arc::clone(&self.members);
        let socket = self.socket.try_clone().expect("Failed to clone socket");
        let shutdown = Arc::clone(&self.shutdown);
        
        thread::spawn(move || {
            let mut buffer = [0; 1024];
            
            while !shutdown.load(Ordering::Relaxed) {
                match socket.recv_from(&mut buffer) {
                    Ok((size, src)) => {
                        if let Ok(message) = bincode::deserialize::<Message>(&buffer[..size]) {
                            Self::handle_message(message, src, &members);
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => {}
                }
            }
            println!("Message receiver stopped");
        });
    }
    
    fn start_failure_detector(&self) {
        let members = Arc::clone(&self.members);
        let timeout = self.failure_timeout;
        let shutdown = Arc::clone(&self.shutdown);
        
        thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(2));
                
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                
                let current_time = Self::current_time();
                let mut members_guard = members.lock().unwrap();
                
                for (_, member) in members_guard.iter_mut() {
                    let time_since_last_seen = current_time - member.last_seen;
                    
                    match member.status {
                        MemberStatus::Alive => {
                            if time_since_last_seen > timeout.as_secs() {
                                member.status = MemberStatus::Suspected;
                                println!("Member {} is now SUSPECTED", member.id);
                            }
                        }
                        MemberStatus::Suspected => {
                            if time_since_last_seen > timeout.as_secs() * 2 {
                                member.status = MemberStatus::Failed;
                                println!("Member {} is now FAILED", member.id);
                            }
                        }
                        MemberStatus::Failed => {}
                    }
                }
            }
            println!("Failure detector stopped");
        });
    }
    
    fn start_gossip_sender(&self) {
        let members = Arc::clone(&self.members);
        let socket = self.socket.try_clone().expect("Failed to clone socket");
        let interval = self.gossip_interval;
        let shutdown = Arc::clone(&self.shutdown);
        
        thread::spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                thread::sleep(interval);
                
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                
                let members_snapshot = {
                    let members_guard = members.lock().unwrap();
                    members_guard.values()
                        .filter(|m| matches!(m.status, MemberStatus::Alive))
                        .cloned()
                        .collect::<Vec<_>>()
                };
                
                if members_snapshot.len() > 1 {
                    let gossip_msg = Message::Gossip {
                        members: members_snapshot.clone(),
                    };
                    
                    // Enviar gossip para alguns membros aleatórios (máximo 3)
                    if let Ok(serialized) = bincode::serialize(&gossip_msg) {
                        let target_count = std::cmp::min(3, members_snapshot.len());
                        for member in members_snapshot.iter().take(target_count) {
                            let _ = socket.send_to(&serialized, member.address);
                        }
                    }
                }
            }
            println!("Gossip sender stopped");
        });
    }
    
    fn handle_message(message: Message, src: SocketAddr, members: &Arc<Mutex<HashMap<String, Member>>>) {
        match message {
            Message::Heartbeat { member_id, counter } => {
                let mut members_guard = members.lock().unwrap();
                if let Some(member) = members_guard.get_mut(&member_id) {
                    if counter > member.heartbeat_counter {
                        member.heartbeat_counter = counter;
                        member.last_seen = Self::current_time();
                        if !matches!(member.status, MemberStatus::Alive) {
                            println!("✓ Member {} is back ALIVE", member.id);
                        }
                        member.status = MemberStatus::Alive;
                    }
                } else {
                    // Novo membro descoberto via heartbeat
                    let new_member = Member {
                        id: member_id.clone(),
                        address: src,
                        heartbeat_counter: counter,
                        last_seen: Self::current_time(),
                        status: MemberStatus::Alive,
                    };
                    members_guard.insert(member_id.clone(), new_member);
                    println!("🔍 Discovered new member via heartbeat: {} from {}", member_id, src);
                }
            }
            Message::Gossip { members: gossip_members } => {
                let mut members_guard = members.lock().unwrap();
                let mut new_discoveries = Vec::new();
                
                for gossip_member in gossip_members {
                    match members_guard.get(&gossip_member.id) {
                        Some(existing_member) => {
                            if gossip_member.heartbeat_counter > existing_member.heartbeat_counter {
                                members_guard.insert(gossip_member.id.clone(), gossip_member);
                            }
                        }
                        None => {
                            new_discoveries.push(gossip_member.id.clone());
                            members_guard.insert(gossip_member.id.clone(), gossip_member);
                        }
                    }
                }
                
                if !new_discoveries.is_empty() {
                    println!("📡 Discovered {} new member(s) via gossip: {:?}", 
                             new_discoveries.len(), new_discoveries);
                }
            }
            Message::Join { member } => {
                let mut members_guard = members.lock().unwrap();
                let is_new = !members_guard.contains_key(&member.id);
                
                let mut corrected_member = member.clone();
                corrected_member.address = src;
                
                members_guard.insert(member.id.clone(), corrected_member.clone());
                
                if is_new {
                    println!("🆕 New member joined: {} at {} (corrected from {})", 
                            member.id, src, member.address);
                }
            }
            Message::Leave { member_id } => {
                let mut members_guard = members.lock().unwrap();
                if members_guard.remove(&member_id).is_some() {
                    println!("👋 Member left gracefully: {}", member_id);
                }
            }
        }
    }
    
    pub fn join_cluster(&self, seed_address: SocketAddr) -> Result<(), std::io::Error> {
        let join_msg = Message::Join {
            member: self.local_member.clone(),
        };
        
        let serialized = bincode::serialize(&join_msg).unwrap();
        self.socket.send_to(&serialized, seed_address)?;
        Ok(())
    }
    
    pub fn leave_cluster(&self) {
        let leave_msg = Message::Leave {
            member_id: self.local_member.id.clone(),
        };
        
        let members_snapshot = {
            let members_guard = self.members.lock().unwrap();
            members_guard.clone()
        };
        
        if let Ok(serialized) = bincode::serialize(&leave_msg) {
            for (_, member) in members_snapshot.iter() {
                if member.id != self.local_member.id {
                    let _ = self.socket.send_to(&serialized, member.address);
                }
            }
        }
    }
    
    pub fn get_alive_members(&self) -> Vec<Member> {
        let members_guard = self.members.lock().unwrap();
        members_guard
            .values()
            .filter(|m| matches!(m.status, MemberStatus::Alive))
            .cloned()
            .collect()
    }
    
    pub fn print_membership_table(&self) {
        let members_guard = self.members.lock().unwrap();
        let alive_count = members_guard.values().filter(|m| matches!(m.status, MemberStatus::Alive)).count();
        let suspected_count = members_guard.values().filter(|m| matches!(m.status, MemberStatus::Suspected)).count();
        let failed_count = members_guard.values().filter(|m| matches!(m.status, MemberStatus::Failed)).count();
        
        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║                    MEMBERSHIP TABLE                          ║");
        println!("║ Alive: {:<3} | Suspected: {:<3} | Failed: {:<3} | Total: {:<3} ║", 
                 alive_count, suspected_count, failed_count, members_guard.len());
        println!("╠═══════════════════════════════════════════════════════════════╣");
        
        for (_, member) in members_guard.iter() {
            let status_icon = match member.status {
                MemberStatus::Alive => "🟢",
                MemberStatus::Suspected => "🟡", 
                MemberStatus::Failed => "🔴",
            };
            
            let time_diff = Self::current_time() - member.last_seen;
            println!("║ {} {:<8} | {:<15} | HB:{:<6} | {}s ago    ║",
                     status_icon,
                     member.id,
                     member.address,
                     member.heartbeat_counter,
                     time_diff);
        }
        println!("╚═══════════════════════════════════════════════════════════════╝\n");
    }
    
    fn current_time() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 3 {
        println!("Algoritmo de Membership para Sistemas Distribuídos");
        println!("\nUso: {} <member_id> <port> [seed_address:port]", args[0]);
        return Ok(());
    }
    
    let member_id = args[1].clone();
    let port: u16 = args[2].parse()?;
    let address = format!("0.0.0.0:{}", port).parse()?;
    
    let mut service = MembershipService::new(member_id.clone(), address)?;
    
    if args.len() > 3 {
        let seed_address: SocketAddr = args[3].parse()?;
        
        let mut attempts = 0;
        let max_attempts = 5;
        
        while attempts < max_attempts {
            match service.join_cluster(seed_address) {
                Ok(_) => {
                    break;
                }
                Err(_) => {
                    attempts += 1;
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
    
    let service_shutdown = Arc::clone(&service.shutdown);
    ctrlc::set_handler(move || {
        println!("\nShutting down gracefully...");
        service_shutdown.store(true, Ordering::Relaxed);
    })?;
    
    service.start();
    
    println!("Membership service started for member: {} on {}", member_id, address);
    if args.len() > 3 {
        println!("Connected to cluster");
    } else {
        println!("Running as SEED node");
    }
    println!("Press Ctrl+C to stop\n");
    
    let mut counter = 0;
    while !service.shutdown.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_secs(5));
        
        if service.shutdown.load(Ordering::Relaxed) {
            break;
        }
        
        counter += 1;
        if counter % 2 == 0 {
            service.print_membership_table();
        }
        
        let alive_count = service.get_alive_members().len();
        println!("Cluster Status: {} alive members", alive_count);
    }
    
    service.shutdown();
    println!("Service stopped successfully");
    Ok(())
}