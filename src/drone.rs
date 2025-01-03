use crossbeam_channel::{Sender, Receiver};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use rand::{rng, Rng};
use wg_2024::packet::*;
use wg_2024::controller::*;
use wg_2024::controller::DroneEvent::{PacketSent,PacketDropped};
use wg_2024::network::*;
use wg_2024::packet::NackType::{Dropped, ErrorInRouting, UnexpectedRecipient};
pub enum SendingCodes{ // For performance and logic enhancements
    SuccessfullySent(u64),
    ErrorSending(String),
    NoNextHop(String)
} impl Debug for SendingCodes {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self{
            SendingCodes::SuccessfullySent(n) => write!(f, "Successfully sent [Packet {}]",n),
            SendingCodes::ErrorSending(s) => write!(f, "Error sending the packet [{}]",s),
            SendingCodes::NoNextHop(s) => write!(f, "No next hop available [{}]",s)
        }
    }
} impl Display for SendingCodes {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}
pub enum RoutingCodes{ // For performance and logic enhancements
    Correct,
    DestinationArrived, // Could be a FloodRequest
    HopsMismatch,
}
// Cache struct
struct Cache{
    history_floodreq: HashMap<NodeId, Vec<u64>>,
    crashed: bool
}

pub struct Drone {
    pub id: NodeId, // u8
    pub pdr: f32, // Network Initializer stuff
    pub packet_send: HashMap<NodeId, Sender<Packet>>, // Neighbor ID to Sender mapping
    pub packet_recv: Receiver<Packet>, // Receive Packets from Neighbors
    pub controller_send: Sender<DroneEvent>,       // Send Events to Sim. Controller
    pub controller_recv: Receiver<DroneCommand>,  // Receive from Sim. Controller
    cache: Cache, // We memorize the Flood Requests and crashings
}

impl wg_2024::drone::Drone for Drone {
    fn new(
        id: NodeId,
        controller_send: Sender<DroneEvent>,
        controller_recv: Receiver<DroneCommand>,
        packet_recv: Receiver<Packet>,
        packet_send: HashMap<NodeId, Sender<Packet>>,
        pdr: f32,
    ) -> Self {
        Self {
            id,
            controller_send,
            controller_recv,
            packet_recv,
            packet_send,
            pdr,
            cache: Cache{
                history_floodreq: HashMap::new(),
                crashed: false
            }
        }
    }
    fn run(&mut self){
        loop {      // Listen for packets and commands
            crossbeam_channel::select_biased! { // I am prioritizing Controller messages using select_biased! macro.
                recv(self.controller_recv) -> command => {
                    if let Ok(command) = command {
                        self.handle_command(command);
                    }else{
                        self.log("Failed to parse command-packet [Crossbeam Error -> Channel Closed / Empty]");
                    }
                }
                recv(self.packet_recv) -> packet => {
                    self.drone_behaviour(packet);
                }
                /* Used it for testing purposes but I choosed to keep it for the groups that wanna have a lively terminal
                default => {
                    self.log("Waiting for messages...");
                    thread::sleep(Duration::from_millis(2000));
                }
                 */
            }
        }
    }
}
impl Drone {
    fn log<S: AsRef<str>>(&self, message: S) {
        println!("LeDron ID {} - {}", self.id, message.as_ref());
    }
    fn drone_behaviour(&mut self, packet:Result<Packet, crossbeam_channel::RecvError>){
        if self.cache.crashed{
            if !self.packet_recv.is_empty(){
                // We gotta empty the queue
                if let Ok(packet) = packet {
                    // Only Ack, Nack, FloodResponse already sent
                    // before the drone-crash will be forwarded
                    // during a crashing status.
                    match packet.clone().pack_type{
                        PacketType::Nack(_) |
                        PacketType::Ack(_) |
                        PacketType::FloodResponse(_) => {
                            self.handle_packet(packet);
                        },
                        // This the case of Flood Request / Fragment message:
                        PacketType::FloodRequest(_) => {
                            self.log("Dropping packet...");
                            self.send_packet(Self::build_packet_nack(packet, Dropped, None), None);
                        },
                        PacketType::MsgFragment(fragment_id) => {
                            self.log("Dropping packet...");
                            self.send_packet(Self::build_packet_nack(packet, Dropped, Some(fragment_id.fragment_index)), None);
                        }
                    }
                }else{
                    self.log("Failed to parse command-packet [Crossbeam Error -> Channel Closed / Empty]");
                }
            }
        }else{
            if let Ok(packet) = packet {
                self.handle_packet(packet);
            }else{
                self.log("Failed to parse command-packet [Crossbeam Error -> Channel Closed / Empty]");
            }
        }
    }
    fn handle_packet(&mut self, packet: Packet) {
        self.log("Handling packet...");
        match self.handle_routing_header(&packet.routing_header){
            RoutingCodes::Correct => {
                let mut return_packet:Packet;
                match packet.pack_type.clone() {
                    PacketType::MsgFragment(fragment_id) => {
                        self.log("Handling fragment...");
                        // We consider our PDR, if bool throws true packet gets dropped.
                        let mut rng = rng();
                        if rng.random_bool(self.pdr as f64){
                            // Drop
                            self.log("Dropping packet...");
                            let _ = self.sendto_controller(packet.clone(), false); // We send the packet to Sim.Controller
                            self.send_packet(Self::build_packet_nack(packet.clone(), Dropped, Some(fragment_id.fragment_index)), None);
                        }else{
                            // println!("Drone ID {} - NOT dropping packet...", self.id);
                            return_packet = Self::update_packet_to_forward(packet);
                            self.send_packet(return_packet.clone(), None);
                        }
                    },
                    PacketType::Ack(_) | PacketType::Nack(_) | PacketType::FloodResponse(_) => {
                        // Send the packet following the SRH
                        return_packet = Self::update_packet_to_forward(packet);
                        self.send_packet(return_packet, None);
                    },
                    _ => { // It never happens
                        println!("Unknown packet [Probably Flooding-Request error-handling / NON SUPPORTED PACKET-TYPE!]");
                    },
                }
            }
            RoutingCodes::DestinationArrived => {
                // Check if it's an FloodingReq, if not, build an Nack, a Drone can't receive messages!
                println!("Drone ID {} - Packet with Drone destination arrived...", self.id);
                match packet.pack_type.clone() {
                    PacketType::FloodRequest(packet_id) => {
                        self.handle_flooding_req(packet_id, packet);
                    },
                    _ => { // DestinationIsDrone
                        println!("{:?}", self.send_packet(Self::build_packet_nack(packet, NackType::DestinationIsDrone ,None),None));
                    }
                }
            }
            RoutingCodes::HopsMismatch => { // Checks if is a Flood Req
                match packet.pack_type.clone() { // If the packet is of Fragment type we have to indicate the index number too.
                    PacketType::MsgFragment(fragment_id) => {
                        if packet.routing_header.valid_hop_index(){
                            self.send_packet(Self::build_packet_nack(packet.clone(), ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index]), Some(fragment_id.fragment_index)), None);
                        }else{
                            // SRH Received is not valid, I can't send back a Nack as I might have to guess where it did come from, fuck the drone before :(
                            println!("Drone ID {} - Discarding packet [SESSION ID: {:?}] because it has an unknown SRH (OUB)", self.id, packet.session_id);
                            let _ = self.sendto_controller(Self::build_packet_nack(packet.clone(), UnexpectedRecipient(self.id), None),false); // As we asked the WGC what to do in this case, we just got told to send to controller an UnexpectedRecipient Nack with the drone self.id.
                        }
                    },
                    PacketType::FloodRequest(packet_id) => {
                        self.handle_flooding_req(packet_id, packet);
                    }
                    _ => {
                        if packet.routing_header.valid_hop_index(){
                            self.send_packet(Self::build_packet_nack(packet.clone(), ErrorInRouting(packet.routing_header.hops[packet.routing_header.hop_index]), None), None);
                        }else{
                            // SRH Received is not valid, I can't send back a Nack as I might have to guess where it did come from, fuck the drone before :(
                            println!("Drone ID {} - Discarding packet [SESSION ID: {:?}] because it has an unknown SRH (OUB)", self.id, packet.session_id);
                            let _ = self.sendto_controller(Self::build_packet_nack(packet.clone(), UnexpectedRecipient(self.id), None),false); // As we asked the WGC what to do in this case, we just got told to send to controller an UnexpectedRecipient Nack with the drone self.id.
                        }
                    },
                }
            }
        }
    }
    fn handle_command(&mut self, command:DroneCommand){
        println!("Drone ID {} - Handling commands...", self.id);
        match command {
            DroneCommand::Crash => {
                if self.cache.crashed{
                    self.cache.crashed = false;
                    println!("Drone ID {:?} - Uncrashed the drone, restore manually the connections to revive!", self.id)
                }else{
                    self.cache.crashed = true;
                    println!("Drone ID {:?} - Crashed the drone, Simulation Controller deleting the connection!", self.id)
                }
            },
            DroneCommand::SetPacketDropRate(newpdr) => {
                self.pdr = newpdr;
            },
            DroneCommand::AddSender(nodeid, senderchannel) => {
                match self.packet_send.insert(nodeid, senderchannel){
                    Some(_) => { println!("Drone ID {:?} - Added sender channel", self.id); },
                    None => { println!("Drone ID {:?} - Failed to add sender channel", self.id); }
                }
            }
            DroneCommand::RemoveSender(nodeid) => {
                match self.packet_send.remove(&nodeid){
                    Some(_) => { println!("Drone ID {:?} - Successfully removed!", self.id) },
                    None => { println!("Drone ID {:?} - Failed to remove sender!", self.id) }
                }
            }
        }
    }     // Simple function that handles the Simulation Controller commands sent to the drone

    // Return - Codes are wrote on the first lines
    fn handle_routing_header(&self, srh: &SourceRoutingHeader)->RoutingCodes{
        println!("Drone ID {} - Handling routing header...", self.id);
        if srh.valid_hop_index(){
            if srh.is_last_hop(){
                RoutingCodes::DestinationArrived
            }else if srh.current_hop().unwrap().eq(&self.id){
                RoutingCodes::Correct
            }else{
                RoutingCodes::HopsMismatch
            }
        }else{
            RoutingCodes::HopsMismatch
        }
    }
    fn handle_flooding_req(&mut self, mut packet_id: FloodRequest, packet: Packet){
        self.log("Handling FloodRequest...");
        if let Some(floods_sent) = self.cache.history_floodreq.get_mut(&packet_id.initiator_id) {
            if floods_sent.contains(&packet_id.flood_id){
                // Already received this FloodReq, we need to build a FloodResponse
                packet_id.path_trace.push((self.id.clone(), NodeType::Drone)); // We add our ID to the path
                self.send_packet(Self::build_packet_flood_response(packet_id, packet.session_id),None);
            }else{
                let packetreceivedfrom = packet_id.path_trace.get(packet_id.path_trace.len()-1usize)
                    .expect(("Drone ID ".to_string()+self.id.to_string().as_str()+" - Received Flood Request with empty path-trace!").as_str()).clone();
                floods_sent.push(packet_id.flood_id.clone()); // **
                let mut return_packet = Packet{
                    session_id: packet.session_id,
                    routing_header: packet.routing_header,
                    pack_type: {
                        let mut return_packet_typefield = packet_id.clone();
                        return_packet_typefield.path_trace.push((self.id.clone(), NodeType::Drone)); // We adding our ID to the path trace
                        PacketType::FloodRequest(return_packet_typefield)
                    }
                };
                // We send the packet to all the neighbours beside the one we received the packet from
                for i in &mut self.packet_send.clone(){
                    if *i.0 !=  packetreceivedfrom.0{
                        self.send_packet(return_packet.clone(), Some(i.1));
                    }
                }
            }
        }else{
            let packetreceivedfrom = packet_id.path_trace.get(packet_id.path_trace.len()-1usize)
                .expect(("Drone ID ".to_string()+self.id.to_string().as_str()+" - Received Flood Request with empty path-trace!").as_str()).clone();
            self.cache.history_floodreq.insert(packet_id.initiator_id.clone(), vec![packet_id.flood_id.clone()]); // **
            let mut return_packet = Packet{
                session_id: packet.session_id,
                routing_header: packet.routing_header,
                pack_type: {
                    let mut return_packet_typefield = packet_id.clone();
                    return_packet_typefield.path_trace.push((self.id.clone(), NodeType::Drone)); // We adding our ID to the path trace
                    PacketType::FloodRequest(return_packet_typefield)
                }
            };
            // We send the packet to all the neighbours beside the one we received the packet from
            for i in &mut self.packet_send.clone(){
                if *i.0 !=  packetreceivedfrom.0{
                    self.send_packet(return_packet.clone(), Some(i.1));
                }
            }
        }
    }

    fn update_packet_to_forward(mut packet: Packet) -> Packet{ // OK
        // It is assumed that this function would be used only in the specified cases where it is used
        // so it will indeed skip all the needed controls and will just update the hop
        // index.
        packet.routing_header.hop_index += 1;
        packet
    }
    fn build_packet_flood_response(flreq_header: FloodRequest, srcid:u64) -> Packet{
        let return_packet:Packet = Packet{
            routing_header: SourceRoutingHeader{
                // Hop Index 1,
                hop_index:1,
                hops: {
                    flreq_header.path_trace.iter().cloned().rev().map(|(id, _)| id).collect()
                }
            },
            pack_type: {
                PacketType::FloodResponse(FloodResponse{
                    flood_id:flreq_header.flood_id,
                    path_trace:flreq_header.path_trace,
                })
            },
            session_id: srcid// random number or session id from the old packet?,
        };
        println!("Drone ID {} - Building Flood Response", return_packet.routing_header.hops[0]);
        return_packet
    }
    fn build_packet_nack(packet: Packet, nack_id: NackType, optional_fragment_index: Option<u64>)-> Packet{
        Packet {
            session_id: packet.session_id,
            routing_header: {
                let mut old_srh = packet.routing_header;
                old_srh = old_srh.sub_route(0..=old_srh.hop_index).unwrap();
                old_srh.reverse();
                old_srh.hop_index=1;
                println!("Drone ID {:?} - Building Nack [{:?}]...", old_srh.hops[0], nack_id);
                old_srh
            },
            pack_type: PacketType::Nack(Nack{
                fragment_index: optional_fragment_index.unwrap_or(0),
                nack_type: nack_id
            })
        }
    }
    fn send_packet(&self, packet: Packet, channel: Option<&mut Sender<Packet>>) -> SendingCodes { // OK
        self.log("Sending packet...");
        //
        match channel {
            Some(ch) => {
                match ch.send(packet.clone()) {
                    Ok(_) => {
                        // self.sendto_controller(packet, false); Ack to Sim. Controller
                        self.log("Successfully sent packet...");
                        SendingCodes::SuccessfullySent(packet.session_id)
                    },
                    Err(er) => {
                        println!("Drone ID {:?} - {:?}", self.id, er.to_string());
                        let _ = self.sendto_controller(packet, false); // We send the packet to Sim.Controller
                        SendingCodes::ErrorSending(er.to_string())
                    }
                }
            },
            None => {
                let channeltosend = self.packet_send.get(&packet.clone().routing_header.hops[packet.routing_header.hop_index]);
                match channeltosend {
                    Some(ch) => {
                        match ch.send(packet.clone()) {
                            Ok(_) => {
                                self.log("Successfully sent packet...");
                                let _ = self.sendto_controller(packet.clone(), true); //Ack to Sim. Controller
                                SendingCodes::SuccessfullySent(packet.session_id)
                            },
                            Err(er) => {
                                println!("Drone ID {:?} - {:?}", self.id, er.to_string());
                                let _ = self.sendto_controller(packet, true); // We send the packet to Sim.Controller
                                SendingCodes::ErrorSending(er.to_string())
                            }
                        }
                    },
                    None => {
                        self.log("Neighbour not found...");
                        let _ = self.sendto_controller(packet, true); // We send the packet to Sim.Controller
                        SendingCodes::NoNextHop("Neighbour not found".to_string())
                    }
                }
            }
        }
    }

    // Every time we send / drop a packet we send an ack to the Simulation Controller,
    // as its implementation it's not specified correctly, we suppose it's up to each
    // group.
    fn sendto_controller(&self, packet: Packet, sent: bool) -> Result<(),String>{
        self.log("Sending to controller...");
        if sent {
            match self.controller_send.send(PacketSent(packet)){
                Ok(_) => {
                    self.log("Successfully sent to controller.");
                    Ok(())
                },
                Err(er) => {
                    println!("Drone ID {} - {:?}", self.id, er.to_string());
                    Err(er.to_string())
                }
            }
        }else{
            match self.controller_send.send(PacketDropped(packet)) {
                Ok(_) => {
                    self.log("Successfully sent to controller.");
                    Ok(())
                },
                Err(er) => {
                    println!("Drone ID {} - {:?}", self.id, er.to_string());
                    Err(er.to_string())
                }
            }
        }
    }
}