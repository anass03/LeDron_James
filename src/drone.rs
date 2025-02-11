use crossbeam_channel::{Receiver, Sender};
use rand::{rng, Rng};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use wg_2024::controller::*;
use wg_2024::network::*;
use wg_2024::packet::NackType::{Dropped, ErrorInRouting, UnexpectedRecipient};
use wg_2024::packet::*;

/// Logging purposes.
enum SendingCodes {
    SuccessfullySent(u64),
    ErrorSending(String),
    NoNextHop(String),
}
impl Debug for SendingCodes {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SendingCodes::SuccessfullySent(n) => write!(f, "Successfully sent [Packet {}]", n),
            SendingCodes::ErrorSending(s) => write!(f, "Error sending the packet [{}]", s),
            SendingCodes::NoNextHop(s) => write!(f, "No next hop available [{}]", s),
        }
    }
}
impl Display for SendingCodes {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}
/// For performance and logic enhancements
enum ControllerTypes {
    Sent,
    Dropped,
    Shortcut,
}

/// For performance and logic enhancements
enum RoutingCodes {
    // For performance and logic enhancements
    Correct,
    DestinationArrived,
    HopsMismatch,
}
/// Drone extra-field, uses it to store data.
struct Cache {
    history_floodreq: HashMap<NodeId, Vec<u64>>,
    crashed: bool,
    logging_enabled: bool,
}
/// Last Update: 08/02/25, Status: No known issue atm
pub struct Drone {
    pub id: NodeId,                                   // u8
    pub pdr: f32,                                     // I think it's Network Initializer stuff
    pub packet_send: HashMap<NodeId, Sender<Packet>>, // Neighbor ID to Sender mapping
    pub packet_recv: Receiver<Packet>,                // Receive Packets from Neighbors
    pub controller_send: Sender<DroneEvent>,          // Send Events to Sim. Controller
    pub controller_recv: Receiver<DroneCommand>,      // Receive from Sim. Controller
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
            cache: Cache {
                history_floodreq: HashMap::new(),
                crashed: false,
                logging_enabled: true,
            },
        }
    }
    fn run(&mut self) {
        loop {
            // Listen for packets and commands
            crossbeam_channel::select_biased! { // Prioritizing Controller messages using select_biased! macro.
                recv(self.controller_recv) -> command => {
                    if let Ok(command) = command {
                        self.handle_command(command);
                    }else{
                        // It means that channel has been closed -> We gotta shut off drone run method
                        return;
                    }
                }
                recv(self.packet_recv) -> packet => {
                    if let Ok(packet) = packet{
                        self.drone_behaviour(packet);
                    }else{
                        // It means that channel has been closed -> We gotta shut off drone run metho
                        return;
                    }
                }
            }
        }
    }
}
impl Drone {
    fn log<S: AsRef<str>>(&self, message: S) {
        #[cfg(feature = "log")]
        if self.cache.logging_enabled {
            println!("LeDron ID {} - {}", self.id, message.as_ref());
        }
    }
    pub fn logging_enabled(&mut self, log: bool) {
        // Self-Explanatory
        self.cache.logging_enabled = log
    }
    fn drone_behaviour(&mut self, packet: Packet) {
        if self.cache.crashed {
            // We gotta empty the queue, only Ack, Nack, FloodResponse already sent
            // before the drone-crash will be forwarded during a crashing status.
            match packet.clone().pack_type {
                PacketType::Nack(_) | PacketType::Ack(_) | PacketType::FloodResponse(_) => {
                    self.handle_packet(packet);
                }
                PacketType::FloodRequest(_) => {
                    self.log("Dropping packet...");
                    self.send_packet(
                        Self::build_packet_nack(packet, ErrorInRouting(self.id), None),
                        None,
                    );
                }
                PacketType::MsgFragment(fragment_id) => {
                    self.log("Dropping packet...");
                    self.send_packet(
                        Self::build_packet_nack(
                            packet,
                            ErrorInRouting(self.id),
                            Some(fragment_id.fragment_index),
                        ),
                        None,
                    );
                }
            }
        } else {
            self.handle_packet(packet);
        }
    }
    fn handle_packet(&mut self, packet: Packet) {
        self.log("Handling packet...");
        match self.handle_routing_header(&packet.routing_header) {
            RoutingCodes::Correct => {
                let return_packet: Packet;
                match packet.pack_type.clone() {
                    PacketType::MsgFragment(fragment_id) => {
                        self.log("Handling fragment...");
                        // We consider our PDR, if bool throws true packet gets dropped.
                        let mut rng = rng();
                        if rng.random_bool(self.pdr as f64) {
                            // Drop
                            self.log("Dropping packet...");
                            let _ =
                                self.sendto_controller(packet.clone(), ControllerTypes::Dropped); // We send the packet to Sim.Controller
                            self.send_packet(
                                Self::build_packet_nack(
                                    packet,
                                    Dropped,
                                    Some(fragment_id.fragment_index),
                                ),
                                None,
                            );
                        } else {
                            // println!("Drone ID {} - NOT dropping packet...", self.id);
                            return_packet = self.update_packet_to_forward(packet);
                            self.send_packet(return_packet, None);
                        }
                    }
                    PacketType::Ack(_) | PacketType::Nack(_) | PacketType::FloodResponse(_) => {
                        // Send the packet following the SRH
                        return_packet = self.update_packet_to_forward(packet);
                        self.send_packet(return_packet, None);
                    }
                    _ => {
                        // It never happens
                        self.log("Unknown packet [Probably Flooding-Request error-handling / NON SUPPORTED PACKET-TYPE!]");
                    }
                }
            }
            RoutingCodes::DestinationArrived => {
                // Check if it's an FloodReq, as handle_routing_header doesn't check it.
                // Gotta implement shortcut to Simulation Controller
                self.log("Packet with Drone destination arrived...");
                match packet.pack_type.clone() {
                    PacketType::FloodRequest(packet_id) => {
                        self.handle_flooding_req(packet_id, packet);
                    }
                    PacketType::Ack(_) | PacketType::Nack(_) | PacketType::FloodResponse(_) => {
                        let _ = self.sendto_controller(packet.clone(), ControllerTypes::Shortcut);
                        self.log(
                            format!(
                                "{:?}",
                                self.send_packet(
                                    Self::build_packet_nack(
                                        packet,
                                        NackType::DestinationIsDrone,
                                        None
                                    ),
                                    None
                                )
                            )
                            .as_str(),
                        );
                    }
                    _ => {
                        // DestinationIsDrone
                        self.log(
                            format!(
                                "{:?}",
                                self.send_packet(
                                    Self::build_packet_nack(
                                        packet,
                                        NackType::DestinationIsDrone,
                                        None
                                    ),
                                    None
                                )
                            )
                            .as_str(),
                        );
                    }
                }
            }
            RoutingCodes::HopsMismatch => {
                // Checks if it's a FloodReq, if not, send UnexpectedRecipient back.
                match packet.pack_type.clone() {
                    // If the packet is of Fragment type we have to indicate the index number too.
                    PacketType::FloodRequest(packet_id) => {
                        self.handle_flooding_req(packet_id, packet);
                    }
                    PacketType::MsgFragment(fragment_id) => {
                        if packet.routing_header.valid_hop_index() {
                            self.send_packet(
                                Self::build_packet_nack(
                                    packet.clone(),
                                    UnexpectedRecipient(
                                        packet.routing_header.hops[packet.routing_header.hop_index],
                                    ),
                                    Some(fragment_id.fragment_index),
                                ),
                                None,
                            );
                        } else {
                            // SRH Received is not valid, I can't send back a Nack as I might have to guess where it did come from, fuck the drone before :(
                            self.log(format!("Discarding packet [SESSION ID: {:?}] because it has an unknown SRH (OUB)", packet.session_id));
                            let _ = self.sendto_controller(
                                Self::build_packet_nack(
                                    packet.clone(),
                                    UnexpectedRecipient(self.id),
                                    None,
                                ),
                                ControllerTypes::Dropped,
                            ); // As we asked the WGC what to do in this case, we just got told to send to controller an UnexpectedRecipient Nack with the drone self.id.
                        }
                    }
                    PacketType::Ack(_) | PacketType::Nack(_) | PacketType::FloodResponse(_) => {
                        if packet.routing_header.valid_hop_index() {
                            self.send_packet(
                                Self::build_packet_nack(
                                    packet.clone(),
                                    UnexpectedRecipient(
                                        packet.routing_header.hops[packet.routing_header.hop_index],
                                    ),
                                    None,
                                ),
                                None,
                            );
                            let _ = self.sendto_controller(packet, ControllerTypes::Shortcut);
                        } else {
                            // SRH Received is not valid, I can't send back a Nack as I might have to guess where it did come from, fuck the drone before :(
                            self.log(format!("Discarding packet [SESSION ID: {:?}] because it has an unknown SRH (OUB)", packet.session_id));
                            let _ = self.sendto_controller(packet, ControllerTypes::Shortcut);
                            //let _ = self.sendto_controller(Self::build_packet_nack(packet.clone(), UnexpectedRecipient(self.id), None), false); // As we asked the WGC what to do in this case, we just got told to send to controller an UnexpectedRecipient Nack with the drone self.id.
                        }
                    }
                }
            }
        }
    }
    fn handle_command(&mut self, command: DroneCommand) {
        self.log("Handling commands...");
        match command {
            DroneCommand::Crash => {
                self.cache.crashed = true;
                self.log("Crashed the drone, Simulation Controller deleting the connection!");
            }
            DroneCommand::SetPacketDropRate(newpdr) => {
                self.pdr = newpdr;
            }
            DroneCommand::AddSender(nodeid, senderchannel) => {
                match self.packet_send.insert(nodeid, senderchannel) {
                    Some(_) => {
                        self.log("Added sender channel");
                    }
                    None => {
                        self.log("Failed to add sender channel");
                    }
                }
            }
            DroneCommand::RemoveSender(nodeid) => match self.packet_send.remove(&nodeid) {
                Some(_) => self.log("Successfully removed!"),
                None => self.log("Failed to remove sender!"),
            },
        }
    } // Simple function that handles the Simulation Controller commands sent to the drone

    // Return - Codes are wrote on the first lines
    fn handle_routing_header(&self, srh: &SourceRoutingHeader) -> RoutingCodes {
        self.log("Handling routing header...");
        if srh.valid_hop_index() {
            if srh.is_last_hop() {
                RoutingCodes::DestinationArrived
            } else if srh.current_hop().is_none() {
                RoutingCodes::HopsMismatch // UnexpectedRecipient
            } else {
                if srh.current_hop().unwrap().eq(&self.id) {
                    RoutingCodes::Correct
                } else {
                    RoutingCodes::HopsMismatch // UnexpectedRecipient
                }
            }
        } else {
            RoutingCodes::HopsMismatch // UnexpectedRecipient
        }
    }
    fn handle_flooding_req(&mut self, mut packet_id: FloodRequest, packet: Packet) {
        self.log("Handling FloodRequest...");
        if let Some(floods_sent) = self.cache.history_floodreq.get_mut(&packet_id.initiator_id) {
            if floods_sent.contains(&packet_id.flood_id) {
                // Already received this FloodReq, we need to build a FloodResponse
                packet_id
                    .path_trace
                    .push((self.id.clone(), NodeType::Drone));
                self.send_packet(
                    Self::build_packet_flood_response(packet_id, packet.session_id),
                    None,
                );
            } else {
                match packet_id
                    .path_trace
                    .get(packet_id.path_trace.len() - 1usize)
                {
                    None => self
                        .log("Received Flood Request with empty path-trace! Throwing packet away."),
                    Some(packetreceivedfrom) => {
                        floods_sent.push(packet_id.flood_id.clone()); // **
                        let return_packet = Packet {
                            session_id: packet.session_id,
                            routing_header: packet.routing_header,
                            pack_type: {
                                let mut return_packet_typefield = packet_id.clone();
                                return_packet_typefield
                                    .path_trace
                                    .push((self.id.clone(), NodeType::Drone)); // We adding our ID to the path trace
                                PacketType::FloodRequest(return_packet_typefield)
                            },
                        };
                        // We send the packet to all the neighbours beside the one we received the packet from
                        for i in &mut self.packet_send.clone() {
                            if *i.0 != packetreceivedfrom.0 {
                                self.send_packet(return_packet.clone(), Some(i.1));
                            }
                        }
                    }
                }
            }
        } else {
            match packet_id.path_trace.get(packet_id.path_trace.len() - 1usize) {
                    None => { self.log("Received Flood Request with empty path-trace! Throwing packet away. Drone doesn't know who to not send it back") },
                    Some(packetreceivedfrom) => {
                        self.cache.history_floodreq.insert(packet_id.initiator_id.clone(), vec![packet_id.flood_id.clone()]); // **
                        let return_packet = Packet {
                            session_id: packet.session_id,
                            routing_header: packet.routing_header,
                            pack_type: {
                                let mut return_packet_typefield = packet_id.clone();
                                return_packet_typefield.path_trace.push((self.id.clone(), NodeType::Drone)); // We adding our ID to the path trace
                                PacketType::FloodRequest(return_packet_typefield)
                            },
                        };
                        // We send the packet to all the neighbours beside the one we received the packet from
                        for i in &mut self.packet_send.clone() {
                            if *i.0 != packetreceivedfrom.0 {
                                self.send_packet(return_packet.clone(), Some(i.1));
                            }
                        }
                    }
                }
        }
    }

    fn update_packet_to_forward(&self, mut packet: Packet) -> Packet {
        // OK
        // It is assumed that this function would be used only in the specified cases where it is used
        // so it will indeed skip all the needed controls and will just update the hop
        // index.
        // [UPDATE]: Added a check if the next hop isn't available on the HashMap, in that case it will generate a nack
        let nexthop = packet.routing_header.next_hop().unwrap();
        if self.packet_send.contains_key(&nexthop) {
            packet.routing_header.hop_index += 1;
            packet
        } else {
            // Genero un nack e lo rimando dove è tornato
            let mut opt: Option<u64> = None;
            match &packet.pack_type {
                PacketType::MsgFragment(fragm) => opt = Some(fragm.fragment_index),
                PacketType::Ack(_) | PacketType::Nack(_) | PacketType::FloodResponse(_) => {
                    let _ = self.sendto_controller(packet.clone(), ControllerTypes::Shortcut);
                }
                _ => (),
            }
            Self::build_packet_nack(packet, ErrorInRouting(nexthop), opt)
        }
    }
    fn build_packet_flood_response(flreq_header: FloodRequest, srcid: u64) -> Packet {
        let return_packet: Packet = Packet {
            routing_header: SourceRoutingHeader {
                // Hop Index 1,
                hop_index: 1,
                hops: {
                    let mut rethop: Vec<NodeId>;
                    rethop = flreq_header
                        .path_trace
                        .iter()
                        .cloned()
                        .rev()
                        .map(|(id, _)| id)
                        .collect();
                    if !rethop.last().unwrap().eq(&flreq_header.initiator_id) {
                        rethop.push(flreq_header.initiator_id.clone());
                    }
                    rethop
                },
            },
            pack_type: {
                PacketType::FloodResponse(FloodResponse {
                    flood_id: flreq_header.flood_id,
                    path_trace: flreq_header.path_trace,
                })
            },
            session_id: srcid, // random number or session id from the old packet?,
        };
        return_packet
    }
    fn build_packet_nack(
        packet: Packet,
        nack_id: NackType,
        optional_fragment_index: Option<u64>,
    ) -> Packet {
        Packet {
            session_id: packet.session_id,
            routing_header: {
                let mut old_srh = packet.routing_header;
                old_srh = old_srh.sub_route(0..=old_srh.hop_index).unwrap();
                old_srh.reverse();
                old_srh.hop_index = 1;
                //println!("Building Nack [{:?}]...", nack_id);
                old_srh
            },
            pack_type: PacketType::Nack(Nack {
                fragment_index: optional_fragment_index.unwrap_or(0),
                nack_type: nack_id,
            }),
        }
    }
    fn send_packet(&self, packet: Packet, channel: Option<&mut Sender<Packet>>) -> SendingCodes {
        // OK
        self.log("Sending packet...");
        match channel {
            Some(ch) => {
                match ch.send(packet.clone()) {
                    Ok(_) => {
                        // self.sendto_controller(packet, false); Ack to Sim. Controller
                        self.log("Successfully sent packet...");
                        SendingCodes::SuccessfullySent(packet.session_id)
                    }
                    Err(er) => {
                        self.log(format!("{:?}", er.to_string()));
                        let _ = self.sendto_controller(packet, ControllerTypes::Dropped); // We send the packet to Sim.Controller
                        SendingCodes::ErrorSending(er.to_string())
                    }
                }
            }
            None => {
                let channeltosend = self
                    .packet_send
                    .get(&packet.clone().routing_header.hops[packet.routing_header.hop_index]);
                match channeltosend {
                    Some(ch) => {
                        match ch.send(packet.clone()) {
                            Ok(_) => {
                                self.log("Successfully sent packet...");
                                let _ =
                                    self.sendto_controller(packet.clone(), ControllerTypes::Sent); //Ack to Sim. Controller
                                SendingCodes::SuccessfullySent(packet.session_id)
                            }
                            Err(er) => {
                                self.log(format!("{}", er.to_string()));
                                let _ = self.sendto_controller(packet, ControllerTypes::Dropped); // We send the packet to Sim.Controller
                                SendingCodes::ErrorSending(er.to_string())
                            }
                        }
                    }
                    None => {
                        // This is never going to be used
                        //self.log("Neighbour not found...");
                        //let _ = self.sendto_controller(packet, ControllerTypes::Dropped); // We send the packet to Sim.Controller
                        SendingCodes::NoNextHop("Neighbour not found".to_string())
                    }
                }
            }
        }
    }
    // Every time we send / drop a packet we send an ack to the Simulation Controller,
    // as its implementation it's not specified correctly, we suppose it's up to each group.
    fn sendto_controller(&self, packet: Packet, sent: ControllerTypes) -> Result<(), String> {
        self.log("Sending to controller...");
        match sent {
            ControllerTypes::Sent => {
                match self.controller_send.send(DroneEvent::PacketSent(packet)) {
                    Ok(_) => {
                        self.log("Successfully sent to controller.");
                        Ok(())
                    }
                    Err(er) => {
                        self.log(format!("{}", er.to_string()));
                        Err(er.to_string())
                    }
                }
            }
            ControllerTypes::Dropped => {
                match self.controller_send.send(DroneEvent::PacketDropped(packet)) {
                    Ok(_) => {
                        self.log("Successfully sent to controller.");
                        Ok(())
                    }
                    Err(er) => {
                        self.log(format!("{}", er.to_string()));
                        Err(er.to_string())
                    }
                }
            }
            ControllerTypes::Shortcut => {
                match self
                    .controller_send
                    .send(DroneEvent::ControllerShortcut(packet))
                {
                    Ok(_) => {
                        self.log("Successfully sent to controller.");
                        Ok(())
                    }
                    Err(er) => {
                        self.log(format!("{}", er.to_string()));
                        Err(er.to_string())
                    }
                }
            }
        }
    }
}
