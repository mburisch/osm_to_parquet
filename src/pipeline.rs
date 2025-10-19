use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, bounded};

#[derive(Debug, Clone)]
pub struct OsmChannels<Node, Way, Relation> {
    pub node_sender: Sender<Arc<Node>>,
    pub node_receiver: Receiver<Arc<Node>>,
    pub way_sender: Sender<Arc<Way>>,
    pub way_receiver: Receiver<Arc<Way>>,
    pub relation_sender: Sender<Arc<Relation>>,
    pub relation_receiver: Receiver<Arc<Relation>>,
}

#[derive(Debug, Clone)]
pub struct OsmSendChannels<Node, Way, Relation> {
    pub node: Sender<Arc<Node>>,
    pub way: Sender<Arc<Way>>,
    pub relation: Sender<Arc<Relation>>,
}

impl<Node, Way, Relation> OsmChannels<Node, Way, Relation> {
    pub fn new(capacity: usize) -> Self {
        let (node_sender, node_receiver) = bounded(capacity);
        let (way_sender, way_receiver) = bounded(capacity);
        let (relation_sender, relation_receiver) = bounded(capacity);
        Self {
            node_sender,
            node_receiver,
            way_sender,
            way_receiver,
            relation_sender,
            relation_receiver,
        }
    }

    pub fn send_channels(&self) -> OsmSendChannels<Node, Way, Relation> {
        OsmSendChannels {
            node: self.node_sender.clone(),
            way: self.way_sender.clone(),
            relation: self.relation_sender.clone(),
        }
    }
}
